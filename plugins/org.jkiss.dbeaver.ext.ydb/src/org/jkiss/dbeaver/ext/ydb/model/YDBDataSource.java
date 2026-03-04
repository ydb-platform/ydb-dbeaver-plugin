/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.ydb.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.ydb.model.autocomplete.YDBAutocompleteClient;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBViewerClient;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataType;
import org.jkiss.dbeaver.model.struct.DBSObject;

import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.navigator.DBNDatabaseFolder;
import org.jkiss.dbeaver.model.navigator.DBNDatabaseNode;
import org.jkiss.dbeaver.model.navigator.DBNModel;
import org.jkiss.dbeaver.model.navigator.DBNUtils;

import tech.ydb.core.Result;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.ListDirectoryResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * YDB DataSource with hierarchical folder support.
 * Tables with "/" in their names are organized into nested folders.
 */
public class YDBDataSource extends GenericDataSource {

    private static final Log log = Log.getLog(YDBDataSource.class);

    public static final String PROP_AUTOCOMPLETE_API_ENABLED = "ydb.autocompleteApiEnabled";

    private String ydbVersion = null;
    private YDBAutocompleteClient autocompleteClient;
    private boolean autocompleteClientInitialized = false;

    private Map<String, YDBTableFolder> rootFolders;
    private List<GenericTableBase> rootTables;
    private boolean hierarchyBuilt = false;
    private boolean navigatorNodesPreloaded = false;
    private YDBSystemViewsFolder systemViewsFolder;
    private YDBResourcePoolsFolder resourcePoolsFolder;
    private YDBResourcePoolClassifiersFolder resourcePoolClassifiersFolder;
    private YDBTopicsFolder topicsFolder;
    private YDBExternalDataSourcesFolder externalDataSourcesFolder;
    private YDBExternalTablesFolder externalTablesFolder;
    private YDBStreamingQueriesFolder streamingQueriesFolder;
    private YDBViewsFolder viewsFolder;
    private YDBTransfersFolder transfersFolder;

    public YDBDataSource(
        DBRProgressMonitor monitor,
        DBPDataSourceContainer container,
        GenericMetaModel metaModel
    ) throws DBException {
        super(monitor, container, metaModel, new YDBSQLDialect());
        log.debug("YDBDataSource created");
    }

    @Nullable
    public synchronized YDBAutocompleteClient getAutocompleteClient() {
        if (autocompleteClientInitialized) {
            return autocompleteClient;
        }
        autocompleteClientInitialized = true;
        DBPConnectionConfiguration cfg = getContainer().getConnectionConfiguration();
        String enabled = cfg.getProviderProperty(PROP_AUTOCOMPLETE_API_ENABLED);
        if ("false".equals(enabled)) {
            log.debug("YDB autocomplete API is disabled in connection settings");
            return null;
        }
        String monitoringUrl = cfg.getProviderProperty("ydb.monitoringUrl");
        String jdbcUrl = cfg.getUrl();
        String host = cfg.getHostName();
        String secureStr = cfg.getProviderProperty("ydb.useSecure");
        boolean secure = secureStr == null || Boolean.parseBoolean(secureStr);
        String baseUrl = YDBViewerClient.resolveBaseUrl(monitoringUrl, jdbcUrl, host, secure);
        if (baseUrl == null) {
            log.debug("YDB autocomplete API: cannot resolve viewer URL");
            return null;
        }
        String database = cfg.getDatabaseName();
        if (database == null || database.isEmpty()) {
            log.debug("YDB autocomplete API: database name is empty");
            return null;
        }
        String token = cfg.getProviderProperty("ydb.token");
        autocompleteClient = new YDBAutocompleteClient(baseUrl, database, token);
        log.debug("YDB autocomplete API initialized: " + baseUrl + " database=" + database);
        return autocompleteClient;
    }

    @Override
    protected String getConnectionURL(DBPConnectionConfiguration connectionInfo) {
        // Always use our provider to build the URL with grpcs:// and auth params.
        // super.getConnectionURL() delegates to DriverDescriptor which uses the
        // sampleURL template and does NOT call YDBDataSourceProvider.getConnectionURL().
        DBPDriver driver = getContainer().getDriver();
        return driver.getDataSourceProvider().getConnectionURL(driver, connectionInfo);
    }

    @Override
    protected void fillConnectionProperties(DBPConnectionConfiguration connectionInfo, Properties connectProps) {
        super.fillConnectionProperties(connectionInfo, connectProps);

        // Ensure SSL certificate is passed as a connection property
        String sslCertificate = connectionInfo.getProviderProperty("ydb.sslCertificate");
        if (sslCertificate != null && !sslCertificate.isEmpty()) {
            connectProps.setProperty("secureConnectionCertificate", sslCertificate);
        }
    }

    /**
     * Build folder hierarchy by scanning directories via SchemeClient.
     */
    private synchronized void buildHierarchy(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (hierarchyBuilt) {
            return;
        }

        rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        rootTables = new ArrayList<>();

        List<TableEntry> tableEntries = new ArrayList<>();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load tables")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                SchemeClient schemeClient = ctx.getSchemeClient();
                String prefixPath = ctx.getPrefixPath();

                scanTables(schemeClient, prefixPath, "", tableEntries);
            }
        } catch (SQLException e) {
            log.debug("Failed to load tables via SchemeClient: " + e.getMessage());
        }

        for (TableEntry entry : tableEntries) {
            String relativePath = entry.path;
            if (relativePath.contains("/")) {
                String[] parts = relativePath.split("/");
                YDBTable table = new YDBTable(this, relativePath, "TABLE", null, entry.columnTable);

                YDBTableFolder currentFolder = null;
                for (int i = 0; i < parts.length - 1; i++) {
                    String folderName = parts[i];
                    if (currentFolder == null) {
                        currentFolder = rootFolders.computeIfAbsent(
                            folderName,
                            n -> new YDBTableFolder(this, null, n)
                        );
                    } else {
                        currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                    }
                }

                if (currentFolder != null) {
                    currentFolder.addTable(table);
                }
            } else {
                rootTables.add(new YDBTable(this, relativePath, "TABLE", null, entry.columnTable));
            }
        }

        rootTables.sort(Comparator.comparing(GenericTableBase::getName, String.CASE_INSENSITIVE_ORDER));

        // Populate standard tableCache so DBeaver navigator/UI can find tables
        List<GenericTableBase> allTables = new ArrayList<>(rootTables);
        collectAllTables(rootFolders.values(), allTables);
        getTableCache().setCache(allTables);

        hierarchyBuilt = true;
    }

    /**
     * Lazily pre-load navigator tree nodes so nodeMap contains all table instances.
     * Called from getChildren()/getChild() — NOT from buildHierarchy(),
     * because during buildHierarchy() the navigator model is not yet ready.
     */
    private void ensureNavigatorNodesPreloaded(@NotNull DBRProgressMonitor monitor) {
        if (navigatorNodesPreloaded) {
            return;
        }
        preloadNavigatorNodes(monitor);
    }

    private void preloadNavigatorNodes(@NotNull DBRProgressMonitor monitor) {
        try {
            DBNModel navModel = DBNUtils.getNavigatorModel(this);
            if (navModel == null) {
                return;
            }
            DBNDatabaseNode dsNode = navModel.getNodeByObject(getContainer());
            if (dsNode == null) {
                return;
            }
            DBNDatabaseNode[] folders = dsNode.getChildren(monitor);
            if (folders == null) {
                return;
            }
            for (DBNDatabaseNode folder : folders) {
                if (folder instanceof DBNDatabaseFolder) {
                    loadNavigatorChildrenRecursively(monitor, folder);
                }
            }
            navigatorNodesPreloaded = true;
        } catch (Exception e) {
            log.debug("preloadNavigatorNodes failed: " + e.getMessage());
        }
    }

    private void loadNavigatorChildrenRecursively(
            @NotNull DBRProgressMonitor monitor, @NotNull DBNDatabaseNode node) {
        try {
            DBNDatabaseNode[] children = node.getChildren(monitor);
            if (children != null) {
                for (DBNDatabaseNode child : children) {
                    DBSObject obj = child.getObject();
                    if (child instanceof DBNDatabaseFolder || obj instanceof YDBTableFolder) {
                        loadNavigatorChildrenRecursively(monitor, child);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to load navigator children: " + e.getMessage());
        }
    }

    private void collectAllTables(Collection<YDBTableFolder> folders, List<GenericTableBase> result) {
        for (YDBTableFolder folder : folders) {
            result.addAll(folder.getTables());
            collectAllTables(folder.getSubFolders(), result);
        }
    }

    private static class TableEntry {
        final String path;
        final boolean columnTable;

        TableEntry(String path, boolean columnTable) {
            this.path = path;
            this.columnTable = columnTable;
        }
    }

    private void scanTables(SchemeClient schemeClient, String basePath,
                            String relativePath, List<TableEntry> result) {
        String fullPath = relativePath.isEmpty() ? basePath : basePath + "/" + relativePath;
        Result<ListDirectoryResult> listResult = schemeClient.listDirectory(fullPath).join();

        if (!listResult.isSuccess()) {
            log.debug("Failed to list directory: " + fullPath);
            return;
        }

        for (SchemeOperationProtos.Entry entry : listResult.getValue().getChildren()) {
            String entryPath = relativePath.isEmpty()
                ? entry.getName()
                : relativePath + "/" + entry.getName();

            SchemeOperationProtos.Entry.Type type = entry.getType();
            if (type == SchemeOperationProtos.Entry.Type.TABLE
                    || type == SchemeOperationProtos.Entry.Type.COLUMN_TABLE) {
                result.add(new TableEntry(entryPath, type == SchemeOperationProtos.Entry.Type.COLUMN_TABLE));
                log.debug("Found table: " + entryPath + " (column=" + (type == SchemeOperationProtos.Entry.Type.COLUMN_TABLE) + ")");
            } else if (type == SchemeOperationProtos.Entry.Type.DIRECTORY
                    && !entry.getName().startsWith(".")) {
                scanTables(schemeClient, basePath, entryPath, result);
            }
        }
    }

    @Override
    public void cacheStructure(@NotNull DBRProgressMonitor monitor, int scope) throws DBException {
        // Use SchemeClient-based hierarchy instead of JDBC getTables()
        // JDBC driver recursively lists all directories including .tmp
        // which causes UNAUTHORIZED errors
        buildHierarchy(monitor);
    }

    @Override
    public List<? extends GenericTableBase> getTables(DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        return getTableCache().getCachedObjects();
    }

    @Override
    public GenericTableBase getTable(DBRProgressMonitor monitor, String name) throws DBException {
        buildHierarchy(monitor);
        return getTableCache().getCachedObject(name);
    }

    @Override
    public DBSObject getChild(@NotNull DBRProgressMonitor monitor, @NotNull String childName) throws DBException {
        buildHierarchy(monitor);
        ensureNavigatorNodesPreloaded(monitor);
        // Strip trailing "." — DBeaver core has hardcoded endsWith(".") check in completion,
        // so "slonn." can arrive as childName when user types "slonn." for completion
        String lookupName = childName;
        if (lookupName.endsWith(".")) {
            lookupName = lookupName.substring(0, lookupName.length() - 1);
        }
        if (rootFolders != null) {
            YDBTableFolder folder = rootFolders.get(lookupName);
            if (folder != null) {
                return folder;
            }
        }
        return findTableByFullPath(lookupName);
    }

    @Nullable
    private YDBTable findTableByFullPath(@NotNull String fullPath) {
        if (!fullPath.contains("/")) {
            if (rootTables != null) {
                for (GenericTableBase table : rootTables) {
                    if (table.getName().equalsIgnoreCase(fullPath)) {
                        return (YDBTable) table;
                    }
                }
            }
            return null;
        }
        String[] parts = fullPath.split("/");
        if (rootFolders == null || parts.length < 2) {
            return null;
        }
        YDBTableFolder folder = rootFolders.get(parts[0]);
        for (int i = 1; i < parts.length - 1 && folder != null; i++) {
            String folderName = parts[i];
            YDBTableFolder found = null;
            for (YDBTableFolder sub : folder.getSubFolders()) {
                if (sub.getName().equalsIgnoreCase(folderName)) {
                    found = sub;
                    break;
                }
            }
            folder = found;
        }
        if (folder == null) {
            return null;
        }
        String tableName = parts[parts.length - 1];
        for (YDBTable table : folder.getTables()) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }

    @Override
    public Collection<? extends DBSObject> getChildren(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        ensureNavigatorNodesPreloaded(monitor);
        List<DBSObject> children = new ArrayList<>();
        if (rootFolders != null) {
            children.addAll(rootFolders.values());
        }
        if (rootTables != null) {
            children.addAll(rootTables);
        }
        return children;
    }

    /**
     * Get root-level folders (tables with "/" are grouped here).
     */
    @Association
    @NotNull
    public Collection<YDBTableFolder> getTableFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    /**
     * Get root-level tables (tables without "/" in names).
     */
    @Association
    @NotNull
    public List<GenericTableBase> getRootTables(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        return rootTables != null ? rootTables : List.of();
    }

    /**
     * Check if system views folder should be shown.
     * Used by plugin.xml visibleIf condition.
     */
    public boolean showSystemViewsFolder() {
        return getContainer().getNavigatorSettings().isShowSystemObjects();
    }

    /**
     * Get system views from .sys directory.
     * Only shown when "Show system objects" is enabled in navigator settings.
     */
    @Association
    @NotNull
    public Collection<YDBSystemView> getSystemViews(@NotNull DBRProgressMonitor monitor) throws DBException {
        log.debug("YDBDataSource.getSystemViews() called");
        if (systemViewsFolder == null) {
            log.debug("Creating new YDBSystemViewsFolder");
            systemViewsFolder = new YDBSystemViewsFolder(this);
        }
        Collection<YDBSystemView> views = systemViewsFolder.getSystemViews(monitor);
        log.debug("YDBDataSource.getSystemViews() returning " + views.size() + " views");
        return views;
    }

    /**
     * Get resource pools from .metadata/workload_manager/pools directory.
     * Resource pools are used for workload management in YDB.
     */
    @Association
    @NotNull
    public Collection<YDBResourcePool> getResourcePools(@NotNull DBRProgressMonitor monitor) throws DBException {
        log.debug("YDBDataSource.getResourcePools() called");
        if (resourcePoolsFolder == null) {
            log.debug("Creating new YDBResourcePoolsFolder");
            resourcePoolsFolder = new YDBResourcePoolsFolder(this);
        }
        Collection<YDBResourcePool> pools = resourcePoolsFolder.getResourcePools(monitor);
        log.debug("YDBDataSource.getResourcePools() returning " + pools.size() + " pools");
        return pools;
    }

    /**
     * Get resource pool classifiers from .sys/resource_pool_classifiers system view.
     * Resource pool classifiers are used for workload management in YDB.
     */
    @Association
    @NotNull
    public Collection<YDBResourcePoolClassifier> getResourcePoolClassifiers(@NotNull DBRProgressMonitor monitor) throws DBException {
        log.debug("YDBDataSource.getResourcePoolClassifiers() called");
        if (resourcePoolClassifiersFolder == null) {
            log.debug("Creating new YDBResourcePoolClassifiersFolder");
            resourcePoolClassifiersFolder = new YDBResourcePoolClassifiersFolder(this);
        }
        Collection<YDBResourcePoolClassifier> classifiers = resourcePoolClassifiersFolder.getResourcePoolClassifiers(monitor);
        log.debug("YDBDataSource.getResourcePoolClassifiers() returning " + classifiers.size() + " classifiers");
        return classifiers;
    }

    private YDBTopicsFolder getOrCreateTopicsFolder() {
        if (topicsFolder == null) {
            topicsFolder = new YDBTopicsFolder(this);
        }
        return topicsFolder;
    }

    @Association
    @NotNull
    public Collection<YDBTopicFolder> getTopicFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateTopicsFolder().getTopicFolders(monitor);
    }

    @Association
    @NotNull
    public List<YDBTopic> getRootTopics(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateTopicsFolder().getRootTopics(monitor);
    }

    private YDBExternalDataSourcesFolder getOrCreateExternalDataSourcesFolder() {
        if (externalDataSourcesFolder == null) {
            externalDataSourcesFolder = new YDBExternalDataSourcesFolder(this);
        }
        return externalDataSourcesFolder;
    }

    @Association
    @NotNull
    public Collection<YDBExternalDataSourceFolder> getExternalDataSourceFolders(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateExternalDataSourcesFolder().getExternalDataSourceFolders(monitor);
    }

    @Association
    @NotNull
    public List<YDBExternalDataSource> getRootExternalDataSources(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateExternalDataSourcesFolder().getRootExternalDataSources(monitor);
    }

    private YDBExternalTablesFolder getOrCreateExternalTablesFolder() {
        if (externalTablesFolder == null) {
            externalTablesFolder = new YDBExternalTablesFolder(this);
        }
        return externalTablesFolder;
    }

    @Association
    @NotNull
    public Collection<YDBExternalTableFolder> getExternalTableFolders(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateExternalTablesFolder().getExternalTableFolders(monitor);
    }

    @Association
    @NotNull
    public List<YDBExternalTable> getRootExternalTables(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateExternalTablesFolder().getRootExternalTables(monitor);
    }

    private YDBStreamingQueriesFolder getOrCreateStreamingQueriesFolder() {
        if (streamingQueriesFolder == null) {
            streamingQueriesFolder = new YDBStreamingQueriesFolder(this);
        }
        return streamingQueriesFolder;
    }

    /**
     * Get root-level streaming query folders (queries with "/" in Path are grouped here).
     */
    @Association
    @NotNull
    public Collection<YDBStreamingQueryFolder> getStreamingQueryFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateStreamingQueriesFolder().getQueryFolders(monitor);
    }

    /**
     * Get root-level streaming queries (queries without "/" in Path).
     */
    @Association
    @NotNull
    public List<YDBStreamingQuery> getRootStreamingQueries(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateStreamingQueriesFolder().getRootQueries(monitor);
    }

    private YDBTransfersFolder getOrCreateTransfersFolder() {
        if (transfersFolder == null) {
            transfersFolder = new YDBTransfersFolder(this);
        }
        return transfersFolder;
    }

    @Association
    @NotNull
    public Collection<YDBTransferFolder> getTransferFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateTransfersFolder().getTransferFolders(monitor);
    }

    @Association
    @NotNull
    public List<YDBTransfer> getRootTransfers(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateTransfersFolder().getRootTransfers(monitor);
    }

    private YDBViewsFolder getOrCreateViewsFolder() {
        if (viewsFolder == null) {
            viewsFolder = new YDBViewsFolder(this);
        }
        return viewsFolder;
    }

    @Association
    @NotNull
    public Collection<YDBViewFolder> getViewFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateViewsFolder().getViewFolders(monitor);
    }

    @Association
    @NotNull
    public List<YDBView> getRootViews(@NotNull DBRProgressMonitor monitor) throws DBException {
        return getOrCreateViewsFolder().getRootViews(monitor);
    }

    @Nullable
    public YDBExternalDataSource findExternalDataSource(
            @NotNull DBRProgressMonitor monitor, @NotNull String path) throws DBException {
        // Search in root entries
        for (YDBExternalDataSource ds : getOrCreateExternalDataSourcesFolder()
                .getRootExternalDataSources(monitor)) {
            if (path.equals(ds.getFullPath()) || path.equals(ds.getName())) {
                return ds;
            }
        }
        // Search in folders
        for (YDBExternalDataSourceFolder folder : getOrCreateExternalDataSourcesFolder()
                .getExternalDataSourceFolders(monitor)) {
            YDBExternalDataSource found = findInFolder(folder, path);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Nullable
    private YDBExternalDataSource findInFolder(
            @NotNull YDBExternalDataSourceFolder folder, @NotNull String path) {
        for (YDBExternalDataSource ds : folder.getExternalDataSources()) {
            if (path.equals(ds.getFullPath()) || path.equals(ds.getName())) {
                return ds;
            }
        }
        for (YDBExternalDataSourceFolder sub : folder.getSubFolders()) {
            YDBExternalDataSource found = findInFolder(sub, path);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Nullable
    public DBSObject findObjectByFullPath(@NotNull DBRProgressMonitor monitor, @NotNull String path) {
        try {
            buildHierarchy(monitor);

            // Try tables — need navigator instance via findNavObjectByPath
            // because buildHierarchy table instances may not match nodeMap keys
            YDBTable table = findTableByFullPath(path);
            if (table != null) {
                DBSObject navObj = findNavObjectByPath(monitor, path);
                return navObj != null ? navObj : table;
            }
            // Try topics
            YDBTopic topic = findTopicByFullPath(monitor, path);
            if (topic != null) {
                return topic;
            }
            // Try views
            YDBView view = findViewByFullPath(monitor, path);
            if (view != null) {
                return view;
            }
        } catch (DBException e) {
            log.debug("Failed to find object by path: " + path, e);
        }
        return null;
    }

    /**
     * Find the navigator's own instance of an object by traversing the navigator tree.
     * Needed for tables because GenericDataSource creates separate instances
     * through its structureContainer, so buildHierarchy instances don't match nodeMap keys.
     */
    @Nullable
    private DBSObject findNavObjectByPath(@NotNull DBRProgressMonitor monitor, @NotNull String relativePath) {
        DBNModel navModel = DBNUtils.getNavigatorModel(this);
        if (navModel == null) {
            return null;
        }
        DBNDatabaseNode dsNode = navModel.getNodeByObject(getContainer());
        if (dsNode == null) {
            return null;
        }
        try {
            DBNDatabaseNode[] topFolders = dsNode.getChildren(monitor);
            if (topFolders == null) {
                return null;
            }
            for (DBNDatabaseNode topFolder : topFolders) {
                DBSObject found = searchNavTree(monitor, topFolder, relativePath);
                if (found != null) {
                    return found;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to find nav object by path: " + e.getMessage());
        }
        return null;
    }

    @Nullable
    private DBSObject searchNavTree(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBNDatabaseNode node,
            @NotNull String relativePath) throws DBException {
        String[] parts = relativePath.split("/");

        DBNDatabaseNode current = node;
        for (int i = 0; i < parts.length; i++) {
            String segment = parts[i];
            DBNDatabaseNode[] children = current.getChildren(monitor);
            if (children == null) {
                return null;
            }
            DBNDatabaseNode match = null;
            for (DBNDatabaseNode child : children) {
                if (child.getNodeDisplayName().equalsIgnoreCase(segment)) {
                    if (i == parts.length - 1) {
                        return child.getObject();
                    }
                    match = child;
                    break;
                }
            }
            if (match == null) {
                return null;
            }
            current = match;
        }
        return null;
    }

    @Nullable
    private YDBTopic findTopicByFullPath(@NotNull DBRProgressMonitor monitor, @NotNull String path) throws DBException {
        YDBTopicsFolder tf = getOrCreateTopicsFolder();
        // Search root topics
        for (YDBTopic topic : tf.getRootTopics(monitor)) {
            if (topic.getFullPath().equalsIgnoreCase(path) || topic.getName().equalsIgnoreCase(path)) {
                return topic;
            }
        }
        // Search in folders
        if (!path.contains("/")) {
            return null;
        }
        String[] parts = path.split("/");
        Collection<YDBTopicFolder> folders = tf.getTopicFolders(monitor);
        YDBTopicFolder folder = null;
        for (int i = 0; i < parts.length - 1; i++) {
            String segment = parts[i];
            YDBTopicFolder found = null;
            Collection<YDBTopicFolder> searchIn = folder == null ? folders : folder.getSubFolders();
            for (YDBTopicFolder f : searchIn) {
                if (f.getName().equalsIgnoreCase(segment)) {
                    found = f;
                    break;
                }
            }
            if (found == null) {
                return null;
            }
            folder = found;
        }
        if (folder == null) {
            return null;
        }
        String leafName = parts[parts.length - 1];
        for (YDBTopic topic : folder.getTopics()) {
            if (topic.getName().equalsIgnoreCase(leafName)) {
                return topic;
            }
        }
        return null;
    }

    @Nullable
    private YDBView findViewByFullPath(@NotNull DBRProgressMonitor monitor, @NotNull String path) throws DBException {
        YDBViewsFolder vf = getOrCreateViewsFolder();
        // Search root views
        for (YDBView view : vf.getRootViews(monitor)) {
            if (view.getFullPath().equalsIgnoreCase(path) || view.getName().equalsIgnoreCase(path)) {
                return view;
            }
        }
        // Search in folders
        if (!path.contains("/")) {
            return null;
        }
        String[] parts = path.split("/");
        Collection<YDBViewFolder> folders = vf.getViewFolders(monitor);
        YDBViewFolder folder = null;
        for (int i = 0; i < parts.length - 1; i++) {
            String segment = parts[i];
            YDBViewFolder found = null;
            Collection<YDBViewFolder> searchIn = folder == null ? folders : folder.getSubFolders();
            for (YDBViewFolder f : searchIn) {
                if (f.getName().equalsIgnoreCase(segment)) {
                    found = f;
                    break;
                }
            }
            if (found == null) {
                return null;
            }
            folder = found;
        }
        if (folder == null) {
            return null;
        }
        String leafName = parts[parts.length - 1];
        for (YDBView view : folder.getViews()) {
            if (view.getName().equalsIgnoreCase(leafName)) {
                return view;
            }
        }
        return null;
    }

    @NotNull
    public String getYDBVersion(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (ydbVersion != null) {
            return ydbVersion;
        }
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Get YDB version")) {
            try (JDBCPreparedStatement stmt = session.prepareStatement("SELECT version()")) {
                try (JDBCResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        ydbVersion = rs.getString(1);
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to get YDB version", e);
        }
        if (ydbVersion == null) {
            ydbVersion = "";
        }
        return ydbVersion;
    }

    public boolean isTopicReadingSupported(@NotNull DBRProgressMonitor monitor) throws DBException {
        String version = getYDBVersion(monitor);
        if (version.startsWith("stable-")) {
            // Version format: "stable-26-1-hotfix-2" or similar
            try {
                java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("^stable-(\\d+)-(\\d+)(?:-|$)")
                    .matcher(version);
                if (m.find()) {
                    int major = Integer.parseInt(m.group(1));
                    int minor = Integer.parseInt(m.group(2));
                    return major > 26 || (major == 26 && minor >= 1);
                }
            } catch (NumberFormatException e) {
                log.debug("Failed to parse YDB version: " + version);
            }
            return false;
        }
        // Non-stable builds (main, trunk, dev, etc.) — assume supported
        return true;
    }

    @Override
    public synchronized DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        ydbVersion = null;
        autocompleteClient = null;
        autocompleteClientInitialized = false;
        hierarchyBuilt = false;
        navigatorNodesPreloaded = false;
        rootFolders = null;
        rootTables = null;
        systemViewsFolder = null;
        resourcePoolsFolder = null;
        resourcePoolClassifiersFolder = null;
        topicsFolder = null;
        externalDataSourcesFolder = null;
        externalTablesFolder = null;
        streamingQueriesFolder = null;
        viewsFolder = null;
        transfersFolder = null;
        DBSObject result = super.refreshObject(monitor);
        buildHierarchy(monitor);
        return result;
    }

    @Nullable
    @Override
    public DBSDataType getLocalDataType(@Nullable String typeName) {
        if (typeName != null) {
            String upper = typeName.toUpperCase();
            if (upper.equals("JSON") || upper.equals("JSONDOCUMENT")) {
                // Return null so that JDBCColumnMetaData falls back to resolveDataKind,
                // which correctly returns CONTENT for JSON types.
                // GenericDataType.getDataKind() uses a static method that maps VARCHAR -> STRING,
                // which prevents ContentValueManager (and JSON editor) from being used.
                return null;
            }
        }
        return super.getLocalDataType(typeName);
    }

    @NotNull
    @Override
    public DBPDataKind resolveDataKind(@NotNull String typeName, int valueType) {
        String upperType = typeName.toUpperCase();

        if (upperType.startsWith("LIST<") || upperType.startsWith("LIST(")) {
            return DBPDataKind.ARRAY;
        }
        if (upperType.startsWith("STRUCT<") || upperType.startsWith("STRUCT(") ||
            upperType.startsWith("TUPLE<") || upperType.startsWith("TUPLE(")) {
            return DBPDataKind.STRUCT;
        }
        if (upperType.startsWith("DICT<") || upperType.startsWith("DICT(")) {
            return DBPDataKind.OBJECT;
        }
        if (upperType.equals("JSON") || upperType.equals("JSONDOCUMENT") || upperType.equals("YSON")) {
            return DBPDataKind.CONTENT;
        }
        if (upperType.equals("UUID")) {
            return DBPDataKind.STRING;
        }
        if (upperType.startsWith("INTERVAL")) {
            return DBPDataKind.STRING;
        }
        if (upperType.equals("DYNUMBER")) {
            return DBPDataKind.NUMERIC;
        }

        return super.resolveDataKind(typeName, valueType);
    }

    @NotNull
    @Override
    public String getDefaultDataTypeName(@NotNull DBPDataKind dataKind) {
        switch (dataKind) {
            case STRING:
                return "Utf8";
            case NUMERIC:
                return "Int64";
            case BOOLEAN:
                return "Bool";
            case DATETIME:
                return "Timestamp";
            case BINARY:
                return "String";
            default:
                return super.getDefaultDataTypeName(dataKind);
        }
    }
}
