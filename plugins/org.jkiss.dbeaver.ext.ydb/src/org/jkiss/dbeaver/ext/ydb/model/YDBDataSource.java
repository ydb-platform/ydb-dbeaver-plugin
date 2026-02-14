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
import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;

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

    private String ydbVersion = null;

    private Map<String, YDBTableFolder> rootFolders;
    private List<GenericTableBase> rootTables;
    private boolean hierarchyBuilt = false;
    private YDBSystemViewsFolder systemViewsFolder;
    private YDBResourcePoolsFolder resourcePoolsFolder;
    private YDBResourcePoolClassifiersFolder resourcePoolClassifiersFolder;
    private YDBTopicsFolder topicsFolder;
    private YDBExternalDataSourcesFolder externalDataSourcesFolder;
    private YDBExternalTablesFolder externalTablesFolder;
    private YDBStreamingQueriesFolder streamingQueriesFolder;

    public YDBDataSource(
        DBRProgressMonitor monitor,
        DBPDataSourceContainer container,
        GenericMetaModel metaModel
    ) throws DBException {
        super(monitor, container, metaModel, new YDBSQLDialect());
        log.debug("YDBDataSource created");
    }

    @Override
    protected void fillConnectionProperties(DBPConnectionConfiguration connectionInfo, Properties connectProps) {
        super.fillConnectionProperties(connectionInfo, connectProps);

        // Log all properties for debugging
        log.debug("YDB Connection properties being sent to JDBC driver:");
        for (Map.Entry<Object, Object> entry : connectProps.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            // Mask sensitive values
            if (key.toLowerCase().contains("password") || key.toLowerCase().contains("token")) {
                log.debug("  " + key + " = [REDACTED, length=" + value.length() + "]");
            } else {
                log.debug("  " + key + " = " + value);
            }
        }
        log.debug("Connection URL: " + connectionInfo.getUrl());
    }

    /**
     * Build folder hierarchy from flat table names.
     */
    private synchronized void buildHierarchy(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (hierarchyBuilt) {
            return;
        }

        rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        rootTables = new ArrayList<>();

        List<? extends GenericTableBase> allTables = super.getTables(monitor);
        if (allTables == null || allTables.isEmpty()) {
            hierarchyBuilt = true;
            return;
        }

        for (GenericTableBase table : allTables) {
            if (table instanceof YDBTable) {
                YDBTable ydbTable = (YDBTable) table;
                if (ydbTable.hasHierarchicalName()) {
                    String fullName = ydbTable.getFullTableName();
                    String[] parts = fullName.split("/");
                    YDBTableFolder currentFolder = null;

                    // Create folder hierarchy (all parts except the last one)
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
                        currentFolder.addTable(ydbTable);
                    }
                } else {
                    rootTables.add(ydbTable);
                }
            } else {
                rootTables.add(table);
            }
        }

        // Sort root tables
        rootTables.sort(Comparator.comparing(GenericTableBase::getName, String.CASE_INSENSITIVE_ORDER));
        hierarchyBuilt = true;
    }

    @Override
    public Collection<? extends DBSObject> getChildren(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
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
        if (version.contains("main")) {
            return true;
        }
        try {
            // Version format: "major.minor.patch" or similar
            String[] parts = version.split("\\.");
            if (parts.length >= 2) {
                int major = Integer.parseInt(parts[0]);
                int minor = Integer.parseInt(parts[1]);
                return major > 26 || (major == 26 && minor >= 1);
            }
        } catch (NumberFormatException e) {
            log.debug("Failed to parse YDB version: " + version);
        }
        return false;
    }

    @Override
    public synchronized DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        ydbVersion = null;
        hierarchyBuilt = false;
        rootFolders = null;
        rootTables = null;
        systemViewsFolder = null;
        resourcePoolsFolder = null;
        resourcePoolClassifiersFolder = null;
        topicsFolder = null;
        externalDataSourcesFolder = null;
        externalTablesFolder = null;
        streamingQueriesFolder = null;
        return super.refreshObject(monitor);
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
