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
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSFolder;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.SessionRetryContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Container for YDB External Data Sources with hierarchical folder support.
 */
public class YDBExternalDataSourcesFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBExternalDataSourcesFolder.class);

    private final YDBDataSource dataSource;
    private Map<String, YDBExternalDataSourceFolder> rootFolders;
    private List<YDBExternalDataSource> rootEntries;
    private boolean loaded = false;

    public YDBExternalDataSourcesFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "External Data Sources";
    }

    @Override
    public String getDescription() {
        return "YDB External Data Sources";
    }

    @NotNull
    @Override
    public YDBDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public DBSObject getParentObject() {
        return dataSource;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @NotNull
    @Override
    public Collection<DBSObject> getChildrenObjects(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        List<DBSObject> children = new ArrayList<>();
        if (rootFolders != null) {
            children.addAll(rootFolders.values());
        }
        if (rootEntries != null) {
            children.addAll(rootEntries);
        }
        return children;
    }

    @Association
    @NotNull
    public synchronized Collection<YDBExternalDataSourceFolder> getExternalDataSourceFolders(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    @Association
    @NotNull
    public synchronized List<YDBExternalDataSource> getRootExternalDataSources(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootEntries != null ? rootEntries : List.of();
    }

    private synchronized void ensureLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!loaded) {
            load(monitor);
        }
    }

    private void load(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading external data sources", 1);
        try {
            rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            rootEntries = new ArrayList<>();

            List<YDBExternalDataSource> all = new ArrayList<>();

            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load external data sources")) {
                Connection conn = session.getOriginal();
                if (conn.isWrapperFor(YdbConnection.class)) {
                    YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                    YdbContext ctx = ydbConn.getCtx();
                    SchemeClient schemeClient = ctx.getSchemeClient();
                    String prefixPath = ctx.getPrefixPath();

                    scanEntries(schemeClient, prefixPath, "", all);
                    buildHierarchy(all);

                    GrpcTransport transport = ctx.getGrpcTransport();
                    SessionRetryContext retryCtx = ctx.getRetryCtx();
                    loadAllProperties(transport, retryCtx, prefixPath, schemeClient);
                }
            } catch (SQLException e) {
                log.debug("Failed to load external data sources via SchemeClient: " + e.getMessage());
            }

            loaded = true;
        } finally {
            monitor.done();
        }
    }

    private void loadAllProperties(
        @NotNull GrpcTransport transport,
        @NotNull SessionRetryContext retryCtx,
        @NotNull String prefixPath,
        @NotNull SchemeClient schemeClient
    ) {
        for (YDBExternalDataSource entry : rootEntries) {
            entry.loadProperties(transport, retryCtx, prefixPath, schemeClient);
        }
        for (YDBExternalDataSourceFolder folder : rootFolders.values()) {
            loadFolderProperties(folder, transport, retryCtx, prefixPath, schemeClient);
        }
    }

    private void loadFolderProperties(
        @NotNull YDBExternalDataSourceFolder folder,
        @NotNull GrpcTransport transport,
        @NotNull SessionRetryContext retryCtx,
        @NotNull String prefixPath,
        @NotNull SchemeClient schemeClient
    ) {
        for (YDBExternalDataSource entry : folder.getExternalDataSources()) {
            entry.loadProperties(transport, retryCtx, prefixPath, schemeClient);
        }
        for (YDBExternalDataSourceFolder sub : folder.getSubFolders()) {
            loadFolderProperties(sub, transport, retryCtx, prefixPath, schemeClient);
        }
    }

    private void scanEntries(SchemeClient schemeClient, String basePath,
                             String relativePath, List<YDBExternalDataSource> result) {
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
            if (type == SchemeOperationProtos.Entry.Type.EXTERNAL_DATA_SOURCE) {
                result.add(new YDBExternalDataSource(dataSource, entryPath));
                log.debug("Found external data source: " + entryPath);
            } else if (type == SchemeOperationProtos.Entry.Type.DIRECTORY
                    && !entry.getName().startsWith(".")) {
                scanEntries(schemeClient, basePath, entryPath, result);
            }
        }
    }

    private void buildHierarchy(@NotNull List<YDBExternalDataSource> all) {
        for (YDBExternalDataSource entry : all) {
            String relativePath = entry.getFullPath();

            if (relativePath.contains("/")) {
                String[] parts = relativePath.split("/");

                YDBExternalDataSourceFolder currentFolder = null;
                for (int i = 0; i < parts.length - 1; i++) {
                    String folderName = parts[i];
                    if (folderName.isEmpty()) {
                        continue;
                    }
                    if (currentFolder == null) {
                        currentFolder = rootFolders.computeIfAbsent(
                            folderName,
                            n -> new YDBExternalDataSourceFolder(this, null, n)
                        );
                    } else {
                        currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                    }
                }

                String shortName = parts[parts.length - 1];
                DBSObject parent = currentFolder != null ? currentFolder : this;
                YDBExternalDataSource leaf = new YDBExternalDataSource(parent, shortName, relativePath);

                if (currentFolder != null) {
                    currentFolder.addEntry(leaf);
                } else {
                    rootEntries.add(leaf);
                }
            } else {
                rootEntries.add(new YDBExternalDataSource(this, relativePath, relativePath));
            }
        }

        rootEntries.sort(Comparator.comparing(YDBExternalDataSource::getName, String.CASE_INSENSITIVE_ORDER));
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        loaded = false;
        rootFolders = null;
        rootEntries = null;
        return this;
    }
}
