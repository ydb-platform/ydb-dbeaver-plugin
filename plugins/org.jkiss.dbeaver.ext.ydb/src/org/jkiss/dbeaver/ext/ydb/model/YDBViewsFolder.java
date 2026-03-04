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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Container for YDB Views with hierarchical folder support.
 */
public class YDBViewsFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBViewsFolder.class);

    /** Entry.Type.VIEW numeric value in ydb_scheme.proto */
    private static final int ENTRY_TYPE_VIEW = 20;

    private final YDBDataSource dataSource;
    private Map<String, YDBViewFolder> rootFolders;
    private List<YDBView> rootEntries;
    private boolean loaded = false;

    public YDBViewsFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "Views";
    }

    @Override
    public String getDescription() {
        return "YDB Views";
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
    public synchronized Collection<YDBViewFolder> getViewFolders(
            @NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    @Association
    @NotNull
    public synchronized List<YDBView> getRootViews(
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
        monitor.beginTask("Loading views", 1);
        try {
            rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            rootEntries = new ArrayList<>();

            List<YDBView> all = new ArrayList<>();

            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load views")) {
                Connection conn = session.getOriginal();
                if (conn.isWrapperFor(YdbConnection.class)) {
                    YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                    YdbContext ctx = ydbConn.getCtx();
                    SchemeClient schemeClient = ctx.getSchemeClient();
                    String prefixPath = ctx.getPrefixPath();

                    scanEntries(schemeClient, prefixPath, "", all);
                    buildHierarchy(all);

                    GrpcTransport transport = ctx.getGrpcTransport();
                    loadAllProperties(transport, prefixPath, schemeClient);
                }
            } catch (SQLException e) {
                log.debug("Failed to load views via SchemeClient: " + e.getMessage());
            }

            loaded = true;
        } finally {
            monitor.done();
        }
    }

    private void loadAllProperties(
        @NotNull GrpcTransport transport,
        @NotNull String prefixPath,
        @NotNull SchemeClient schemeClient
    ) {
        for (YDBView entry : rootEntries) {
            entry.loadProperties(transport, prefixPath, schemeClient);
        }
        for (YDBViewFolder folder : rootFolders.values()) {
            loadFolderProperties(folder, transport, prefixPath, schemeClient);
        }
    }

    private void loadFolderProperties(
        @NotNull YDBViewFolder folder,
        @NotNull GrpcTransport transport,
        @NotNull String prefixPath,
        @NotNull SchemeClient schemeClient
    ) {
        for (YDBView entry : folder.getViews()) {
            entry.loadProperties(transport, prefixPath, schemeClient);
        }
        for (YDBViewFolder sub : folder.getSubFolders()) {
            loadFolderProperties(sub, transport, prefixPath, schemeClient);
        }
    }

    private void scanEntries(SchemeClient schemeClient, String basePath,
                             String relativePath, List<YDBView> result) {
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

            int typeValue = entry.getTypeValue();
            if (typeValue == ENTRY_TYPE_VIEW) {
                result.add(new YDBView(dataSource, entryPath));
                log.debug("Found view: " + entryPath);
            } else if (entry.getType() == SchemeOperationProtos.Entry.Type.DIRECTORY
                    && !entry.getName().startsWith(".")) {
                scanEntries(schemeClient, basePath, entryPath, result);
            }
        }
    }

    private void buildHierarchy(@NotNull List<YDBView> all) {
        for (YDBView entry : all) {
            String relativePath = entry.getFullPath();

            if (relativePath.contains("/")) {
                String[] parts = relativePath.split("/");

                YDBViewFolder currentFolder = null;
                for (int i = 0; i < parts.length - 1; i++) {
                    String folderName = parts[i];
                    if (folderName.isEmpty()) {
                        continue;
                    }
                    if (currentFolder == null) {
                        currentFolder = rootFolders.computeIfAbsent(
                            folderName,
                            n -> new YDBViewFolder(dataSource, null, n)
                        );
                    } else {
                        currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                    }
                }

                String shortName = parts[parts.length - 1];
                DBSObject parent = currentFolder != null ? currentFolder : dataSource;
                YDBView leaf = new YDBView(parent, shortName, relativePath);

                if (currentFolder != null) {
                    currentFolder.addEntry(leaf);
                } else {
                    rootEntries.add(leaf);
                }
            } else {
                rootEntries.add(new YDBView(dataSource, relativePath, relativePath));
            }
        }

        rootEntries.sort(Comparator.comparing(YDBView::getName, String.CASE_INSENSITIVE_ORDER));
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        loaded = false;
        rootFolders = null;
        rootEntries = null;
        return this;
    }
}
