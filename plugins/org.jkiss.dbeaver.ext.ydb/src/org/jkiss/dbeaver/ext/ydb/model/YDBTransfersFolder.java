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
 * Container for YDB Transfers with hierarchical folder support.
 * Transfers are used for data replication between databases in YDB.
 * Data is loaded by recursively scanning the database directory tree via SchemeClient.
 * Transfer entries are detected by type value 23 (TRANSFER in ydb_scheme.proto).
 */
public class YDBTransfersFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBTransfersFolder.class);

    private static final int TRANSFER_TYPE_VALUE = 23;

    private final YDBDataSource dataSource;
    private Map<String, YDBTransferFolder> rootFolders;
    private List<YDBTransfer> rootTransfers;
    private boolean transfersLoaded = false;

    public YDBTransfersFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "Transfers";
    }

    @Override
    public String getDescription() {
        return "YDB Transfers for data replication";
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
        if (rootTransfers != null) {
            children.addAll(rootTransfers);
        }
        return children;
    }

    @Association
    @NotNull
    public synchronized Collection<YDBTransferFolder> getTransferFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    @Association
    @NotNull
    public synchronized List<YDBTransfer> getRootTransfers(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootTransfers != null ? rootTransfers : List.of();
    }

    private synchronized void ensureLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!transfersLoaded) {
            loadTransfers(monitor);
        }
    }

    private void loadTransfers(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading transfers", 1);
        try {
            rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            rootTransfers = new ArrayList<>();

            List<YDBTransfer> allTransfers = new ArrayList<>();

            SchemeClient schemeClient = null;
            GrpcTransport transport = null;
            String prefixPath = null;

            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load transfers")) {
                Connection conn = session.getOriginal();
                if (conn.isWrapperFor(YdbConnection.class)) {
                    YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                    YdbContext ctx = ydbConn.getCtx();
                    schemeClient = ctx.getSchemeClient();
                    transport = ctx.getGrpcTransport();
                    prefixPath = ctx.getPrefixPath();

                    scanForTransfers(schemeClient, prefixPath, "", allTransfers);
                }
            } catch (SQLException e) {
                log.debug("Failed to load transfers via SchemeClient: " + e.getMessage());
            }

            buildHierarchy(allTransfers);

            if (transport != null) {
                loadAllProperties(transport, schemeClient, prefixPath);
            }
            transfersLoaded = true;
        } finally {
            monitor.done();
        }
    }

    private void scanForTransfers(SchemeClient schemeClient, String basePath,
                                  String relativePath, List<YDBTransfer> result) {
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

            if (entry.getTypeValue() == TRANSFER_TYPE_VALUE) {
                result.add(new YDBTransfer(dataSource, entryPath));
                log.debug("Found transfer: " + entryPath);
            } else if (entry.getType() == SchemeOperationProtos.Entry.Type.DIRECTORY
                    && !entry.getName().startsWith(".")) {
                scanForTransfers(schemeClient, basePath, entryPath, result);
            }
        }
    }

    private void buildHierarchy(@NotNull List<YDBTransfer> allTransfers) {
        for (YDBTransfer transfer : allTransfers) {
            String relativePath = transfer.getFullPath();

            if (relativePath.contains("/")) {
                String[] parts = relativePath.split("/");

                YDBTransferFolder currentFolder = null;
                for (int i = 0; i < parts.length - 1; i++) {
                    String folderName = parts[i];
                    if (folderName.isEmpty()) {
                        continue;
                    }
                    if (currentFolder == null) {
                        currentFolder = rootFolders.computeIfAbsent(
                            folderName,
                            n -> new YDBTransferFolder(this, null, n)
                        );
                    } else {
                        currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                    }
                }

                String shortName = parts[parts.length - 1];
                DBSObject parent = currentFolder != null ? currentFolder : this;
                YDBTransfer leafTransfer = new YDBTransfer(parent, shortName, relativePath);

                if (currentFolder != null) {
                    currentFolder.addTransfer(leafTransfer);
                } else {
                    rootTransfers.add(leafTransfer);
                }
            } else {
                rootTransfers.add(new YDBTransfer(this, relativePath, relativePath));
            }
        }

        rootTransfers.sort(Comparator.comparing(YDBTransfer::getName, String.CASE_INSENSITIVE_ORDER));
    }

    private void loadAllProperties(
        @NotNull GrpcTransport transport,
        @Nullable SchemeClient schemeClient,
        @NotNull String prefixPath
    ) {
        if (rootTransfers != null) {
            for (YDBTransfer transfer : rootTransfers) {
                transfer.loadProperties(transport, prefixPath, schemeClient);
            }
        }
        if (rootFolders != null) {
            for (YDBTransferFolder folder : rootFolders.values()) {
                loadFolderProperties(folder, transport, schemeClient, prefixPath);
            }
        }
    }

    private void loadFolderProperties(
        @NotNull YDBTransferFolder folder,
        @NotNull GrpcTransport transport,
        @Nullable SchemeClient schemeClient,
        @NotNull String prefixPath
    ) {
        for (YDBTransfer transfer : folder.getTransfers()) {
            transfer.loadProperties(transport, prefixPath, schemeClient);
        }
        for (YDBTransferFolder sub : folder.getSubFolders()) {
            loadFolderProperties(sub, transport, schemeClient, prefixPath);
        }
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        transfersLoaded = false;
        rootFolders = null;
        rootTransfers = null;
        return this;
    }
}
