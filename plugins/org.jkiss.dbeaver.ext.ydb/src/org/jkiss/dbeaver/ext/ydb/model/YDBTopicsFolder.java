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
 * Container for YDB Topics with hierarchical folder support.
 * Topics are used for message streaming in YDB.
 * Data is loaded by recursively scanning the database directory tree via SchemeClient.
 * Topics with "/" in their paths are organized into nested folders.
 */
public class YDBTopicsFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBTopicsFolder.class);

    private final YDBDataSource dataSource;
    private Map<String, YDBTopicFolder> rootFolders;
    private List<YDBTopic> rootTopics;
    private boolean topicsLoaded = false;

    public YDBTopicsFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "Topics";
    }

    @Override
    public String getDescription() {
        return "YDB Topics for message streaming";
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
        if (rootTopics != null) {
            children.addAll(rootTopics);
        }
        return children;
    }

    @Association
    @NotNull
    public synchronized Collection<YDBTopicFolder> getTopicFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    @Association
    @NotNull
    public synchronized List<YDBTopic> getRootTopics(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootTopics != null ? rootTopics : List.of();
    }

    private synchronized void ensureLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!topicsLoaded) {
            loadTopics(monitor);
        }
    }

    private void loadTopics(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading topics", 1);
        try {
            rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            rootTopics = new ArrayList<>();

            List<YDBTopic> allTopics = new ArrayList<>();

            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load topics")) {
                Connection conn = session.getOriginal();
                if (conn.isWrapperFor(YdbConnection.class)) {
                    YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                    YdbContext ctx = ydbConn.getCtx();
                    SchemeClient schemeClient = ctx.getSchemeClient();
                    String prefixPath = ctx.getPrefixPath();

                    scanForTopics(schemeClient, prefixPath, "", allTopics);
                }
            } catch (SQLException e) {
                log.debug("Failed to load topics via SchemeClient: " + e.getMessage());
            }

            buildHierarchy(allTopics);
            topicsLoaded = true;
        } finally {
            monitor.done();
        }
    }

    private void scanForTopics(SchemeClient schemeClient, String basePath,
                               String relativePath, List<YDBTopic> result) {
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
            if (type == SchemeOperationProtos.Entry.Type.PERS_QUEUE_GROUP
                    || type == SchemeOperationProtos.Entry.Type.TOPIC) {
                result.add(new YDBTopic(dataSource, entryPath));
                log.debug("Found topic: " + entryPath);
            } else if (type == SchemeOperationProtos.Entry.Type.DIRECTORY) {
                scanForTopics(schemeClient, basePath, entryPath, result);
            }
        }
    }

    private void buildHierarchy(@NotNull List<YDBTopic> allTopics) {
        for (YDBTopic topic : allTopics) {
            String relativePath = topic.getFullPath();

            if (relativePath.contains("/")) {
                String[] parts = relativePath.split("/");

                YDBTopicFolder currentFolder = null;
                for (int i = 0; i < parts.length - 1; i++) {
                    String folderName = parts[i];
                    if (folderName.isEmpty()) {
                        continue;
                    }
                    if (currentFolder == null) {
                        currentFolder = rootFolders.computeIfAbsent(
                            folderName,
                            n -> new YDBTopicFolder(this, null, n)
                        );
                    } else {
                        currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                    }
                }

                String shortName = parts[parts.length - 1];
                DBSObject parent = currentFolder != null ? currentFolder : this;
                YDBTopic leafTopic = new YDBTopic(parent, shortName, relativePath);

                if (currentFolder != null) {
                    currentFolder.addTopic(leafTopic);
                } else {
                    rootTopics.add(leafTopic);
                }
            } else {
                rootTopics.add(new YDBTopic(this, relativePath, relativePath));
            }
        }

        rootTopics.sort(Comparator.comparing(YDBTopic::getName, String.CASE_INSENSITIVE_ORDER));
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        topicsLoaded = false;
        rootFolders = null;
        rootTopics = null;
        return this;
    }
}
