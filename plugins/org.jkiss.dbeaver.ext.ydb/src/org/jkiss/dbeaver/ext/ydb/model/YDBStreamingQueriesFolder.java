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
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSFolder;
import org.jkiss.dbeaver.model.struct.DBSObject;

import org.jkiss.dbeaver.ext.ydb.core.YDBStreamingQueryRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Container for YDB streaming queries with hierarchical folder support.
 * Streaming queries are loaded from .sys/streaming_queries system view.
 * The Path field contains hierarchical paths that are split into a tree structure.
 */
public class YDBStreamingQueriesFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBStreamingQueriesFolder.class);

    private static final String STREAMING_QUERIES_QUERY =
        org.jkiss.dbeaver.ext.ydb.core.YDBSysQueries.STREAMING_QUERIES_QUERY;

    private final YDBDataSource dataSource;
    private Map<String, YDBStreamingQueryFolder> rootFolders;
    private List<YDBStreamingQuery> rootQueries;
    private boolean queriesLoaded = false;

    public YDBStreamingQueriesFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "Streaming Queries";
    }

    @Override
    public String getDescription() {
        return "Active streaming queries";
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
        if (rootQueries != null) {
            children.addAll(rootQueries);
        }
        return children;
    }

    /**
     * Get root-level folders (queries with "/" in their Path are grouped here).
     */
    @Association
    @NotNull
    public synchronized Collection<YDBStreamingQueryFolder> getQueryFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    /**
     * Get root-level queries (queries without "/" in their Path).
     */
    @Association
    @NotNull
    public synchronized List<YDBStreamingQuery> getRootQueries(@NotNull DBRProgressMonitor monitor) throws DBException {
        ensureLoaded(monitor);
        return rootQueries != null ? rootQueries : List.of();
    }

    private synchronized void ensureLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!queriesLoaded) {
            loadStreamingQueries(monitor);
        }
    }

    private void loadStreamingQueries(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading streaming queries", 1);
        try {
            rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            rootQueries = new ArrayList<>();

            List<YDBStreamingQuery> allQueries = new ArrayList<>();

            // Query streaming queries from .sys/streaming_queries system view
            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load streaming queries")) {
                try (JDBCPreparedStatement stmt = session.prepareStatement(STREAMING_QUERIES_QUERY)) {
                    try (JDBCResultSet rs = stmt.executeQuery()) {
                        List<YDBStreamingQueryRow> rows =
                            YDBStreamingQueryRow.parseResultSet(rs.getOriginal());
                        for (YDBStreamingQueryRow row : rows) {
                            String relativePath = stripDatabasePrefix(row.path);
                            if (relativePath.isEmpty()) {
                                relativePath = row.path;
                            }
                            allQueries.add(new YDBStreamingQuery(
                                dataSource, relativePath, relativePath,
                                row.status, row.issues, row.plan, row.ast, row.queryText,
                                row.run, row.resourcePool, row.retryCount,
                                row.lastFailAt, row.suspendedUntil
                            ));
                            log.debug("Added streaming query: " + row.path);
                        }
                    }
                }
            } catch (SQLException e) {
                log.debug("Failed to load streaming queries: " + e.getMessage());
                // Streaming queries may not be available in all YDB configurations
            } catch (DBCException e) {
                log.debug("Failed to open session for loading streaming queries: " + e.getMessage());
            }

            // Build hierarchy from paths
            buildHierarchy(allQueries);
            queriesLoaded = true;

        } finally {
            monitor.done();
        }
    }

    /**
     * Get the database path prefix from the connection configuration.
     * This prefix is stripped from query Path values before building the hierarchy.
     * For example, if database is "/ydb-cluster/mydb" and Path is
     * "/ydb-cluster/mydb/folder/query", the relative path becomes "folder/query".
     */
    @NotNull
    private String getDatabasePrefix() {
        String dbName = dataSource.getContainer().getConnectionConfiguration().getDatabaseName();
        if (dbName == null || dbName.isEmpty()) {
            return "";
        }
        // Normalize: ensure it starts with "/" and ends with "/"
        if (!dbName.startsWith("/")) {
            dbName = "/" + dbName;
        }
        if (!dbName.endsWith("/")) {
            dbName = dbName + "/";
        }
        return dbName;
    }

    /**
     * Strip the database path prefix from a full query Path.
     */
    @NotNull
    private String stripDatabasePrefix(@NotNull String fullPath) {
        String prefix = getDatabasePrefix();
        if (!prefix.isEmpty() && fullPath.startsWith(prefix)) {
            return fullPath.substring(prefix.length());
        }
        // Fallback: strip leading "/"
        String result = fullPath;
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }

    /**
     * Split flat query list into hierarchical folder structure based on "/" in Path.
     * The database name prefix is stripped from paths before building the tree.
     */
    private void buildHierarchy(@NotNull List<YDBStreamingQuery> allQueries) {
        for (YDBStreamingQuery query : allQueries) {
            String relativePath = query.getFullPath();

            if (relativePath.contains("/")) {
                String[] parts = relativePath.split("/");

                // Build folder chain (all parts except the last one)
                YDBStreamingQueryFolder currentFolder = null;
                for (int i = 0; i < parts.length - 1; i++) {
                    String folderName = parts[i];
                    if (folderName.isEmpty()) {
                        continue;
                    }
                    if (currentFolder == null) {
                        currentFolder = rootFolders.computeIfAbsent(
                            folderName,
                            n -> new YDBStreamingQueryFolder(this, null, n)
                        );
                    } else {
                        currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                    }
                }

                // Last segment is the query name
                String shortName = parts[parts.length - 1];
                DBSObject parent = currentFolder != null ? currentFolder : this;
                YDBStreamingQuery leafQuery = new YDBStreamingQuery(parent, shortName, query);

                if (currentFolder != null) {
                    currentFolder.addQuery(leafQuery);
                } else {
                    rootQueries.add(leafQuery);
                }
            } else {
                // No "/" — root-level query
                rootQueries.add(new YDBStreamingQuery(this, relativePath, query));
            }
        }

        // Sort root queries
        rootQueries.sort(Comparator.comparing(YDBStreamingQuery::getName, String.CASE_INSENSITIVE_ORDER));
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        queriesLoaded = false;
        rootFolders = null;
        rootQueries = null;
        return this;
    }
}
