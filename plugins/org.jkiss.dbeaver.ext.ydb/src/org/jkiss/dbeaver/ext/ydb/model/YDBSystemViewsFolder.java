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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Container for YDB system views from .sys directory.
 * System views provide metadata about the database.
 */
public class YDBSystemViewsFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBSystemViewsFolder.class);

    private final YDBDataSource dataSource;
    private List<YDBSystemView> systemViews;
    private boolean viewsLoaded = false;

    public YDBSystemViewsFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "System views";
    }

    @Override
    public String getDescription() {
        return "System views from .sys directory";
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
        List<DBSObject> children = new ArrayList<>();
        children.addAll(getSystemViews(monitor));
        return children;
    }

    /**
     * Load system views from .sys directory.
     */
    @Association
    @NotNull
    public synchronized List<YDBSystemView> getSystemViews(@NotNull DBRProgressMonitor monitor) throws DBException {
        log.debug("getSystemViews() called, viewsLoaded=" + viewsLoaded);
        if (!viewsLoaded) {
            loadSystemViews(monitor);
        }
        log.debug("Returning " + (systemViews != null ? systemViews.size() : 0) + " system views");
        return systemViews != null ? systemViews : List.of();
    }

    private void loadSystemViews(@NotNull DBRProgressMonitor monitor) throws DBException {
        // Known YDB system views from .sys directory
        // Reference: https://ydb.tech/docs/en/devops/manual/system-views
        String[] knownViews = {
            // Query statistics views
            "query_sessions",                      // Active query sessions

            // Top queries views
            "top_queries_by_duration_one_minute",
            "top_queries_by_duration_one_hour",
        };

        monitor.beginTask("Loading system views", knownViews.length);
        try {
            systemViews = new ArrayList<>();

            for (String viewName : knownViews) {
                if (monitor.isCanceled()) {
                    break;
                }
                monitor.subTask("Loading " + viewName);

                YDBSystemView systemView = new YDBSystemView(dataSource, viewName, null);

                // Try to load column structure by executing SELECT with LIMIT 0
                loadViewColumns(monitor, systemView, viewName);

                systemViews.add(systemView);
                log.debug("Added system view: " + viewName);
                monitor.worked(1);
            }

            // Sort views by name
            systemViews.sort(Comparator.comparing(YDBSystemView::getName, String.CASE_INSENSITIVE_ORDER));
            viewsLoaded = true;

        } finally {
            monitor.done();
        }
    }

    /**
     * Load column structure for a system view by executing SELECT * FROM .sys/<view_name> LIMIT 0.
     * This allows us to get the column metadata from the result set.
     */
    private void loadViewColumns(@NotNull DBRProgressMonitor monitor, @NotNull YDBSystemView systemView, @NotNull String viewName) {
        String query = "SELECT * FROM `.sys/" + viewName + "` LIMIT 0";

        try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load system view columns")) {
            try (JDBCPreparedStatement stmt = session.prepareStatement(query)) {
                try (JDBCResultSet rs = stmt.executeQuery()) {
                    // Get column metadata from result set
                    java.sql.ResultSetMetaData metaData = rs.getOriginal().getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        String typeName = metaData.getColumnTypeName(i);
                        int jdbcType = metaData.getColumnType(i);
                        int precision = metaData.getPrecision(i);
                        int scale = metaData.getScale(i);
                        boolean nullable = metaData.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls;

                        systemView.addColumnInfo(columnName, typeName, jdbcType, precision, scale, nullable, i);
                    }

                    log.debug("Loaded " + columnCount + " columns for system view: " + viewName);
                }
            }
        } catch (SQLException e) {
            log.debug("Failed to load columns for system view " + viewName + ": " + e.getMessage());
        } catch (DBCException e) {
            log.debug("Failed to open session for loading system view columns: " + e.getMessage());
        }
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        viewsLoaded = false;
        systemViews = null;
        return this;
    }
}
