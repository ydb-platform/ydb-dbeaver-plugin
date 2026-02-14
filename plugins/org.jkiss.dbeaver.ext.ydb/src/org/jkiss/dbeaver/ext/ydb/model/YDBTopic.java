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
import org.jkiss.dbeaver.model.DBFetchProgress;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.data.DBDDataFilter;
import org.jkiss.dbeaver.model.data.DBDDataReceiver;
import org.jkiss.dbeaver.model.exec.DBCExecutionSource;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatistics;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;

import java.sql.SQLException;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataContainer;
import org.jkiss.dbeaver.model.struct.DBSObject;

/**
 * YDB Topic (PersQueueGroup).
 * Topics are used for message streaming in YDB.
 */
public class YDBTopic implements DBSObject, DBSDataContainer {

    private static final long DEFAULT_MAX_ROWS = 200;

    private final DBSObject parent;
    private final String name;
    private final String fullPath;

    public YDBTopic(@NotNull DBSObject parent, @NotNull String fullPath) {
        this.parent = parent;
        this.fullPath = fullPath;
        if (fullPath.contains("/")) {
            int lastSlash = fullPath.lastIndexOf('/');
            this.name = fullPath.substring(lastSlash + 1);
        } else {
            this.name = fullPath;
        }
    }

    public YDBTopic(@NotNull DBSObject parent, @NotNull String name, @NotNull String fullPath) {
        this.parent = parent;
        this.name = name;
        this.fullPath = fullPath;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return name;
    }

    @NotNull
    @Property(viewable = true, order = 2)
    public String getFullPath() {
        return fullPath;
    }

    @Nullable
    @Override
    public String getDescription() {
        return null;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return parent.getDataSource();
    }

    @Override
    public DBSObject getParentObject() {
        return parent;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @NotNull
    @Override
    public String[] getSupportedFeatures() {
        return new String[]{FEATURE_DATA_SELECT};
    }

    @NotNull
    @Override
    public DBCStatistics readData(
        @Nullable DBCExecutionSource source,
        @NotNull DBCSession session,
        @NotNull DBDDataReceiver dataReceiver,
        @Nullable DBDDataFilter dataFilter,
        long firstRow,
        long maxRows,
        long flags,
        int fetchSize
    ) throws DBException {
        YDBDataSource ydbDataSource = (YDBDataSource) getDataSource();
        DBRProgressMonitor monitor = session.getProgressMonitor();

        if (!ydbDataSource.isTopicReadingSupported(monitor)) {
            throw new DBException("This functionality is available in YDB version 26.1 or higher");
        }

        long effectiveMaxRows = maxRows > 0 ? maxRows : DEFAULT_MAX_ROWS;

        String sql = "SELECT * FROM `" + fullPath + "` WITH (FORMAT = \"raw\", SCHEMA = (data String), STREAMING = \"TRUE\") LIMIT " + effectiveMaxRows;

        DBCStatistics statistics = new DBCStatistics();
        statistics.setQueryText(sql);

        long startTime = System.currentTimeMillis();

        JDBCSession jdbcSession = (JDBCSession) session;
        try (JDBCPreparedStatement stmt = jdbcSession.prepareStatement(sql)) {
            JDBCResultSet resultSet = stmt.executeQuery();
            statistics.addExecuteTime(System.currentTimeMillis() - startTime);
            statistics.addStatementsCount();

            DBDDataReceiver.startFetchWorkflow(dataReceiver, session, resultSet, firstRow, effectiveMaxRows);

            try (resultSet) {
                DBFetchProgress fetchProgress = new DBFetchProgress(monitor);
                long fetchStartTime = System.currentTimeMillis();

                while (!fetchProgress.isMaxRowsFetched(effectiveMaxRows) && !fetchProgress.isCanceled() && resultSet.nextRow()) {
                    dataReceiver.fetchRow(session, resultSet);
                    fetchProgress.monitorRowFetch();
                }

                statistics.addFetchTime(System.currentTimeMillis() - fetchStartTime);
                statistics.setRowsFetched(fetchProgress.getRowCount());
            }
        } catch (SQLException e) {
            throw new DBException("Error reading topic data", e);
        }

        return statistics;
    }

    @Override
    public long countData(
        @NotNull DBCExecutionSource source,
        @NotNull DBCSession session,
        @Nullable DBDDataFilter dataFilter,
        long flags
    ) throws DBException {
        return -1;
    }

    @Override
    public String toString() {
        return name;
    }
}
