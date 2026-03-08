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
 * Container for YDB resource pools.
 * Pool list and configuration are loaded from .sys/resource_pools system view.
 * Permissions are loaded lazily via SchemeClient.
 */
public class YDBResourcePoolsFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBResourcePoolsFolder.class);

    private static final String RESOURCE_POOLS_QUERY =
        org.jkiss.dbeaver.ext.ydb.core.YDBSysQueries.RESOURCE_POOLS_QUERY;

    private final YDBDataSource dataSource;
    private List<YDBResourcePool> resourcePools;
    private boolean poolsLoaded = false;

    public YDBResourcePoolsFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "Resource Pools";
    }

    @Override
    public String getDescription() {
        return "Resource pools for workload management";
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
        return new ArrayList<>(getResourcePools(monitor));
    }

    @Association
    @NotNull
    public synchronized List<YDBResourcePool> getResourcePools(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!poolsLoaded) {
            loadResourcePools(monitor);
        }
        return resourcePools != null ? resourcePools : List.of();
    }

    private void loadResourcePools(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading resource pools", 1);
        try {
            resourcePools = new ArrayList<>();

            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load resource pools")) {
                try (JDBCPreparedStatement stmt = session.prepareStatement(RESOURCE_POOLS_QUERY)) {
                    try (JDBCResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            String name = rs.getString(1);
                            if (name == null || name.isEmpty()) {
                                continue;
                            }
                            int concurrentQueryLimit = rs.getInt(2);
                            if (rs.wasNull()) concurrentQueryLimit = -1;
                            int queueSize = rs.getInt(3);
                            if (rs.wasNull()) queueSize = -1;
                            double databaseLoadCpuThreshold = rs.getDouble(4);
                            if (rs.wasNull()) databaseLoadCpuThreshold = -1;
                            double resourceWeight = rs.getDouble(5);
                            if (rs.wasNull()) resourceWeight = -1;
                            double totalCpuLimitPercentPerNode = rs.getDouble(6);
                            if (rs.wasNull()) totalCpuLimitPercentPerNode = -1;
                            double queryCpuLimitPercentPerNode = rs.getDouble(7);
                            if (rs.wasNull()) queryCpuLimitPercentPerNode = -1;
                            double queryMemoryLimitPercentPerNode = rs.getDouble(8);
                            if (rs.wasNull()) queryMemoryLimitPercentPerNode = -1;

                            resourcePools.add(new YDBResourcePool(
                                dataSource, name,
                                concurrentQueryLimit, queueSize,
                                databaseLoadCpuThreshold, resourceWeight,
                                totalCpuLimitPercentPerNode, queryCpuLimitPercentPerNode,
                                queryMemoryLimitPercentPerNode
                            ));
                        }
                    }
                }
            } catch (SQLException e) {
                log.debug("Failed to load resource pools: " + e.getMessage());
            } catch (DBCException e) {
                log.debug("Failed to open session for loading resource pools: " + e.getMessage());
            }

            resourcePools.sort(Comparator.comparing(YDBResourcePool::getName, String.CASE_INSENSITIVE_ORDER));
            poolsLoaded = true;
        } finally {
            monitor.done();
        }
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        poolsLoaded = false;
        resourcePools = null;
        return this;
    }
}
