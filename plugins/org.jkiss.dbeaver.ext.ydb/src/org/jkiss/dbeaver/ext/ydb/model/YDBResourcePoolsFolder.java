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
 * Resource pools are used for workload management in YDB.
 * Data is loaded from .sys/resource_pools system view.
 */
public class YDBResourcePoolsFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBResourcePoolsFolder.class);

    private static final String RESOURCE_POOLS_QUERY = "SELECT * FROM `.sys/resource_pools`";

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
        List<DBSObject> children = new ArrayList<>();
        children.addAll(getResourcePools(monitor));
        return children;
    }

    /**
     * Load resource pools from .sys/resource_pools system view.
     */
    @Association
    @NotNull
    public synchronized List<YDBResourcePool> getResourcePools(@NotNull DBRProgressMonitor monitor) throws DBException {
        log.debug("getResourcePools() called, poolsLoaded=" + poolsLoaded);
        if (!poolsLoaded) {
            loadResourcePools(monitor);
        }
        log.debug("Returning " + (resourcePools != null ? resourcePools.size() : 0) + " resource pools");
        return resourcePools != null ? resourcePools : List.of();
    }

    private void loadResourcePools(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading resource pools", 1);
        try {
            resourcePools = new ArrayList<>();

            // Query resource pools from .sys/resource_pools system view
            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load resource pools")) {
                try (JDBCPreparedStatement stmt = session.prepareStatement(RESOURCE_POOLS_QUERY)) {
                    try (JDBCResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            // The first column should be the pool name
                            String poolName = rs.getString(1);
                            if (poolName != null && !poolName.isEmpty()) {
                                YDBResourcePool pool = new YDBResourcePool(dataSource, poolName);
                                resourcePools.add(pool);
                                log.debug("Added resource pool: " + poolName);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                log.debug("Failed to load resource pools: " + e.getMessage());
                // Resource pools may not be available in all YDB configurations
            } catch (DBCException e) {
                log.debug("Failed to open session for loading resource pools: " + e.getMessage());
            }

            // Sort pools by name
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
