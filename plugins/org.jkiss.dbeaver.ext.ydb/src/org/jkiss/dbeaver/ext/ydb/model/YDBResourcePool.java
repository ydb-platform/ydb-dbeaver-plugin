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
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;

import java.util.List;

/**
 * YDB Resource Pool.
 * Configuration properties are loaded from .sys/resource_pools system view.
 * Permissions are loaded via SchemeClient from .metadata/workload_manager/pools.
 */
public class YDBResourcePool implements DBSObject, YDBPermissionHolder {

    private static final Log log = Log.getLog(YDBResourcePool.class);

    private final YDBDataSource dataSource;
    private final String name;

    // Configuration properties from .sys/resource_pools (-1 means disabled/not set)
    private final int concurrentQueryLimit;
    private final int queueSize;
    private final double databaseLoadCpuThreshold;
    private final double resourceWeight;
    private final double totalCpuLimitPercentPerNode;
    private final double queryCpuLimitPercentPerNode;
    private final double queryMemoryLimitPercentPerNode;

    // Permissions loaded via SchemeClient
    private String owner;
    private List<YDBPermissionHolder.PermissionEntry> explicitPermissions = List.of();
    private List<YDBPermissionHolder.PermissionEntry> effectivePermissionsEntries = List.of();
    private boolean permissionsLoaded = false;

    public YDBResourcePool(
        @NotNull YDBDataSource dataSource,
        @NotNull String name,
        int concurrentQueryLimit,
        int queueSize,
        double databaseLoadCpuThreshold,
        double resourceWeight,
        double totalCpuLimitPercentPerNode,
        double queryCpuLimitPercentPerNode,
        double queryMemoryLimitPercentPerNode
    ) {
        this.dataSource = dataSource;
        this.name = name;
        this.concurrentQueryLimit = concurrentQueryLimit;
        this.queueSize = queueSize;
        this.databaseLoadCpuThreshold = databaseLoadCpuThreshold;
        this.resourceWeight = resourceWeight;
        this.totalCpuLimitPercentPerNode = totalCpuLimitPercentPerNode;
        this.queryCpuLimitPercentPerNode = queryCpuLimitPercentPerNode;
        this.queryMemoryLimitPercentPerNode = queryMemoryLimitPercentPerNode;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return name;
    }

    @NotNull
    @Property(viewable = true, order = 2)
    public String getConcurrentQueryLimit() {
        return concurrentQueryLimit == -1 ? "unlimited" : String.valueOf(concurrentQueryLimit);
    }

    @NotNull
    @Property(viewable = true, order = 3)
    public String getQueueSize() {
        return queueSize == -1 ? "unlimited" : String.valueOf(queueSize);
    }

    @NotNull
    @Property(viewable = true, order = 4)
    public String getDatabaseLoadCpuThreshold() {
        return databaseLoadCpuThreshold < 0 ? "unlimited" : String.valueOf(databaseLoadCpuThreshold);
    }

    @NotNull
    @Property(viewable = true, order = 5)
    public String getResourceWeight() {
        return resourceWeight < 0 ? "unlimited" : String.valueOf(resourceWeight);
    }

    @NotNull
    @Property(viewable = true, order = 6)
    public String getTotalCpuLimitPercentPerNode() {
        return totalCpuLimitPercentPerNode < 0 ? "unlimited" : String.valueOf(totalCpuLimitPercentPerNode);
    }

    @NotNull
    @Property(viewable = true, order = 7)
    public String getQueryCpuLimitPercentPerNode() {
        return queryCpuLimitPercentPerNode < 0 ? "unlimited" : String.valueOf(queryCpuLimitPercentPerNode);
    }

    @NotNull
    @Property(viewable = true, order = 8)
    public String getQueryMemoryLimitPercentPerNode() {
        return queryMemoryLimitPercentPerNode < 0 ? "unlimited" : String.valueOf(queryMemoryLimitPercentPerNode);
    }

    @Nullable
    @Property(viewable = false, order = 100)
    public String getOwner() {
        return owner;
    }

    @NotNull
    @Override
    public List<YDBPermissionHolder.PermissionEntry> getExplicitPermissions() {
        return explicitPermissions;
    }

    @NotNull
    @Override
    public List<YDBPermissionHolder.PermissionEntry> getEffectivePermissions() {
        return effectivePermissionsEntries;
    }

    @Override
    public void ensurePermissionsLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (permissionsLoaded) {
            return;
        }
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load permissions")) {
            java.sql.Connection conn = session.getOriginal();
            if (conn.isWrapperFor(tech.ydb.jdbc.YdbConnection.class)) {
                tech.ydb.jdbc.YdbConnection ydbConn = conn.unwrap(tech.ydb.jdbc.YdbConnection.class);
                tech.ydb.jdbc.context.YdbContext ctx = ydbConn.getCtx();
                loadPermissions(ctx.getSchemeClient(), ctx.getPrefixPath());
            }
        } catch (Exception e) {
            log.debug("Failed to load permissions: " + e.getMessage());
        }
    }

    synchronized void loadFromEntry(@NotNull SchemeOperationProtos.Entry entry) {
        owner = entry.getOwner();
        explicitPermissions = YDBDescribeHelper.toPermissionEntries(entry.getPermissionsList());
        effectivePermissionsEntries = YDBDescribeHelper.toPermissionEntries(entry.getEffectivePermissionsList());
        permissionsLoaded = true;
    }

    synchronized void loadPermissions(
        @NotNull SchemeClient schemeClient,
        @NotNull String prefixPath
    ) {
        if (permissionsLoaded) {
            return;
        }
        String absolutePath = prefixPath + "/.metadata/workload_manager/pools/" + name;
        SchemeOperationProtos.Entry entry = YDBDescribeHelper.describePath(schemeClient, absolutePath);
        if (entry != null) {
            loadFromEntry(entry);
        } else {
            permissionsLoaded = true;
        }
    }

    @Override
    public void resetPermissions() {
        permissionsLoaded = false;
        owner = null;
        explicitPermissions = new java.util.ArrayList<>();
        effectivePermissionsEntries = new java.util.ArrayList<>();
    }

    @Override
    public void modifyPermissions(
        @NotNull DBRProgressMonitor monitor,
        @NotNull java.util.List<YDBPermissionHolder.PermissionAction> actions,
        boolean clearPermissions,
        @Nullable Boolean interruptInheritance
    ) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Modify permissions")) {
            java.sql.Connection conn = session.getOriginal();
            if (conn.isWrapperFor(tech.ydb.jdbc.YdbConnection.class)) {
                tech.ydb.jdbc.YdbConnection ydbConn = conn.unwrap(tech.ydb.jdbc.YdbConnection.class);
                tech.ydb.jdbc.context.YdbContext ctx = ydbConn.getCtx();
                String absolutePath = ctx.getPrefixPath() + "/.metadata/workload_manager/pools/" + name;
                boolean ok = YDBDescribeHelper.modifyPermissions(
                    ctx.getGrpcTransport(), absolutePath, actions, clearPermissions, interruptInheritance);
                if (!ok) {
                    throw new DBException("Failed to modify permissions on " + absolutePath);
                }
                resetPermissions();
            }
        } catch (java.sql.SQLException e) {
            throw new DBException("Failed to modify permissions", e);
        }
    }

    @Nullable
    @Override
    public String getDescription() {
        return null;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return dataSource;
    }

    @Nullable
    @Override
    public DBSObject getParentObject() {
        return dataSource;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }
}
