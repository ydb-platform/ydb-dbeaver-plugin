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
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.SessionRetryContext;

import java.util.Map;

/**
 * YDB External Data Source.
 */
public class YDBExternalDataSource implements DBSObject, DBPRefreshableObject, YDBPermissionHolder {

    private static final Log log = Log.getLog(YDBExternalDataSource.class);

    private final DBSObject parent;
    private final String name;
    private final String fullPath;

    private boolean propertiesLoaded = false;
    private String sourceType;
    private String location;
    private String databaseName;
    private String authMethod;
    private String tokenSecretPath;
    private String installation;
    private String owner;
    private java.util.List<YDBPermissionHolder.PermissionEntry> explicitPermissions = java.util.List.of();
    private java.util.List<YDBPermissionHolder.PermissionEntry> effectivePermissionsEntries = java.util.List.of();
    private boolean permissionsLoaded = false;

    public YDBExternalDataSource(@NotNull DBSObject parent, @NotNull String fullPath) {
        this.parent = parent;
        this.fullPath = fullPath;
        if (fullPath.contains("/")) {
            this.name = fullPath.substring(fullPath.lastIndexOf('/') + 1);
        } else {
            this.name = fullPath;
        }
    }

    public YDBExternalDataSource(@NotNull DBSObject parent, @NotNull String name, @NotNull String fullPath) {
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
    @Property(viewable = true, order = 3)
    public String getSourceType() {
        return sourceType;
    }

    @Nullable
    @Property(viewable = true, order = 4)
    public String getLocation() {
        return location;
    }

    @Nullable
    @Property(order = 5)
    public String getDatabaseName() {
        return databaseName;
    }

    @Nullable
    @Property(order = 6)
    public String getAuthMethod() {
        return authMethod;
    }

    @Nullable
    @Property(order = 7)
    public String getTokenSecretPath() {
        return tokenSecretPath;
    }

    @Nullable
    @Property(order = 8)
    public String getInstallation() {
        return installation;
    }

    @Nullable
    @Property(viewable = false, order = 100)
    public String getOwner() {
        return owner;
    }

    @NotNull
    @Override
    public java.util.List<YDBPermissionHolder.PermissionEntry> getExplicitPermissions() {
        return explicitPermissions;
    }

    @NotNull
    @Override
    public java.util.List<YDBPermissionHolder.PermissionEntry> getEffectivePermissions() {
        return effectivePermissionsEntries;
    }

    @Override
    public void ensurePermissionsLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (permissionsLoaded) {
            return;
        }
        try (org.jkiss.dbeaver.model.exec.jdbc.JDBCSession session =
                 org.jkiss.dbeaver.model.DBUtils.openMetaSession(monitor, getDataSource(), "Load permissions")) {
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

    synchronized void loadPermissions(
        @NotNull SchemeClient schemeClient,
        @NotNull String prefixPath
    ) {
        if (permissionsLoaded) {
            return;
        }
        String absolutePath = prefixPath + "/" + fullPath;
        SchemeOperationProtos.Entry entry = YDBDescribeHelper.describePath(schemeClient, absolutePath);
        if (entry != null) {
            owner = entry.getOwner();
            explicitPermissions = YDBDescribeHelper.toPermissionEntries(entry.getPermissionsList());
            effectivePermissionsEntries = YDBDescribeHelper.toPermissionEntries(entry.getEffectivePermissionsList());
        }
        permissionsLoaded = true;
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
        try (org.jkiss.dbeaver.model.exec.jdbc.JDBCSession session =
                 org.jkiss.dbeaver.model.DBUtils.openMetaSession(monitor, getDataSource(), "Modify permissions")) {
            java.sql.Connection conn = session.getOriginal();
            if (conn.isWrapperFor(tech.ydb.jdbc.YdbConnection.class)) {
                tech.ydb.jdbc.YdbConnection ydbConn = conn.unwrap(tech.ydb.jdbc.YdbConnection.class);
                tech.ydb.jdbc.context.YdbContext ctx = ydbConn.getCtx();
                String absolutePath = ctx.getPrefixPath() + "/" + fullPath;
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

    synchronized void loadProperties(
        @NotNull GrpcTransport transport,
        @NotNull SessionRetryContext retryCtx,
        @NotNull String prefixPath,
        @Nullable SchemeClient schemeClient
    ) {
        if (propertiesLoaded) {
            return;
        }
        String absolutePath = prefixPath + "/" + fullPath;
        org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper.ExternalDataSourceInfo info =
            YDBDescribeHelper.describeExternalDataSource(transport, absolutePath);
        if (info != null) {
            sourceType = info.sourceType;
            location = info.location;
            owner = info.owner;
            explicitPermissions = YDBDescribeHelper.convertPermissions(info.permissions);
            effectivePermissionsEntries = YDBDescribeHelper.convertPermissions(info.effectivePermissions);
            permissionsLoaded = true;

            // Additional properties from the map
            Map<String, String> props = info.properties;
            databaseName = getPropertyIgnoreCase(props, "DATABASE_NAME");
            authMethod = getPropertyIgnoreCase(props, "AUTH_METHOD");
            tokenSecretPath = getPropertyIgnoreCase(props, "TOKEN_SECRET_PATH");
            if (tokenSecretPath == null) {
                tokenSecretPath = getPropertyIgnoreCase(props, "TOKEN_SECRET_NAME");
            }
            installation = getPropertyIgnoreCase(props, "INSTALLATION");

            log.debug("External data source: sourceType=" + sourceType
                + ", location=" + location + ", properties=" + props);
        } else if (schemeClient != null && !permissionsLoaded) {
            loadPermissions(schemeClient, prefixPath);
        }
        propertiesLoaded = true;
    }

    @Nullable
    private static String getPropertyIgnoreCase(@NotNull Map<String, String> props, @NotNull String key) {
        String value = props.get(key);
        if (value == null) {
            value = props.get(key.toLowerCase(java.util.Locale.ROOT));
        }
        return value;
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

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        propertiesLoaded = false;
        permissionsLoaded = false;
        sourceType = null;
        location = null;
        databaseName = null;
        authMethod = null;
        tokenSecretPath = null;
        installation = null;
        owner = null;
        explicitPermissions = java.util.List.of();
        effectivePermissionsEntries = java.util.List.of();
        return this;
    }

    @Override
    public String toString() {
        return name;
    }
}
