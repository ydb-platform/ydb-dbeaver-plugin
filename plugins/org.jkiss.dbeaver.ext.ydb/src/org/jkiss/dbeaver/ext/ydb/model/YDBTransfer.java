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
import org.jkiss.dbeaver.model.DBIcon;
import org.jkiss.dbeaver.model.DBIconComposite;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPImage;
import org.jkiss.dbeaver.model.DBPImageProvider;
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * YDB Transfer object for data replication between databases.
 */
public class YDBTransfer implements DBSObject, DBPRefreshableObject, DBPScriptObject, DBPImageProvider, YDBPermissionHolder {

    private static final Log log = Log.getLog(YDBTransfer.class);

    private final DBSObject parent;
    private final String name;
    private final String fullPath;

    private boolean propertiesLoaded = false;
    private String sourcePath;
    private String destinationPath;
    private String sourceConnection;
    private String state;
    private String transformationLambda;
    private String consumerName;
    private String owner;
    private String prefixPath;
    private List<YDBPermissionHolder.PermissionEntry> explicitPermissions = List.of();
    private List<YDBPermissionHolder.PermissionEntry> effectivePermissionsEntries = List.of();
    private boolean permissionsLoaded = false;

    public YDBTransfer(@NotNull DBSObject parent, @NotNull String fullPath) {
        this.parent = parent;
        this.fullPath = fullPath;
        if (fullPath.contains("/")) {
            int lastSlash = fullPath.lastIndexOf('/');
            this.name = fullPath.substring(lastSlash + 1);
        } else {
            this.name = fullPath;
        }
    }

    public YDBTransfer(@NotNull DBSObject parent, @NotNull String name, @NotNull String fullPath) {
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
    @Property(viewable = true, order = 3, linkPossible = true)
    public DBSObject getSourcePath(@NotNull DBRProgressMonitor monitor) {
        if (!propertiesLoaded) {
            loadPropertiesFromMonitor(monitor);
        }
        if (sourcePath == null || sourcePath.isEmpty()) {
            return null;
        }
        if (isLocalConnection()) {
            return resolveLinkedObject(monitor, sourcePath);
        }
        return new YDBLinkedObject(this, sourcePath);
    }

    @Nullable
    @Property(viewable = true, order = 4)
    public String getSourceConnection() {
        if (isLocalConnection()) {
            return "(local)";
        }
        return sourceConnection;
    }

    @Nullable
    @Property(viewable = true, order = 5, linkPossible = true)
    public DBSObject getDestinationPath(@NotNull DBRProgressMonitor monitor) {
        if (!propertiesLoaded) {
            loadPropertiesFromMonitor(monitor);
        }
        return resolveLinkedObject(monitor, destinationPath);
    }

    private boolean isLocalConnection() {
        return sourceConnection == null || sourceConnection.isEmpty()
            || sourceConnection.startsWith("grpc:///?");
    }

    @Nullable
    private DBSObject resolveLinkedObject(@NotNull DBRProgressMonitor monitor, @Nullable String rawPath) {
        if (rawPath == null || rawPath.isEmpty()) {
            return null;
        }
        String relativePath = stripPrefix(rawPath);
        YDBDataSource ds = (YDBDataSource) getDataSource();
        DBSObject real = ds.findObjectByFullPath(monitor, relativePath);
        if (real != null) {
            return new YDBLinkedObject(real, relativePath);
        }
        return null;
    }

    @Nullable
    private String stripPrefix(@Nullable String path) {
        if (path == null) {
            return null;
        }
        if (prefixPath != null) {
            String normalizedPrefix = prefixPath.replaceAll("^/+", "/");
            String normalizedPath = path.replaceAll("^/+", "/");
            if (normalizedPath.startsWith(normalizedPrefix + "/")) {
                return normalizedPath.substring(normalizedPrefix.length() + 1);
            }
        }
        if (path.startsWith("/")) {
            return path.replaceAll("^/+", "");
        }
        return path;
    }

    @Nullable
    @Property(viewable = true, order = 6)
    public String getState() {
        return state;
    }

    /**
     * Whether the transfer is in an error state.
     * Reflects the last loaded state; if properties have not been loaded yet, returns false.
     */
    public boolean isInErrorState() {
        return state != null && "error".equalsIgnoreCase(state);
    }

    @Nullable
    @Override
    public DBPImage getObjectImage() {
        if (isInErrorState()) {
            return new DBIconComposite(DBIcon.TREE_TABLE, false, null, null, null, DBIcon.OVER_ERROR);
        }
        return null;
    }

    @Nullable
    @Property(viewable = false, order = 100)
    public String getOwner() {
        return owner;
    }

    synchronized void loadProperties(
        @NotNull GrpcTransport transport,
        @NotNull String prefixPath,
        @Nullable SchemeClient schemeClient
    ) {
        if (propertiesLoaded) {
            return;
        }
        this.prefixPath = prefixPath;
        String absolutePath = prefixPath + "/" + fullPath;
        org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper.TransferInfo info =
            YDBDescribeHelper.describeTransfer(transport, absolutePath);
        if (info != null) {
            sourcePath = info.sourcePath;
            destinationPath = info.destinationPath;
            sourceConnection = info.sourceConnection;
            transformationLambda = info.transformationLambda;
            state = info.state;
            consumerName = info.consumerName;
            owner = info.owner;
            explicitPermissions = YDBDescribeHelper.convertPermissions(info.permissions);
            effectivePermissionsEntries = YDBDescribeHelper.convertPermissions(info.effectivePermissions);
            permissionsLoaded = true;
        } else if (schemeClient != null && !permissionsLoaded) {
            loadPermissions(schemeClient, prefixPath);
        }
        propertiesLoaded = true;
    }

    private void loadPropertiesFromMonitor(@NotNull DBRProgressMonitor monitor) {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load transfer properties")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                loadProperties(ctx.getGrpcTransport(), ctx.getPrefixPath(), ctx.getSchemeClient());
            }
        } catch (Exception e) {
            log.debug("Failed to load transfer properties: " + e.getMessage());
        }
    }

    @NotNull
    @Override
    public String getObjectDefinitionText(@NotNull DBRProgressMonitor monitor, @NotNull Map<String, Object> options) {
        if (!propertiesLoaded) {
            loadPropertiesFromMonitor(monitor);
        }
        StringBuilder sb = new StringBuilder();
        if (transformationLambda != null && !transformationLambda.isEmpty()) {
            // Remove the $__ydb_transfer_lambda assignment line
            String cleaned = transformationLambda.replaceAll("(?m)^\\$__ydb_transfer_lambda\\s*=.*\\n?", "").stripTrailing();
            if (!cleaned.isEmpty()) {
                sb.append(cleaned);
                sb.append("\n\n");
            }
        }
        sb.append("CREATE TRANSFER `").append(fullPath).append("`\n");
        String relSource = sourcePath != null ? stripPrefix(sourcePath) : "";
        String relDest = destinationPath != null ? stripPrefix(destinationPath) : "";
        sb.append("    FROM `").append(relSource != null ? relSource : "").append("`");
        sb.append(" TO `").append(relDest != null ? relDest : "").append("`");
        if (transformationLambda != null && !transformationLambda.isEmpty()) {
            sb.append(" USING $transformation_lambda");
        }
        sb.append(";\n");
        return sb.toString();
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
        loadPropertiesFromMonitor(monitor);
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
        explicitPermissions = new ArrayList<>();
        effectivePermissionsEntries = new ArrayList<>();
    }

    @Override
    public void modifyPermissions(
        @NotNull DBRProgressMonitor monitor,
        @NotNull List<YDBPermissionHolder.PermissionAction> actions,
        boolean clearPermissions,
        @Nullable Boolean interruptInheritance
    ) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Modify permissions")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                String absolutePath = ctx.getPrefixPath() + "/" + fullPath;
                boolean ok = YDBDescribeHelper.modifyPermissions(
                    ctx.getGrpcTransport(), absolutePath, actions, clearPermissions, interruptInheritance);
                if (!ok) {
                    throw new DBException("Failed to modify permissions on " + absolutePath);
                }
                resetPermissions();
            }
        } catch (SQLException e) {
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
        sourcePath = null;
        destinationPath = null;
        sourceConnection = null;
        state = null;
        transformationLambda = null;
        consumerName = null;
        prefixPath = null;
        owner = null;
        explicitPermissions = List.of();
        effectivePermissionsEntries = List.of();
        return this;
    }

    @Override
    public String toString() {
        return name;
    }
}
