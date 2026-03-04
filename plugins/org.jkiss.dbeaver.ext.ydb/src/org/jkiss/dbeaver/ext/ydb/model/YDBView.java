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
import org.jkiss.dbeaver.model.DBFetchProgress;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPQualifiedObject;
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.data.DBDDataFilter;
import org.jkiss.dbeaver.model.data.DBDDataReceiver;
import org.jkiss.dbeaver.model.exec.DBCExecutionSource;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatistics;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataContainer;
import org.jkiss.dbeaver.model.struct.DBSEntity;
import org.jkiss.dbeaver.model.struct.DBSEntityAssociation;
import org.jkiss.dbeaver.model.struct.DBSEntityAttribute;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraint;
import org.jkiss.dbeaver.model.struct.DBSEntityType;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * YDB View.
 */
public class YDBView implements DBSEntity, DBPRefreshableObject, DBPScriptObject, DBPQualifiedObject, DBSDataContainer, YDBPermissionHolder {

    private static final long DEFAULT_MAX_ROWS = 200;

    private static final Log log = Log.getLog(YDBView.class);

    private final DBSObject parent;
    private final String name;
    private final String fullPath;

    private boolean propertiesLoaded = false;
    private String queryText;
    private String owner;
    private List<YDBPermissionHolder.PermissionEntry> explicitPermissions = List.of();
    private List<YDBPermissionHolder.PermissionEntry> effectivePermissionsEntries = List.of();
    private boolean permissionsLoaded = false;

    public YDBView(@NotNull DBSObject parent, @NotNull String fullPath) {
        this.parent = parent;
        this.fullPath = fullPath;
        if (fullPath.contains("/")) {
            this.name = fullPath.substring(fullPath.lastIndexOf('/') + 1);
        } else {
            this.name = fullPath;
        }
    }

    public YDBView(@NotNull DBSObject parent, @NotNull String name, @NotNull String fullPath) {
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
    public String getQueryText() {
        return queryText;
    }

    @Nullable
    @Property(viewable = false, order = 100)
    public String getOwner() {
        return owner;
    }

    @NotNull
    @Override
    public String getObjectDefinitionText(@NotNull DBRProgressMonitor monitor, @NotNull Map<String, Object> options) {
        if (queryText == null) {
            loadQueryText(monitor);
        }
        if (queryText == null || queryText.isEmpty()) {
            return "";
        }
        return "CREATE VIEW `" + fullPath + "` WITH (security_invoker = TRUE) AS\n" + queryText;
    }

    private void loadQueryText(@NotNull DBRProgressMonitor monitor) {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load view query text")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                String absolutePath = ctx.getPrefixPath() + "/" + fullPath;
                String text = YDBDescribeHelper.describeViewQueryText(ctx.getGrpcTransport(), absolutePath);
                if (text != null) {
                    queryText = text;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to load view query text: " + e.getMessage());
        }
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
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
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
        } catch (java.sql.SQLException e) {
            throw new DBException("Failed to modify permissions", e);
        }
    }

    synchronized void loadProperties(
        @NotNull GrpcTransport transport,
        @NotNull String prefixPath,
        @Nullable SchemeClient schemeClient
    ) {
        if (propertiesLoaded) {
            return;
        }
        String absolutePath = prefixPath + "/" + fullPath;
        String text = YDBDescribeHelper.describeViewQueryText(transport, absolutePath);
        if (text != null) {
            queryText = text;
        }
        if (schemeClient != null && !permissionsLoaded) {
            loadPermissions(schemeClient, prefixPath);
        }
        propertiesLoaded = true;
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
        queryText = null;
        owner = null;
        explicitPermissions = List.of();
        effectivePermissionsEntries = List.of();
        return this;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(@NotNull DBPEvaluationContext context) {
        return "`" + fullPath + "`";
    }

    @NotNull
    @Override
    public DBSEntityType getEntityType() {
        return DBSEntityType.VIEW;
    }

    @Nullable
    @Override
    public List<? extends DBSEntityAttribute> getAttributes(@NotNull DBRProgressMonitor monitor) throws DBException {
        return null;
    }

    @Nullable
    @Override
    public DBSEntityAttribute getAttribute(@NotNull DBRProgressMonitor monitor, @NotNull String attributeName) throws DBException {
        return null;
    }

    @Nullable
    @Override
    public Collection<? extends DBSEntityConstraint> getConstraints(@NotNull DBRProgressMonitor monitor) throws DBException {
        return null;
    }

    @Nullable
    @Override
    public Collection<? extends DBSEntityAssociation> getAssociations(@NotNull DBRProgressMonitor monitor) throws DBException {
        return null;
    }

    @Nullable
    @Override
    public Collection<? extends DBSEntityAssociation> getReferences(@NotNull DBRProgressMonitor monitor) throws DBException {
        return null;
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
        long effectiveMaxRows = maxRows > 0 ? maxRows : DEFAULT_MAX_ROWS;

        String sql = "SELECT * FROM `" + fullPath + "` LIMIT " + effectiveMaxRows;

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
                DBRProgressMonitor monitor = session.getProgressMonitor();
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
            throw new DBException("Error reading view data", e);
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
