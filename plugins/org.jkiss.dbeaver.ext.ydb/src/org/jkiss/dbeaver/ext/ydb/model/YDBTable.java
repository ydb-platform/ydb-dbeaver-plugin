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
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericTableConstraintColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericTableForeignKey;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.model.DBIcon;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPImage;
import org.jkiss.dbeaver.model.DBPImageProvider;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBPUniqueObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.SessionRetryContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * YDB Table with support for hierarchical names.
 * Tables with "/" in their names display only the last part (short name)
 * but use the full name for SQL queries.
 */
public class YDBTable extends GenericTable implements DBPImageProvider, DBPScriptObject, DBPUniqueObject, YDBPermissionHolder {

    private static final DBIcon ICON_TABLE_ROW = new DBIcon(
        "table_row", "platform:/plugin/org.jkiss.dbeaver.ext.ydb/icons/table_row.svg");
    private static final DBIcon ICON_TABLE_COLUMN = new DBIcon(
        "table_column", "platform:/plugin/org.jkiss.dbeaver.ext.ydb/icons/table_column.svg");

    private static final Log log = Log.getLog(YDBTable.class);

    private final String shortName;
    private final String fullName;
    private final boolean columnTable;
    private List<GenericTableColumn> columns;
    private boolean columnsLoaded = false;
    private List<String> primaryKeyColumnNames;
    private List<String> partitionKeyColumnNames;
    private List<GenericUniqueKey> cachedConstraints;
    private String owner;
    private List<YDBPermissionHolder.PermissionEntry> explicitPermissions = List.of();
    private List<YDBPermissionHolder.PermissionEntry> effectivePermissionsEntries = List.of();
    private boolean permissionsLoaded = false;

    public YDBTable(
        GenericStructContainer container,
        @Nullable String tableName,
        @Nullable String tableType,
        @Nullable JDBCResultSet dbResult
    ) {
        this(container, tableName, tableType, dbResult, false);
    }

    public YDBTable(
        GenericStructContainer container,
        @Nullable String tableName,
        @Nullable String tableType,
        @Nullable JDBCResultSet dbResult,
        boolean columnTable
    ) {
        super(container, tableName, tableType, dbResult);
        this.fullName = tableName;
        this.columnTable = columnTable;
        if (tableName != null && tableName.contains("/")) {
            int lastSlash = tableName.lastIndexOf('/');
            this.shortName = tableName.substring(lastSlash + 1);
        } else {
            this.shortName = tableName;
        }
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return shortName != null ? shortName : "";
    }

    @Property(viewable = true, order = 2)
    public String getTableType() {
        return columnTable ? "Column" : "Row";
    }

    public boolean isColumnTable() {
        return columnTable;
    }

    @Nullable
    @Override
    public DBPImage getObjectImage() {
        return columnTable ? ICON_TABLE_COLUMN : ICON_TABLE_ROW;
    }

    @NotNull
    @Override
    public String getUniqueName() {
        return fullName != null ? fullName : "";
    }

    /**
     * Returns the full table name including path prefix.
     * This is used for SQL queries.
     */
    @NotNull
    public String getFullTableName() {
        return fullName != null ? fullName : "";
    }

    /**
     * Check if this table has a hierarchical name (contains "/").
     */
    public boolean hasHierarchicalName() {
        return fullName != null && fullName.contains("/");
    }

    /**
     * Get the folder path (everything before the last "/").
     */
    @Nullable
    public String getFolderPath() {
        if (fullName != null && fullName.contains("/")) {
            int lastSlash = fullName.lastIndexOf('/');
            return fullName.substring(0, lastSlash);
        }
        return null;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        if (context == DBPEvaluationContext.DML || context == DBPEvaluationContext.DDL) {
            return "`" + fullName + "`";
        }
        return super.getFullyQualifiedName(context);
    }

    @Override
    public List<? extends GenericTableColumn> getAttributes(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!columnsLoaded) {
            loadColumns(monitor);
        }
        // Do not fall back to super.getAttributes() — it uses JDBC getColumns()
        // which calls listTables() and recursively lists all directories including .tmp,
        // causing UNAUTHORIZED errors
        return columns != null ? columns : List.of();
    }

    @Override
    public GenericTableColumn getAttribute(@NotNull DBRProgressMonitor monitor, @NotNull String attributeName) throws DBException {
        if (!columnsLoaded) {
            loadColumns(monitor);
        }
        if (columns != null) {
            for (GenericTableColumn column : columns) {
                if (column.getName().equals(attributeName)) {
                    return column;
                }
            }
        }
        return null;
    }

    @Override
    public Collection<? extends GenericTableIndex> getIndexes(@NotNull DBRProgressMonitor monitor) throws DBException {
        return List.of();
    }

    @Override
    public List<GenericUniqueKey> getConstraints(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!columnsLoaded) {
            loadColumns(monitor);
        }
        return cachedConstraints != null ? cachedConstraints : List.of();
    }

    @Override
    public Collection<? extends GenericTableForeignKey> getAssociations(@NotNull DBRProgressMonitor monitor) throws DBException {
        return List.of();
    }

    /**
     * Get primary key column names in their original order.
     * Returns null if columns haven't been loaded yet.
     */
    @Nullable
    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    private synchronized void loadColumns(@NotNull DBRProgressMonitor monitor) {
        if (columnsLoaded) {
            return;
        }
        columns = new ArrayList<>();
        String tablePath = fullName != null ? fullName : shortName;

        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load table columns")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                GrpcTransport transport = ctx.getGrpcTransport();
                SessionRetryContext retryCtx = ctx.getRetryCtx();
                String prefixPath = ctx.getPrefixPath();

                String absolutePath = prefixPath + "/" + tablePath;
                SchemeClient schemeClient = ctx.getSchemeClient();
                if (!permissionsLoaded) {
                    loadPermissions(schemeClient, prefixPath);
                }

                YdbTable.DescribeTableResult result = YDBDescribeHelper.describeTable(
                    transport, retryCtx, absolutePath);

                if (result != null) {
                    primaryKeyColumnNames = new ArrayList<>(result.getPrimaryKeyList());

                    if (result.hasPartitioningSettings()
                            && result.getPartitioningSettings().getPartitionByCount() > 0) {
                        partitionKeyColumnNames = new ArrayList<>(
                            result.getPartitioningSettings().getPartitionByList());
                    }

                    for (int i = 0; i < result.getColumnsCount(); i++) {
                        YdbTable.ColumnMeta colMeta = result.getColumns(i);
                        String typeName = YDBDescribeHelper.resolveTypeName(colMeta.getType());
                        int jdbcType = YDBDescribeHelper.resolveJdbcType(colMeta.getType());
                        int precision = YDBDescribeHelper.getSqlPrecision(colMeta.getType());
                        int scale = YDBDescribeHelper.getSqlScale(colMeta.getType());
                        boolean notNull = YDBDescribeHelper.isNotNull(colMeta);

                        GenericTableColumn column = new GenericTableColumn(
                            this,
                            colMeta.getName(),
                            typeName,
                            jdbcType,
                            jdbcType,      // sourceType
                            i + 1,         // ordinalPosition
                            precision,     // columnSize
                            precision,     // charLength
                            scale > 0 ? scale : null,
                            precision > 0 ? precision : null,
                            10,            // radix
                            notNull,
                            null,          // remarks
                            null,          // defaultValue
                            false,         // autoIncrement
                            false          // autoGenerated
                        );
                        columns.add(column);
                    }

                    reorderColumns();
                    buildPrimaryKeyConstraint();
                    log.debug("Loaded " + result.getColumnsCount() + " columns for table via DescribeTable: " + tablePath);
                } else {
                    log.debug("DescribeTable returned null for: " + tablePath);
                }
            }
        } catch (SQLException e) {
            log.debug("Failed to load columns for table " + tablePath + ": " + e.getMessage());
        } catch (DBCException e) {
            log.debug("Failed to open session for loading table columns: " + e.getMessage());
        }
        columnsLoaded = true;
    }

    private void reorderColumns() {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.isEmpty() || columns == null) {
            return;
        }
        List<GenericTableColumn> reordered = new ArrayList<>(columns.size());
        for (String pkColName : primaryKeyColumnNames) {
            for (GenericTableColumn col : columns) {
                if (col.getName().equals(pkColName)) {
                    reordered.add(col);
                    break;
                }
            }
        }
        java.util.Set<String> pkSet = new java.util.HashSet<>(primaryKeyColumnNames);
        for (GenericTableColumn col : columns) {
            if (!pkSet.contains(col.getName())) {
                reordered.add(col);
            }
        }
        columns = reordered;
        for (int i = 0; i < columns.size(); i++) {
            columns.get(i).setOrdinalPosition(i + 1);
        }
    }

    private void buildPrimaryKeyConstraint() {
        if (primaryKeyColumnNames == null || primaryKeyColumnNames.isEmpty() || columns == null) {
            cachedConstraints = List.of();
            return;
        }

        GenericUniqueKey pk = new GenericUniqueKey(
            this, "PRIMARY_KEY", null, DBSEntityConstraintType.PRIMARY_KEY, true);

        for (int i = 0; i < primaryKeyColumnNames.size(); i++) {
            String pkColName = primaryKeyColumnNames.get(i);
            log.debug("Primary key " + pkColName + " table ["+fullName+"] index is " + i);
            for (GenericTableColumn column : columns) {
                if (column.getName().equals(pkColName)) {
                    pk.addColumn(new GenericTableConstraintColumn(pk, column, i));
                    break;
                }
            }
        }

        if (pk.getAttributeReferences(null) == null || pk.getAttributeReferences(null).isEmpty()) {
            cachedConstraints = List.of();
        } else {
            cachedConstraints = List.of(pk);
            addUniqueKey(pk);
        }
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
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                loadPermissions(ctx.getSchemeClient(), ctx.getPrefixPath());
            }
        } catch (SQLException e) {
            log.debug("Failed to load permissions: " + e.getMessage());
        } catch (DBCException e) {
            log.debug("Failed to open session for loading permissions: " + e.getMessage());
        }
    }

    synchronized void loadPermissions(
        @NotNull SchemeClient schemeClient,
        @NotNull String prefixPath
    ) {
        if (permissionsLoaded) {
            return;
        }
        String tablePath = fullName != null ? fullName : shortName;
        String absolutePath = prefixPath + "/" + tablePath;
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
                String tablePath = fullName != null ? fullName : shortName;
                String absolutePath = ctx.getPrefixPath() + "/" + tablePath;
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

    @NotNull
    @Override
    public String getObjectDefinitionText(
            @NotNull DBRProgressMonitor monitor,
            @NotNull Map<String, Object> options) throws DBException {
        if (!columnsLoaded) {
            loadColumns(monitor);
        }
        if (columns == null || columns.isEmpty()) {
            return "";
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(getFullyQualifiedName(DBPEvaluationContext.DDL)).append(" (\n");

        java.util.Set<String> pkSet = primaryKeyColumnNames != null
            ? new java.util.HashSet<>(primaryKeyColumnNames) : java.util.Set.of();

        for (int i = 0; i < columns.size(); i++) {
            GenericTableColumn col = columns.get(i);
            ddl.append("    `").append(col.getName()).append("` ").append(col.getTypeName());
            if (pkSet.contains(col.getName()) || col.isRequired()) {
                ddl.append(" NOT NULL");
            }
            if (i < columns.size() - 1 || (primaryKeyColumnNames != null && !primaryKeyColumnNames.isEmpty())) {
                ddl.append(",");
            }
            ddl.append("\n");
        }

        if (primaryKeyColumnNames != null && !primaryKeyColumnNames.isEmpty()) {
            ddl.append("    PRIMARY KEY (");
            for (int i = 0; i < primaryKeyColumnNames.size(); i++) {
                if (i > 0) ddl.append(", ");
                ddl.append("`").append(primaryKeyColumnNames.get(i)).append("`");
            }
            ddl.append(")\n");
        }

        ddl.append(")");

        if (columnTable && partitionKeyColumnNames != null && !partitionKeyColumnNames.isEmpty()) {
            ddl.append("\nPARTITION BY HASH(");
            for (int i = 0; i < partitionKeyColumnNames.size(); i++) {
                if (i > 0) ddl.append(", ");
                ddl.append("`").append(partitionKeyColumnNames.get(i)).append("`");
            }
            ddl.append(")");
        }

        if (columnTable) {
            ddl.append("\nWITH (STORE = COLUMN)");
        }

        return ddl.toString();
    }

    @Override
    public String toString() {
        return shortName != null ? shortName : super.toString();
    }
}
