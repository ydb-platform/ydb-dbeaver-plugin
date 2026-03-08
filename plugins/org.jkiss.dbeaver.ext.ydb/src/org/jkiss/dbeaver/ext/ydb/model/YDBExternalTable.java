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
import org.jkiss.dbeaver.ext.generic.model.GenericTableForeignKey;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.table.SessionRetryContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * YDB External Table.
 * Extends GenericTable to enable data viewing, pagination, and standard DBeaver table functionality.
 */
public class YDBExternalTable extends GenericTable implements YDBPermissionHolder, DBPScriptObject {

    private static final Log log = Log.getLog(YDBExternalTable.class);

    private final String shortName;
    private final String fullPath;

    private boolean propertiesLoaded = false;
    private boolean columnsLoaded = false;
    private String dataSourcePath;
    private String sourceType;
    private String location;
    private String format;
    private String compression;
    private List<GenericTableColumn> columns;
    private String owner;
    private List<YDBPermissionHolder.PermissionEntry> explicitPermissions = List.of();
    private List<YDBPermissionHolder.PermissionEntry> effectivePermissionsEntries = List.of();
    private boolean permissionsLoaded = false;

    public YDBExternalTable(
        @NotNull GenericStructContainer container,
        @NotNull String shortName,
        @NotNull String fullPath
    ) {
        super(container, fullPath, "EXTERNAL TABLE", (JDBCResultSet) null);
        this.shortName = shortName;
        this.fullPath = fullPath;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return shortName;
    }

    @NotNull
    @Property(viewable = true, order = 2)
    public String getFullPath() {
        return fullPath;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        if (context == DBPEvaluationContext.DML || context == DBPEvaluationContext.DDL) {
            return "`" + fullPath + "`";
        }
        return super.getFullyQualifiedName(context);
    }

    @Override
    public boolean isView() {
        return false;
    }

    @Override
    public boolean isSystem() {
        return false;
    }

    @Nullable
    @Property(viewable = true, order = 3)
    public String getDataSourcePath() {
        return dataSourcePath;
    }

    @Nullable
    @Property(viewable = true, order = 4)
    public String getSourceType() {
        return sourceType;
    }

    @Nullable
    @Property(order = 5)
    public String getLocation() {
        return location;
    }

    @Nullable
    @Property(order = 6)
    public String getFormat() {
        return format;
    }

    @Nullable
    @Property(order = 7)
    public String getCompression() {
        return compression;
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
        try (org.jkiss.dbeaver.model.exec.jdbc.JDBCSession session =
                 org.jkiss.dbeaver.model.DBUtils.openMetaSession(monitor, getDataSource(), "Load permissions")) {
            java.sql.Connection conn = session.getOriginal();
            if (conn.isWrapperFor(tech.ydb.jdbc.YdbConnection.class)) {
                tech.ydb.jdbc.YdbConnection ydbConn = conn.unwrap(tech.ydb.jdbc.YdbConnection.class);
                tech.ydb.jdbc.context.YdbContext ctx = ydbConn.getCtx();
                loadPermissions(ctx.getSchemeClient(), ctx.getPrefixPath());
            }
        } catch (SQLException e) {
            // ignore
        } catch (org.jkiss.dbeaver.model.exec.DBCException e) {
            // ignore
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

    @Nullable
    public YDBExternalDataSource getDataSourceObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (dataSourcePath == null) {
            return null;
        }
        DBPDataSource ds = getDataSource();
        if (ds instanceof YDBDataSource) {
            return ((YDBDataSource) ds).findExternalDataSource(monitor, dataSourcePath);
        }
        return null;
    }

    @Override
    public List<? extends GenericTableColumn> getAttributes(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (!columnsLoaded) {
            loadColumns(monitor);
        }
        // Do not fall back to super.getAttributes() — it uses JDBC getTables()
        // which recursively lists all directories including .tmp, causing UNAUTHORIZED errors
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
        return List.of();
    }

    @Override
    public Collection<? extends GenericTableForeignKey> getAssociations(@NotNull DBRProgressMonitor monitor) throws DBException {
        return List.of();
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
        org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper.ExternalTableInfo info =
            YDBDescribeHelper.describeExternalTable(transport, absolutePath);
        if (info != null) {
            sourceType = info.sourceType;
            dataSourcePath = info.dataSourcePath;
            location = info.location;
            owner = info.owner;
            explicitPermissions = YDBDescribeHelper.convertPermissions(info.permissions);
            effectivePermissionsEntries = YDBDescribeHelper.convertPermissions(info.effectivePermissions);
            permissionsLoaded = true;

            Map<String, String> content = info.content;
            format = unwrapJsonArrayValue(getPropertyIgnoreCase(content, "FORMAT"));
            compression = unwrapJsonArrayValue(getPropertyIgnoreCase(content, "COMPRESSION"));

            log.debug("External table: sourceType=" + sourceType
                + ", dataSourcePath=" + dataSourcePath
                + ", location=" + location + ", content=" + content);

            if (!columnsLoaded && !info.columns.isEmpty()) {
                parseColumns(info.columns);
            }
        } else {
            // Fallback: try generic DescribeTable
            YdbTable.DescribeTableResult result = YDBDescribeHelper.describeTable(
                transport, retryCtx, absolutePath);
            if (result != null) {
                parseAttributes(result.getAttributesMap());
                if (!columnsLoaded) {
                    parseColumnsFromDescribeTable(result);
                }
            }
            if (schemeClient != null && !permissionsLoaded) {
                loadPermissions(schemeClient, prefixPath);
            }
        }
        propertiesLoaded = true;
    }

    private synchronized void loadColumns(@NotNull DBRProgressMonitor monitor) {
        if (columnsLoaded) {
            return;
        }
        columns = new ArrayList<>();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load external table columns")) {
            java.sql.Connection conn = session.getOriginal();
            if (conn.isWrapperFor(tech.ydb.jdbc.YdbConnection.class)) {
                tech.ydb.jdbc.YdbConnection ydbConn = conn.unwrap(tech.ydb.jdbc.YdbConnection.class);
                tech.ydb.jdbc.context.YdbContext ctx = ydbConn.getCtx();
                GrpcTransport transport = ctx.getGrpcTransport();
                SessionRetryContext retryCtx = ctx.getRetryCtx();
                String prefixPath = ctx.getPrefixPath();

                if (!permissionsLoaded) {
                    SchemeClient schemeClient = ctx.getSchemeClient();
                    loadPermissions(schemeClient, prefixPath);
                }

                String absolutePath = prefixPath + "/" + fullPath;
                org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper.ExternalTableInfo info =
                    YDBDescribeHelper.describeExternalTable(transport, absolutePath);

                if (info != null) {
                    if (!info.columns.isEmpty()) {
                        parseColumns(info.columns);
                    }
                    if (!propertiesLoaded) {
                        sourceType = info.sourceType;
                        dataSourcePath = info.dataSourcePath;
                        location = info.location;
                        format = unwrapJsonArrayValue(getPropertyIgnoreCase(info.content, "FORMAT"));
                        compression = unwrapJsonArrayValue(getPropertyIgnoreCase(info.content, "COMPRESSION"));
                    }
                    log.debug("Loaded columns for external table via DescribeExternalTable: " + fullPath);
                } else {
                    // Fallback: try generic DescribeTable
                    YdbTable.DescribeTableResult result = YDBDescribeHelper.describeTable(
                        transport, retryCtx, absolutePath);
                    if (result != null) {
                        parseColumnsFromDescribeTable(result);
                        if (!propertiesLoaded) {
                            parseAttributes(result.getAttributesMap());
                        }
                        log.debug("Loaded columns for external table via DescribeTable fallback: " + fullPath);
                    } else {
                        log.debug("DescribeTable returned null for external table: " + fullPath);
                    }
                }
            }
        } catch (SQLException e) {
            log.debug("Failed to load columns for external table " + fullPath + ": " + e.getMessage());
        } catch (DBCException e) {
            log.debug("Failed to open session for loading external table columns: " + e.getMessage());
        }
        columnsLoaded = true;
    }

    private void parseAttributes(@NotNull Map<String, String> attributes) {
        dataSourcePath = getPropertyIgnoreCase(attributes, "DATA_SOURCE");
        sourceType = getPropertyIgnoreCase(attributes, "SOURCE_TYPE");
        location = getPropertyIgnoreCase(attributes, "LOCATION");
        format = unwrapJsonArrayValue(getPropertyIgnoreCase(attributes, "FORMAT"));
        compression = unwrapJsonArrayValue(getPropertyIgnoreCase(attributes, "COMPRESSION"));

        log.debug("External table attributes: " + attributes);
    }

    @Nullable
    private static String getPropertyIgnoreCase(@NotNull Map<String, String> props, @NotNull String key) {
        String value = props.get(key);
        if (value == null) {
            value = props.get(key.toLowerCase(java.util.Locale.ROOT));
        }
        return value;
    }

    /**
     * Unwrap a value that may be stored as a JSON array with a single element,
     * e.g. {@code ["parquet"]} → {@code parquet}.
     */
    @Nullable
    private static String unwrapJsonArrayValue(@Nullable String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            String inner = trimmed.substring(1, trimmed.length() - 1).trim();
            if (inner.startsWith("\"") && inner.endsWith("\"") && inner.length() >= 2) {
                return inner.substring(1, inner.length() - 1);
            }
            if (!inner.isEmpty()) {
                return inner;
            }
        }
        return value;
    }

    private void parseColumns(@NotNull List<YdbTable.ColumnMeta> columnMetas) {
        columns = new ArrayList<>();
        for (int i = 0; i < columnMetas.size(); i++) {
            YdbTable.ColumnMeta colMeta = columnMetas.get(i);
            columns.add(createColumn(colMeta, i));
        }
        columnsLoaded = true;
        log.debug("Parsed " + columns.size() + " columns from DescribeExternalTable for: " + fullPath);
    }

    private void parseColumnsFromDescribeTable(@NotNull YdbTable.DescribeTableResult result) {
        if (result.getColumnsCount() > 0) {
            columns = new ArrayList<>();
            for (int i = 0; i < result.getColumnsCount(); i++) {
                YdbTable.ColumnMeta colMeta = result.getColumns(i);
                columns.add(createColumn(colMeta, i));
            }
            columnsLoaded = true;
            log.debug("Parsed " + columns.size() + " columns from DescribeTable for: " + fullPath);
        }
    }

    @NotNull
    private GenericTableColumn createColumn(@NotNull YdbTable.ColumnMeta colMeta, int ordinal) {
        String typeName = YDBDescribeHelper.resolveTypeName(colMeta.getType());
        int jdbcType = YDBDescribeHelper.resolveJdbcType(colMeta.getType());
        int precision = YDBDescribeHelper.getSqlPrecision(colMeta.getType());
        int scale = YDBDescribeHelper.getSqlScale(colMeta.getType());
        boolean notNull = YDBDescribeHelper.isNotNull(colMeta);

        return new GenericTableColumn(
            this,
            colMeta.getName(),
            typeName,
            jdbcType,
            jdbcType,              // sourceType
            ordinal + 1,           // ordinalPosition
            precision,             // columnSize
            precision,             // charLength
            scale > 0 ? scale : null,
            precision > 0 ? precision : null,
            10,                    // radix
            notNull,
            null,                  // remarks
            null,                  // defaultValue
            false,                 // autoIncrement
            false                  // autoGenerated
        );
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
        ddl.append("CREATE EXTERNAL TABLE ").append(getFullyQualifiedName(DBPEvaluationContext.DDL)).append(" (\n");

        for (int i = 0; i < columns.size(); i++) {
            GenericTableColumn col = columns.get(i);
            ddl.append("    `").append(col.getName()).append("` ").append(col.getTypeName());
            if (col.isRequired()) {
                ddl.append(" NOT NULL");
            }
            if (i < columns.size() - 1) {
                ddl.append(",");
            }
            ddl.append("\n");
        }

        ddl.append(") WITH (\n");

        List<String> withParams = new ArrayList<>();
        if (dataSourcePath != null) {
            withParams.add("    DATA_SOURCE=\"" + dataSourcePath + "\"");
        }
        if (sourceType != null) {
            withParams.add("    SOURCE_TYPE=\"" + sourceType + "\"");
        }
        if (location != null) {
            withParams.add("    LOCATION=\"" + location + "\"");
        }
        if (format != null) {
            withParams.add("    FORMAT=\"" + format + "\"");
        }
        if (compression != null) {
            withParams.add("    COMPRESSION=\"" + compression + "\"");
        }

        for (int i = 0; i < withParams.size(); i++) {
            ddl.append(withParams.get(i));
            if (i < withParams.size() - 1) {
                ddl.append(",");
            }
            ddl.append("\n");
        }

        ddl.append(")");
        return ddl.toString();
    }

    @Override
    public String toString() {
        return shortName;
    }
}
