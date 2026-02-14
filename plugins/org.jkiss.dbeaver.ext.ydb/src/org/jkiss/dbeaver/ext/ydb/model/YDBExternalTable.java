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
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.table.YdbTable;
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
public class YDBExternalTable extends GenericTable {

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
        if (columns != null && !columns.isEmpty()) {
            return columns;
        }
        return super.getAttributes(monitor);
    }

    @Override
    public GenericTableColumn getAttribute(@NotNull DBRProgressMonitor monitor, @NotNull String attributeName) throws DBException {
        if (!columnsLoaded) {
            loadColumns(monitor);
        }
        if (columns != null && !columns.isEmpty()) {
            for (GenericTableColumn column : columns) {
                if (column.getName().equals(attributeName)) {
                    return column;
                }
            }
            return null;
        }
        return super.getAttribute(monitor, attributeName);
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
        @NotNull String prefixPath
    ) {
        if (propertiesLoaded) {
            return;
        }
        String absolutePath = prefixPath + "/" + fullPath;
        YdbTable.DescribeTableResult result = YDBDescribeHelper.describeTable(
            transport, retryCtx, absolutePath);
        if (result != null) {
            parseAttributes(result.getAttributesMap());
            if (!columnsLoaded) {
                parseColumns(result);
            }
        }
        propertiesLoaded = true;
    }

    private synchronized void loadColumns(@NotNull DBRProgressMonitor monitor) {
        if (columnsLoaded) {
            return;
        }
        columns = new ArrayList<>();
        String query = "SELECT * FROM `" + fullPath + "` LIMIT 0";

        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load external table columns")) {
            try (JDBCPreparedStatement stmt = session.prepareStatement(query)) {
                try (JDBCResultSet rs = stmt.executeQuery()) {
                    java.sql.ResultSetMetaData metaData = rs.getOriginal().getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        String typeName = metaData.getColumnTypeName(i);
                        int jdbcType = metaData.getColumnType(i);
                        int precision = metaData.getPrecision(i);
                        int scale = metaData.getScale(i);
                        boolean nullable = metaData.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls;

                        GenericTableColumn column = new GenericTableColumn(
                            this,
                            columnName,
                            typeName,
                            jdbcType,
                            jdbcType,      // sourceType
                            i,             // ordinalPosition
                            precision,     // columnSize
                            precision,     // charLength
                            scale > 0 ? scale : null,
                            precision > 0 ? precision : null,
                            10,            // radix
                            !nullable,     // notNull
                            null,          // remarks
                            null,          // defaultValue
                            false,         // autoIncrement
                            false          // autoGenerated
                        );
                        columns.add(column);
                    }

                    log.debug("Loaded " + columnCount + " columns for external table: " + fullPath);
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
        dataSourcePath = attributes.get("DATA_SOURCE");
        sourceType = attributes.get("SOURCE_TYPE");
        location = attributes.get("LOCATION");
        format = attributes.get("FORMAT");
        compression = attributes.get("COMPRESSION");

        if (dataSourcePath == null) {
            dataSourcePath = attributes.get("data_source");
        }
        if (sourceType == null) {
            sourceType = attributes.get("source_type");
        }
        if (location == null) {
            location = attributes.get("location");
        }
        if (format == null) {
            format = attributes.get("format");
        }
        if (compression == null) {
            compression = attributes.get("compression");
        }

        log.debug("External table attributes: " + attributes);
    }

    private void parseColumns(@NotNull YdbTable.DescribeTableResult result) {
        if (result.getColumnsCount() > 0) {
            columns = new ArrayList<>();
            for (int i = 0; i < result.getColumnsCount(); i++) {
                YdbTable.ColumnMeta column = result.getColumns(i);
                String typeName = column.getType().toString();
                GenericTableColumn col = new GenericTableColumn(
                    this,
                    column.getName(),
                    typeName,
                    java.sql.Types.OTHER,  // jdbcType
                    java.sql.Types.OTHER,  // sourceType
                    i + 1,                 // ordinalPosition
                    0,                     // columnSize
                    0,                     // charLength
                    null,                  // scale
                    null,                  // precision
                    10,                    // radix
                    false,                 // notNull (assume nullable)
                    null,                  // remarks
                    null,                  // defaultValue
                    false,                 // autoIncrement
                    false                  // autoGenerated
                );
                columns.add(col);
            }
            columnsLoaded = true;
            log.debug("Parsed " + columns.size() + " columns from DescribeTable for: " + fullPath);
        }
    }

    @Override
    public String toString() {
        return shortName;
    }
}
