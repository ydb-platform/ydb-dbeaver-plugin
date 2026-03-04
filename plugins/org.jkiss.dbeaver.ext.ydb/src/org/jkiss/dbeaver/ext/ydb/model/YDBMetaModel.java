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
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.ydb.model.data.YDBJSONValueHandler;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.data.DBDFormatSettings;
import org.jkiss.dbeaver.model.data.DBDValueHandler;
import org.jkiss.dbeaver.model.data.DBDValueHandlerProvider;
import org.jkiss.dbeaver.model.exec.DBCQueryTransformProvider;
import org.jkiss.dbeaver.model.exec.DBCQueryTransformType;
import org.jkiss.dbeaver.model.exec.DBCQueryTransformer;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.sql.QueryTransformerLimit;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlanner;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBQueryPlanner;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

import java.sql.SQLException;
import java.util.Map;

/**
 * YDBMetaModel
 */
public class YDBMetaModel extends GenericMetaModel implements DBCQueryTransformProvider, DBDValueHandlerProvider {

    public YDBMetaModel() {
        super();
    }

    @NotNull
    @Override
    public GenericDataSource createDataSourceImpl(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBPDataSourceContainer container
    ) throws DBException {
        return new YDBDataSource(monitor, container, this);
    }

    @Override
    public GenericCatalog createCatalogImpl(
        @NotNull GenericDataSource dataSource,
        @NotNull String catalogName
    ) {
        return new YDBCatalog(dataSource, catalogName);
    }

    @Override
    public GenericSchema createSchemaImpl(
        @NotNull GenericDataSource dataSource,
        @Nullable GenericCatalog catalog,
        @NotNull String schemaName
    ) throws DBException {
        return new YDBSchema(dataSource, catalog, schemaName);
    }

    @Override
    public boolean isSystemSchema(GenericSchema schema) {
        // Show all schemas including system ones (.sys, etc.)
        return false;
    }

    @Override
    public boolean supportsSequences(@NotNull GenericDataSource dataSource) {
        return false;
    }

    @Override
    public boolean supportsNotNullColumnModifiers(DBSObject object) {
        return false;
    }

    @Override
    public boolean supportsTableDDLSplit(@NotNull org.jkiss.dbeaver.ext.generic.model.GenericTableBase sourceObject) {
        return false;
    }

    @NotNull
    @Override
    public GenericTableBase createTableOrViewImpl(
        @NotNull GenericStructContainer container,
        @Nullable String tableName,
        @Nullable String tableType,
        @Nullable JDBCResultSet dbResult
    ) {
        if (tableType != null && isView(tableType)) {
            return new GenericView(container, tableName, tableType, dbResult);
        }
        return new YDBTable(container, tableName, tableType, dbResult);
    }

    @Override
    public JDBCStatement prepareTableColumnLoadStatement(
        @NotNull JDBCSession session,
        @NotNull GenericStructContainer container,
        @Nullable GenericTableBase table
    ) throws SQLException {
        // YDBTable.getName() returns the short name (without path prefix) for display,
        // but JDBC metadata requires the full path to find columns.
        if (table instanceof YDBTable && ((YDBTable) table).hasHierarchicalName()) {
            String catalogName = container.getCatalog() != null
                ? container.getCatalog().getName() : null;
            String schemaName = container.getSchema() != null
                && !DBUtils.isVirtualObject(container.getSchema())
                ? JDBCUtils.escapeWildCards(session, container.getSchema().getName()) : null;
            String tableName = JDBCUtils.escapeWildCards(
                session, ((YDBTable) table).getFullTableName());
            return session.getMetaData().getColumns(
                catalogName, schemaName, tableName,
                container.getDataSource().getAllObjectsPattern()
            ).getSourceStatement();
        }
        return super.prepareTableColumnLoadStatement(session, container, table);
    }

    @Override
    public String getTableDDL(
        @NotNull DBRProgressMonitor monitor,
        @NotNull GenericTableBase sourceObject,
        @NotNull Map<String, Object> options
    ) throws DBException {
        if (sourceObject instanceof YDBTable) {
            return ((YDBTable) sourceObject).getObjectDefinitionText(monitor, options);
        }
        return super.getTableDDL(monitor, sourceObject, options);
    }

    @Nullable
    @Override
    public DBCQueryPlanner getQueryPlanner(@NotNull GenericDataSource dataSource) {
        return new YDBQueryPlanner(dataSource);
    }

    @Nullable
    @Override
    public DBDValueHandler getValueHandler(DBPDataSource dataSource, DBDFormatSettings preferences, DBSTypedObject typedObject) {
        String typeName = typedObject.getTypeName();
        if (typeName != null) {
            String upperName = typeName.toUpperCase();
            if (upperName.equals("JSON") || upperName.equals("JSONDOCUMENT")) {
                return YDBJSONValueHandler.INSTANCE;
            }
        }
        return null;
    }

    @Nullable
    @Override
    public DBCQueryTransformer createQueryTransformer(@NotNull DBCQueryTransformType type) {
        if (type == DBCQueryTransformType.RESULT_SET_LIMIT) {
            return new QueryTransformerLimit(true, true);
        }
        return null;
    }
}
