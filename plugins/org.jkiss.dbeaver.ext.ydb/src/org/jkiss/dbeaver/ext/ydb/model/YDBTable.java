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
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.meta.Property;

/**
 * YDB Table with support for hierarchical names.
 * Tables with "/" in their names display only the last part (short name)
 * but use the full name for SQL queries.
 */
public class YDBTable extends GenericTable {

    private final String shortName;
    private final String fullName;

    public YDBTable(
        GenericStructContainer container,
        @Nullable String tableName,
        @Nullable String tableType,
        @Nullable JDBCResultSet dbResult
    ) {
        super(container, tableName, tableType, dbResult);
        this.fullName = tableName;
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
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        // For SQL context, use the full table name with path
        if (context == DBPEvaluationContext.DML || context == DBPEvaluationContext.DDL) {
            return getDataSource().getSQLDialect().getQuotedIdentifier(fullName, false, true);
        }
        return super.getFullyQualifiedName(context);
    }

    @Override
    public String toString() {
        return shortName != null ? shortName : super.toString();
    }
}
