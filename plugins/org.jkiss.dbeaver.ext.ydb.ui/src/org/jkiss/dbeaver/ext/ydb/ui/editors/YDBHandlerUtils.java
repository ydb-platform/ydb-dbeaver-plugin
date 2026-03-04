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
package org.jkiss.dbeaver.ext.ydb.ui.editors;

import org.eclipse.core.expressions.IEvaluationContext;
import org.eclipse.ui.ISources;
import org.jkiss.dbeaver.ext.ydb.model.YDBDataSource;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPDataSourceContainerProvider;
import org.jkiss.dbeaver.ui.editors.sql.SQLEditor;
import org.jkiss.dbeaver.utils.RuntimeUtils;

/**
 * Utility methods for YDB command handlers.
 */
final class YDBHandlerUtils {

    private static final String YDB_DRIVER_ID = "ydb";

    private YDBHandlerUtils() {
    }

    /**
     * Checks if the active editor in the given evaluation context is a SQL editor
     * connected to a YDB datasource.
     */
    static boolean isYdbEditor(Object evaluationContext) {
        if (!(evaluationContext instanceof IEvaluationContext ctx)) {
            return false;
        }
        Object editor = ctx.getVariable(ISources.ACTIVE_EDITOR_NAME);
        if (editor == null) {
            return false;
        }
        SQLEditor sqlEditor = RuntimeUtils.getObjectAdapter(editor, SQLEditor.class);
        if (sqlEditor == null) {
            return false;
        }
        // Check connected datasource first
        DBPDataSource dataSource = sqlEditor.getDataSource();
        if (dataSource instanceof YDBDataSource) {
            return true;
        }
        // Fallback: check datasource container's driver ID
        // (datasource may not be connected yet when editor just opened)
        DBPDataSourceContainer container = sqlEditor.getDataSourceContainer();
        return container != null && YDB_DRIVER_ID.equals(container.getDriver().getId());
    }
}
