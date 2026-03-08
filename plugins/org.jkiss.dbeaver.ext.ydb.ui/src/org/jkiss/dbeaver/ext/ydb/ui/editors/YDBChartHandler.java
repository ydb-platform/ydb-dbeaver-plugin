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

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.ui.handlers.HandlerUtil;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.controls.resultset.IResultSetController;
import org.jkiss.dbeaver.ui.controls.resultset.ResultSetModel;
import org.jkiss.dbeaver.ui.controls.resultset.ResultSetRow;
import org.jkiss.dbeaver.ui.editors.sql.SQLEditor;
import org.jkiss.dbeaver.utils.RuntimeUtils;

import java.util.List;

public class YDBChartHandler extends AbstractHandler {

    @Override
    public void setEnabled(Object evaluationContext) {
        setBaseEnabled(YDBHandlerUtils.isYdbEditor(evaluationContext));
    }

    @Override
    public Object execute(ExecutionEvent event) throws ExecutionException {
        SQLEditor editor = RuntimeUtils.getObjectAdapter(HandlerUtil.getActiveEditor(event), SQLEditor.class);
        if (editor == null) {
            return null;
        }

        IResultSetController controller = editor.getResultSetController();
        if (controller == null || !controller.hasData()) {
            DBWorkbench.getPlatformUI().showError("Chart", "No query results available. Execute a query first.");
            return null;
        }

        ResultSetModel model = controller.getModel();
        List<DBDAttributeBinding> attributes = model.getVisibleAttributes();
        List<ResultSetRow> rows = model.getAllRows();

        if (attributes.isEmpty() || rows.isEmpty()) {
            DBWorkbench.getPlatformUI().showError("Chart", "Query result is empty.");
            return null;
        }

        YDBChartDialog dialog = new YDBChartDialog(
            HandlerUtil.getActiveShell(event), attributes, rows);
        dialog.open();

        return null;
    }
}
