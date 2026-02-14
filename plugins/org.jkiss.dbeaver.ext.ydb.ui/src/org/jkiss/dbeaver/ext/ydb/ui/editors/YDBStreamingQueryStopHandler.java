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
import org.jkiss.dbeaver.ext.ydb.model.YDBStreamingQuery;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.runtime.ui.UIServiceSQL;
import org.jkiss.dbeaver.ui.navigator.NavigatorUtils;

/**
 * Handler for "Alter Query" context menu action on YDB streaming queries.
 * Opens a SQL console pre-filled with an ALTER STREAMING QUERY statement.
 */
public class YDBStreamingQueryStopHandler extends AbstractHandler {

    @Override
    public Object execute(ExecutionEvent event) throws ExecutionException {
        DBSObject object = NavigatorUtils.getSelectedObject(
            HandlerUtil.getCurrentSelection(event));
        if (!(object instanceof YDBStreamingQuery)) {
            return null;
        }
        YDBStreamingQuery query = (YDBStreamingQuery) object;

        String queryPath = query.getFullPath();
        String queryText = query.getQueryText();
        if (queryText == null) {
            queryText = "";
        }

        String sql = "ALTER STREAMING QUERY `" + queryPath + "` SET (RUN=FALSE)";

        UIServiceSQL serviceSQL = DBWorkbench.getService(UIServiceSQL.class);
        if (serviceSQL != null) {
            serviceSQL.openSQLConsole(
                query.getDataSource().getContainer(),
                null,
                query,
                "Alter " + query.getName(),
                sql
            );
        }
        return null;
    }
}
