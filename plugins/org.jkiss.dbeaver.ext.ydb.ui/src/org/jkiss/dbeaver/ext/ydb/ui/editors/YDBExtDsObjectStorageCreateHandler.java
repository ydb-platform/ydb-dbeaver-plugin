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
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.runtime.ui.UIServiceSQL;
import org.jkiss.dbeaver.ui.navigator.NavigatorUtils;

public class YDBExtDsObjectStorageCreateHandler extends AbstractHandler {

    private static final String SQL_TEMPLATE =
        "CREATE EXTERNAL DATA SOURCE `<name>` WITH (\n" +
        "    SOURCE_TYPE=\"ObjectStorage\",\n" +
        "    LOCATION=\"https://<endpoint>/<bucket>/\",\n" +
        "    AUTH_METHOD=\"AWS\",\n" +
        "    AWS_ACCESS_KEY_ID_SECRET_NAME=\"<access_key_id_secret>\",\n" +
        "    AWS_SECRET_ACCESS_KEY_SECRET_NAME=\"<secret_access_key_secret>\",\n" +
        "    AWS_REGION=\"<region>\"\n" +
        ");";

    @Override
    public Object execute(ExecutionEvent event) throws ExecutionException {
        DBSObject object = NavigatorUtils.getSelectedObject(
            HandlerUtil.getCurrentSelection(event));
        if (object == null) {
            return null;
        }

        UIServiceSQL serviceSQL = DBWorkbench.getService(UIServiceSQL.class);
        if (serviceSQL != null) {
            serviceSQL.openSQLConsole(
                object.getDataSource().getContainer(),
                null,
                object,
                "Create External Data Source (S3/ObjectStorage)",
                SQL_TEMPLATE
            );
        }
        return null;
    }
}
