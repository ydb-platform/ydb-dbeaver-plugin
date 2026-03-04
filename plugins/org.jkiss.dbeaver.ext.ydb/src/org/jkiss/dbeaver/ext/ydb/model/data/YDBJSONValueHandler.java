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
package org.jkiss.dbeaver.ext.ydb.model.data;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.model.data.DBDContent;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.data.JDBCContentChars;
import org.jkiss.dbeaver.model.impl.jdbc.data.handlers.JDBCContentValueHandler;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

import java.sql.SQLException;

/**
 * YDBJSONValueHandler
 */
public class YDBJSONValueHandler extends JDBCContentValueHandler {

    public static final YDBJSONValueHandler INSTANCE = new YDBJSONValueHandler();

    @Override
    protected DBDContent fetchColumnValue(DBCSession session, JDBCResultSet resultSet, DBSTypedObject type, int index) throws SQLException {
        String json = resultSet.getString(index);
        return new YDBContentJSON(session.getExecutionContext(), json);
    }

    @Override
    public DBDContent getValueFromObject(
        @NotNull DBCSession session,
        @NotNull DBSTypedObject type,
        @Nullable Object object,
        boolean copy,
        boolean validateValue
    ) throws DBCException {
        if (object == null) {
            return new YDBContentJSON(session.getExecutionContext(), null);
        } else if (object instanceof YDBContentJSON contentJSON) {
            return copy ? contentJSON.cloneValue(session.getProgressMonitor()) : contentJSON;
        } else if (object instanceof String stringValue) {
            return new YDBContentJSON(session.getExecutionContext(), stringValue);
        }
        return super.getValueFromObject(session, type, object, copy, validateValue);
    }

    private static class YDBContentJSON extends JDBCContentChars {
        public YDBContentJSON(DBCExecutionContext executionContext, String value) {
            super(executionContext, value);
        }

        @Override
        public String getContentType() {
            return "text/json";
        }

        @Override
        public YDBContentJSON cloneValue(DBRProgressMonitor monitor) {
            return new YDBContentJSON(executionContext, (String) getRawValue());
        }
    }
}
