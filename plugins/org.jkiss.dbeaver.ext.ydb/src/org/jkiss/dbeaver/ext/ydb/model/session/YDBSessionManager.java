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
package org.jkiss.dbeaver.ext.ydb.model.session;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBDatabaseException;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.ydb.model.YDBDataSource;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionManager;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionManagerSQL;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * YDB Session Manager.
 *
 * Manages query sessions using the .sys/query_sessions system view.
 * Note: YDB currently does not support killing sessions, so alterSession is a no-op.
 */
public class YDBSessionManager implements DBAServerSessionManager<YDBSession>, DBAServerSessionManagerSQL {

    public static final String OPTION_HIDE_IDLE = "hideIdle";

    private final YDBDataSource dataSource;

    public YDBSessionManager(YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return dataSource;
    }

    @NotNull
    @Override
    public Collection<YDBSession> getSessions(@NotNull DBCSession session, @NotNull Map<String, Object> options) throws DBException {
        try {
            try (JDBCPreparedStatement dbStat = ((JDBCSession) session).prepareStatement(generateSessionReadQuery(options))) {
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    List<YDBSession> sessions = new ArrayList<>();
                    while (dbResult.next()) {
                        YDBSession sessionInfo = new YDBSession(dbResult);
                        // Optionally filter idle sessions (YDB uses "Idle" state)
                        if (options.containsKey(OPTION_HIDE_IDLE) &&
                            Boolean.TRUE.equals(options.get(OPTION_HIDE_IDLE)) &&
                            "Idle".equalsIgnoreCase(sessionInfo.getState())) {
                            continue;
                        }
                        sessions.add(sessionInfo);
                    }
                    return sessions;
                }
            }
        } catch (SQLException e) {
            throw new DBDatabaseException(e, session.getDataSource());
        }
    }

    @Override
    public void alterSession(@NotNull DBCSession session, @NotNull String sessionId, @NotNull Map<String, Object> options) throws DBException {
        // YDB does not currently support killing sessions
        // This method is intentionally left as no-op
    }

    @NotNull
    @Override
    public Map<String, Object> getTerminateOptions() {
        return Map.of();
    }

    @Override
    public boolean canGenerateSessionReadQuery() {
        return true;
    }

    @NotNull
    @Override
    public String generateSessionReadQuery(@NotNull Map<String, Object> options) {
        // Query the .sys/query_sessions system view
        // Use SELECT * to get all columns with their actual names (YDB uses PascalCase)
        return "SELECT * FROM `.sys/query_sessions` ORDER BY SessionStartAt ASC";
    }
}
