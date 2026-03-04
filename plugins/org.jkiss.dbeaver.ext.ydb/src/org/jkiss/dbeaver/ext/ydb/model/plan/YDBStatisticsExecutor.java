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
package org.jkiss.dbeaver.ext.ydb.model.plan;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.result.QueryStats;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.settings.QueryStatsMode;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

/**
 * Executes a query with StatsMode.PROFILE and extracts the plan JSON with runtime statistics.
 */
public class YDBStatisticsExecutor {

    private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(30);

    @NotNull
    public static YDBStatisticsResult execute(@NotNull DBCSession dbcSession, @NotNull String query) throws DBException {
        JDBCSession jdbcSession = (JDBCSession) dbcSession;
        try {
            Connection conn = jdbcSession.getOriginal();
            if (!conn.isWrapperFor(YdbConnection.class)) {
                throw new DBException("Cannot unwrap YDB connection. Statistics profiling requires YDB JDBC driver.");
            }

            YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
            YdbContext ctx = ydbConn.getCtx();
            QueryClient queryClient = ctx.getQueryClient();

            ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
                .withStatsMode(QueryStatsMode.PROFILE)
                .build();

            Result<QuerySession> sessionResult = queryClient.createSession(SESSION_TIMEOUT).join();
            if (!sessionResult.isSuccess()) {
                throw new DBException("Failed to create YDB query session: " + sessionResult.getStatus());
            }

            try (QuerySession session = sessionResult.getValue()) {
                QueryStream stream = session.createQuery(query, TxMode.SERIALIZABLE_RW, Params.empty(), settings);
                Result<QueryReader> readerResult = QueryReader.readFrom(stream).join();

                if (!readerResult.isSuccess()) {
                    throw new DBException("Failed to execute query with statistics: " + readerResult.getStatus());
                }

                QueryReader reader = readerResult.getValue();
                QueryInfo info = reader.getQueryInfo();

                if (!info.hasStats()) {
                    throw new DBException("Query executed but no statistics were returned. "
                        + "The server may not support PROFILE statistics mode.");
                }

                QueryStats stats = info.getStats();
                String planJson = stats.getQueryPlan();

                if (planJson == null || planJson.isEmpty()) {
                    throw new DBException("Query statistics returned but plan JSON is empty.");
                }

                return new YDBStatisticsResult(
                    query,
                    planJson,
                    stats.getTotalDurationUs(),
                    stats.getTotalCpuTimeUs()
                );
            }
        } catch (SQLException e) {
            throw new DBException("Failed to execute statistics query", e);
        }
    }
}
