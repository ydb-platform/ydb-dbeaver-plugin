package org.jkiss.dbeaver.ext.ydb.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DTO for a single row from the {@code .sys/streaming_queries} system view.
 * Used by both the DBeaver plugin and integration tests so that column-name
 * resolution logic is not duplicated.
 */
public final class YDBStreamingQueryRow {

    public final String path;
    public final String status;
    public final String issues;
    public final String plan;
    public final String ast;
    public final String queryText;
    public final String run;
    public final String resourcePool;
    public final String retryCount;
    public final String lastFailAt;
    public final String suspendedUntil;

    public YDBStreamingQueryRow(
        String path, String status, String issues, String plan, String ast,
        String queryText, String run, String resourcePool, String retryCount,
        String lastFailAt, String suspendedUntil
    ) {
        this.path = path;
        this.status = status;
        this.issues = issues;
        this.plan = plan;
        this.ast = ast;
        this.queryText = queryText;
        this.run = run;
        this.resourcePool = resourcePool;
        this.retryCount = retryCount;
        this.lastFailAt = lastFailAt;
        this.suspendedUntil = suspendedUntil;
    }

    /**
     * Parse all rows from a {@code .sys/streaming_queries} ResultSet.
     *
     * <p>Column names are resolved case-insensitively; the query text column
     * may be named {@code Text}, {@code query_text}, or {@code query}
     * depending on the YDB version.</p>
     */
    public static List<YDBStreamingQueryRow> parseResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        Map<String, Integer> columnMap = new HashMap<>();
        for (int i = 1; i <= columnCount; i++) {
            columnMap.put(meta.getColumnName(i).toLowerCase(), i);
        }

        Integer pathIdx = findColumn(columnMap, "path");
        Integer statusIdx = findColumn(columnMap, "status");
        Integer issuesIdx = findColumn(columnMap, "issues");
        Integer planIdx = findColumn(columnMap, "plan");
        Integer astIdx = findColumn(columnMap, "ast");
        Integer textIdx = findColumn(columnMap, "text", "query_text", "query");
        Integer runIdx = findColumn(columnMap, "run");
        Integer resourcePoolIdx = findColumn(columnMap, "resourcepool");
        Integer retryCountIdx = findColumn(columnMap, "retrycount");
        Integer lastFailAtIdx = findColumn(columnMap, "lastfailat");
        Integer suspendedUntilIdx = findColumn(columnMap, "suspendeduntil");

        if (pathIdx == null && columnCount > 0) {
            pathIdx = 1;
        }

        List<YDBStreamingQueryRow> rows = new ArrayList<>();
        int rowNum = 0;
        while (rs.next()) {
            rowNum++;
            String path = pathIdx != null ? rs.getString(pathIdx) : null;
            if (path == null || path.isEmpty()) {
                path = "Query #" + rowNum;
            }
            rows.add(new YDBStreamingQueryRow(
                path,
                statusIdx != null ? rs.getString(statusIdx) : null,
                issuesIdx != null ? rs.getString(issuesIdx) : null,
                planIdx != null ? rs.getString(planIdx) : null,
                astIdx != null ? rs.getString(astIdx) : null,
                textIdx != null ? rs.getString(textIdx) : null,
                runIdx != null ? rs.getString(runIdx) : null,
                resourcePoolIdx != null ? rs.getString(resourcePoolIdx) : null,
                retryCountIdx != null ? rs.getString(retryCountIdx) : null,
                lastFailAtIdx != null ? rs.getString(lastFailAtIdx) : null,
                suspendedUntilIdx != null ? rs.getString(suspendedUntilIdx) : null
            ));
        }
        return rows;
    }

    private static Integer findColumn(Map<String, Integer> columnMap, String... names) {
        for (String name : names) {
            Integer idx = columnMap.get(name);
            if (idx != null) {
                return idx;
            }
        }
        return null;
    }
}
