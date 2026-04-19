package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBStreamingQueryRow;
import org.jkiss.dbeaver.ext.ydb.core.YDBSysQueries;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBStreamingQueryIT extends YDBBaseIT {

    @BeforeAll
    void createPrerequisites() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TOPIC it_sq_src");
            stmt.execute("CREATE TOPIC it_sq_dst");
            stmt.execute(
                "CREATE EXTERNAL DATA SOURCE it_sq_eds WITH (" +
                "    SOURCE_TYPE=\"Ydb\"," +
                "    LOCATION=\"localhost:2136\"," +
                "    DATABASE_NAME=\"/local\"," +
                "    AUTH_METHOD=\"NONE\"" +
                ")"
            );
            stmt.execute(
                "CREATE STREAMING QUERY it_sq WITH (RUN = FALSE) AS DO BEGIN " +
                "    INSERT INTO it_sq_eds.it_sq_dst SELECT * FROM it_sq_eds.it_sq_src " +
                "END DO"
            );
        }
    }

    @AfterAll
    void dropAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            try { stmt.execute("DROP STREAMING QUERY it_sq"); } catch (Exception ignored) {}
            try { stmt.execute("DROP EXTERNAL DATA SOURCE it_sq_eds"); } catch (Exception ignored) {}
            try { stmt.execute("DROP TOPIC it_sq_src"); } catch (Exception ignored) {}
            try { stmt.execute("DROP TOPIC it_sq_dst"); } catch (Exception ignored) {}
        }
    }

    /**
     * Verifies that the streaming query appears in .sys/streaming_queries using the same
     * query that YDBStreamingQueriesFolder uses (YDBSysQueries.STREAMING_QUERIES_QUERY).
     */
    @Test
    void testStreamingQueryVisible() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(YDBSysQueries.STREAMING_QUERIES_QUERY)) {

            List<YDBStreamingQueryRow> rows = YDBStreamingQueryRow.parseResultSet(rs);
            YDBStreamingQueryRow found = rows.stream()
                .filter(r -> r.path != null && r.path.contains("it_sq"))
                .findFirst()
                .orElse(null);

            List<String> paths = rows.stream()
                .map(r -> String.valueOf(r.path))
                .collect(Collectors.toList());
            assertNotNull(found,
                "No streaming query 'it_sq' found in .sys/streaming_queries. Found paths: " + paths);
            assertNotEquals("Failed", found.status,
                "Streaming query status should not be Failed");
            assertNotNull(found.queryText, "Streaming query text should not be null");
            assertTrue(found.queryText.contains("it_sq_src"),
                "Streaming query text should reference source topic it_sq_src, but was: " + found.queryText);
        }
    }

    /**
     * Verifies the raw data behind the navigator error-icon overlay:
     * the .sys/streaming_queries status column is populated, and for a healthy
     * (non-failed) query it must NOT match the rule used by
     * YDBStreamingQuery.isInErrorState().
     */
    @Test
    void testHealthyStreamingQueryStatusDoesNotTriggerErrorIcon() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(YDBSysQueries.STREAMING_QUERIES_QUERY)) {

            List<YDBStreamingQueryRow> rows = YDBStreamingQueryRow.parseResultSet(rs);
            YDBStreamingQueryRow found = rows.stream()
                .filter(r -> r.path != null && r.path.contains("it_sq"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("it_sq not found"));

            assertNotNull(found.status,
                "Streaming query status should be populated (required by error-icon overlay)");
            assertFalse(isErrorStatus(found.status),
                "Healthy streaming query should not be in error state, but status=" + found.status);
        }
    }

    /**
     * Mirrors the rule in YDBStreamingQuery.isInErrorState() — kept here so the IT
     * validates real YDB status values against the same predicate used by the UI.
     */
    private static boolean isErrorStatus(String status) {
        if (status == null) {
            return false;
        }
        String s = status.toLowerCase();
        return s.contains("error") || s.contains("failed") || s.contains("suspended");
    }
}
