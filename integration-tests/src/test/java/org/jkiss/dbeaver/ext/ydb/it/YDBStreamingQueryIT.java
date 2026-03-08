package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBSysQueries;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

            // Build a column name → index map (same approach as YDBStreamingQueriesFolder)
            ResultSetMetaData meta = rs.getMetaData();
            Map<String, Integer> colMap = new HashMap<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                colMap.put(meta.getColumnName(i).toLowerCase(), i);
            }
            Integer pathIdx = colMap.get("path");
            Integer statusIdx = colMap.get("status");
            Integer textIdx = colMap.containsKey("text") ? colMap.get("text") : colMap.get("query_text");

            List<String> paths = new ArrayList<>();
            while (rs.next()) {
                String path = pathIdx != null ? rs.getString(pathIdx) : null;
                paths.add(String.valueOf(path));
                if (path != null && path.contains("it_sq")) {
                    String status = statusIdx != null ? rs.getString(statusIdx) : null;
                    String text = textIdx != null ? rs.getString(textIdx) : null;
                    assertNotEquals("Failed", status,
                        "Streaming query status should not be Failed");
                    assertNotNull(text, "Streaming query text should not be null");
                    assertTrue(text.contains("it_sq_src"),
                        "Streaming query text should reference source topic it_sq_src, but was: " + text);
                    return;
                }
            }
            fail("No streaming query 'it_sq' found in .sys/streaming_queries. Found paths: " + paths);
        }
    }
}
