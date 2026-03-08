package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import tech.ydb.core.Result;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.description.DescribePathResult;

import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBViewIT extends YDBBaseIT {

    @BeforeAll
    void createViewAndTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE it_view_src (" +
                "    id Int32 NOT NULL," +
                "    data Utf8," +
                "    PRIMARY KEY (id)" +
                ")"
            );
            stmt.execute(
                "CREATE VIEW it_test_view WITH (security_invoker = TRUE) AS " +
                "SELECT * FROM `" + prefixPath + "/it_view_src`"
            );
        }
    }

    @AfterAll
    void dropViewAndTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP VIEW it_test_view");
            stmt.execute("DROP TABLE it_view_src");
        }
    }

    @Test
    void testViewExistsViaSchemeClient() {
        assertNotNull(schemeClient, "SchemeClient must be available");
        Result<DescribePathResult> result = schemeClient.describePath(prefixPath + "/it_test_view").join();
        assertTrue(result.isSuccess(), "describePath should succeed for it_test_view: " + result.getStatus());
        SchemeOperationProtos.Entry.Type type = result.getValue().getSelf().getType();
        assertEquals(SchemeOperationProtos.Entry.Type.VIEW, type,
            "Entry type should be VIEW but was " + type);
    }

    /**
     * Verifies the view query text using describeViewQueryText() —
     * the same gRPC call the plugin makes (YDBView.loadQueryText).
     */
    @Test
    void testViewQueryTextViaDescribe() {
        String queryText = YDBGrpcHelper.describeViewQueryText(grpcTransport, prefixPath + "/it_test_view");
        assertNotNull(queryText, "describeViewQueryText should succeed for it_test_view");
        assertFalse(queryText.isEmpty(), "View query text should not be empty");
        assertTrue(queryText.contains("it_view_src"),
            "View query text should reference it_view_src, but was: " + queryText);
    }
}
