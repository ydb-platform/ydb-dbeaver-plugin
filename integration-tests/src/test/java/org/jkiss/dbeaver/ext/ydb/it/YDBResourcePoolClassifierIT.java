package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBSysQueries;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBResourcePoolClassifierIT extends YDBBaseIT {

    @BeforeAll
    void createPrerequisites() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE RESOURCE POOL it_clf_pool WITH (CONCURRENT_QUERY_LIMIT=1)");
            stmt.execute(
                "CREATE RESOURCE POOL CLASSIFIER it_test_classifier WITH (" +
                "    RANK=1000," +
                "    RESOURCE_POOL=\"it_clf_pool\"" +
                ")"
            );
        }
    }

    @AfterAll
    void dropAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP RESOURCE POOL CLASSIFIER it_test_classifier");
            stmt.execute("DROP RESOURCE POOL it_clf_pool");
        }
    }

    @Test
    void testClassifierProperties() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                YDBSysQueries.RESOURCE_POOL_CLASSIFIERS_QUERY + " WHERE Name = 'it_test_classifier'")) {
            assertTrue(rs.next(), "Classifier it_test_classifier should appear in .sys/resource_pool_classifiers");
            assertEquals("it_test_classifier", rs.getString("Name"));
            assertEquals(1000, rs.getInt("Rank"));
            assertEquals("it_clf_pool", rs.getString("ResourcePool"));
        }
    }
}
