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
class YDBResourcePoolIT extends YDBBaseIT {

    @BeforeAll
    void createResourcePool() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE RESOURCE POOL it_test_pool WITH (" +
                "    CONCURRENT_QUERY_LIMIT=5," +
                "    QUEUE_SIZE=10," +
                "    DATABASE_LOAD_CPU_THRESHOLD=80" +
                ")"
            );
        }
    }

    @AfterAll
    void dropResourcePool() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP RESOURCE POOL it_test_pool");
        }
    }

    /**
     * Verifies resource pool properties using the same SQL query that
     * YDBResourcePoolsFolder uses to load pools (YDBSysQueries.RESOURCE_POOLS_QUERY).
     */
    @Test
    void testResourcePoolProperties() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                YDBSysQueries.RESOURCE_POOLS_QUERY + " WHERE Name = 'it_test_pool'")) {
            assertTrue(rs.next(), "Resource pool it_test_pool should appear in .sys/resource_pools");
            assertEquals("it_test_pool", rs.getString("Name"));
            assertEquals(5, rs.getInt("ConcurrentQueryLimit"));
            assertEquals(10, rs.getInt("QueueSize"));
            assertEquals(80.0, rs.getDouble("DatabaseLoadCpuThreshold"));
        }
    }
}
