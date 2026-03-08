package org.jkiss.dbeaver.ext.ydb.it;

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
class YDBTopicIT extends YDBBaseIT {

    @BeforeAll
    void createTopic() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TOPIC it_test_topic (" +
                "    CONSUMER it_consumer WITH (important = true)" +
                ") WITH (" +
                "    min_active_partitions = 2," +
                "    retention_period = Interval('P1D')" +
                ")"
            );
        }
    }

    @AfterAll
    void dropTopic() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TOPIC it_test_topic");
        }
    }

    @Test
    void testTopicExistsViaSchemeClient() {
        assertNotNull(schemeClient, "SchemeClient must be available");
        Result<DescribePathResult> result = schemeClient.describePath(prefixPath + "/it_test_topic").join();
        assertTrue(result.isSuccess(), "describePath should succeed for it_test_topic: " + result.getStatus());
        SchemeOperationProtos.Entry.Type type = result.getValue().getSelf().getType();
        assertEquals(SchemeOperationProtos.Entry.Type.TOPIC, type,
            "Entry type should be TOPIC but was " + type);
    }
}
