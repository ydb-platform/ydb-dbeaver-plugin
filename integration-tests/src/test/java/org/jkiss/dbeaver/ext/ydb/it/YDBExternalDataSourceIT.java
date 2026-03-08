package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBExternalDataSourceIT extends YDBBaseS3IT {

    @BeforeAll
    void createExternalDataSource() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE EXTERNAL DATA SOURCE it_test_ds WITH (" +
                "    SOURCE_TYPE=\"ObjectStorage\"," +
                "    LOCATION=\"" + s3Location + "\"," +
                "    AUTH_METHOD=\"NONE\"" +
                ")"
            );
        }
    }

    @AfterAll
    void dropExternalDataSource() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP EXTERNAL DATA SOURCE it_test_ds");
        }
    }

    /**
     * Verifies the external data source using describeExternalDataSource() —
     * the same gRPC call the plugin makes (YDBExternalDataSource.loadProperties).
     */
    @Test
    void testExternalDataSourceViaDescribe() {
        YDBGrpcHelper.ExternalDataSourceInfo info =
            YDBGrpcHelper.describeExternalDataSource(grpcTransport, prefixPath + "/it_test_ds");
        assertNotNull(info, "describeExternalDataSource should succeed for it_test_ds");
        assertEquals("ObjectStorage", info.sourceType,
            "sourceType should be ObjectStorage but was: " + info.sourceType);
        assertNotNull(info.location, "location should not be null");
        assertTrue(info.location.contains("test-bucket"),
            "location should reference test-bucket, but was: " + info.location);
    }
}
