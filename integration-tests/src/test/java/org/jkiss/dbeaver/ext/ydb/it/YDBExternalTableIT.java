package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBExternalTableIT extends YDBBaseS3IT {

    @BeforeAll
    void createPrerequisites() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE EXTERNAL DATA SOURCE it_ext_ds WITH (" +
                "    SOURCE_TYPE=\"ObjectStorage\"," +
                "    LOCATION=\"" + s3Location + "\"," +
                "    AUTH_METHOD=\"NONE\"" +
                ")"
            );
            stmt.execute(
                "CREATE EXTERNAL TABLE it_ext_table (" +
                "    key Utf8 NOT NULL," +
                "    value Utf8" +
                ") WITH (" +
                "    DATA_SOURCE=\"it_ext_ds\"," +
                "    LOCATION=\"test_folder/\"," +
                "    FORMAT=\"csv_with_names\"" +
                ")"
            );
        }
    }

    @AfterAll
    void dropAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            try { stmt.execute("DROP EXTERNAL TABLE it_ext_table"); } catch (Exception ignored) {}
            try { stmt.execute("DROP EXTERNAL DATA SOURCE it_ext_ds"); } catch (Exception ignored) {}
        }
    }

    /**
     * Verifies that describeExternalTable() — the same call the plugin makes — succeeds.
     */
    @Test
    void testExternalTableDescribable() {
        YDBGrpcHelper.ExternalTableInfo info =
            YDBGrpcHelper.describeExternalTable(grpcTransport, prefixPath + "/it_ext_table");
        assertNotNull(info, "describeExternalTable should succeed for it_ext_table");
    }

    /**
     * Verifies column metadata using describeExternalTable() — the same gRPC call the plugin uses
     * (YDBExternalTable.loadProperties calls YDBDescribeHelper.describeExternalTable which delegates here).
     */
    @Test
    void testExternalTableColumns() {
        YDBGrpcHelper.ExternalTableInfo info =
            YDBGrpcHelper.describeExternalTable(grpcTransport, prefixPath + "/it_ext_table");
        assertNotNull(info, "describeExternalTable should succeed for it_ext_table");

        List<String> cols = info.columns.stream()
            .map(c -> c.getName())
            .collect(Collectors.toList());
        assertTrue(cols.contains("key"), "Column 'key' not found, got: " + cols);
        assertTrue(cols.contains("value"), "Column 'value' not found, got: " + cols);
    }
}
