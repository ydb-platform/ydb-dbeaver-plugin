package org.jkiss.dbeaver.ext.ydb.it;

import org.jkiss.dbeaver.ext.ydb.core.YDBGrpcHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBTransferIT extends YDBBaseIT {

    @BeforeAll
    void createPrerequisites() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE it_transfer_dst (" +
                "    partition Uint32 NOT NULL," +
                "    offset Uint64 NOT NULL," +
                "    message Utf8," +
                "    PRIMARY KEY (partition, offset)" +
                ")"
            );
            stmt.execute("CREATE TOPIC it_transfer_src");
            stmt.execute(
                "CREATE TRANSFER it_test_transfer " +
                "FROM it_transfer_src TO it_transfer_dst " +
                "USING ($msg) -> { return [<|" +
                "partition: $msg._partition, " +
                "offset: $msg._offset, " +
                "message: CAST($msg._data AS Utf8)" +
                "|>]; }"
            );
        }
    }

    @AfterAll
    void dropAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            try { stmt.execute("DROP TRANSFER it_test_transfer"); } catch (Exception ignored) {}
            try { stmt.execute("DROP TOPIC it_transfer_src"); } catch (Exception ignored) {}
            try { stmt.execute("DROP TABLE it_transfer_dst"); } catch (Exception ignored) {}
        }
    }

    /**
     * Verifies the transfer using describeTransfer() —
     * the same gRPC call the plugin makes (YDBTransfer.loadProperties).
     */
    @Test
    void testTransferViaDescribe() {
        YDBGrpcHelper.TransferInfo info =
            YDBGrpcHelper.describeTransfer(grpcTransport, prefixPath + "/it_test_transfer");
        assertNotNull(info, "describeTransfer returned null — RPC may not be supported");
        assertNotNull(info.sourcePath, "Transfer source path should not be null");
        assertTrue(info.sourcePath.contains("it_transfer_src"),
            "Source path should reference topic it_transfer_src, but was: " + info.sourcePath);
        assertNotNull(info.destinationPath, "Transfer destination path should not be null");
        assertTrue(info.destinationPath.contains("it_transfer_dst"),
            "Destination path should reference table it_transfer_dst, but was: " + info.destinationPath);
    }
}
