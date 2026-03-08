package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBConnectionIT extends YDBBaseIT {

    @Test
    void testSelectOne() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next(), "Expected at least one row");
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testConnectionMetadata() throws Exception {
        DatabaseMetaData meta = connection.getMetaData();
        assertNotNull(meta, "DatabaseMetaData should not be null");
        String driverName = meta.getDriverName();
        assertNotNull(driverName, "Driver name should not be null");
        assertTrue(
            driverName.toLowerCase().contains("ydb"),
            "Driver name should contain 'ydb', but was: " + driverName
        );
    }
}
