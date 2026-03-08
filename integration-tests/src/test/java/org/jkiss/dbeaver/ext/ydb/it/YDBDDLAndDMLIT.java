package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBDDLAndDMLIT extends YDBBaseIT {

    private static final String TABLE_DDL = "ddl_test";
    private static final String TABLE_DML = "dml_test";

    @Test
    void testCreateAndDropTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE " + TABLE_DDL + " (" +
                "    id Int32 NOT NULL," +
                "    name Utf8," +
                "    PRIMARY KEY (id)" +
                ")"
            );

            // Verify table exists by querying it
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_DDL)) {
                assertTrue(rs.next(), "Table should be queryable after creation");
            }

            stmt.execute("DROP TABLE " + TABLE_DDL);

            // Verify table no longer exists
            try {
                stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_DDL);
                fail("Table should not exist after DROP TABLE");
            } catch (Exception e) {
                // Expected - table should be gone
            }
        }
    }

    @Test
    void testInsertSelectDelete() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE " + TABLE_DML + " (" +
                "    id Int32 NOT NULL," +
                "    value Utf8," +
                "    PRIMARY KEY (id)" +
                ")"
            );
            try {
                // Insert rows
                stmt.execute("INSERT INTO " + TABLE_DML + " (id, value) VALUES (1, 'alpha')");
                stmt.execute("INSERT INTO " + TABLE_DML + " (id, value) VALUES (2, 'beta')");
                stmt.execute("INSERT INTO " + TABLE_DML + " (id, value) VALUES (3, 'gamma')");

                // Select and verify
                List<String> values = new ArrayList<>();
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT value FROM " + TABLE_DML + " ORDER BY id")) {
                    while (rs.next()) {
                        values.add(rs.getString("value"));
                    }
                }
                assertEquals(List.of("alpha", "beta", "gamma"), values);

                // Delete and verify empty
                stmt.execute("DELETE FROM " + TABLE_DML);
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_DML)) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getLong(1), "Table should be empty after DELETE");
                }
            } finally {
                stmt.execute("DROP TABLE " + TABLE_DML);
            }
        }
    }
}
