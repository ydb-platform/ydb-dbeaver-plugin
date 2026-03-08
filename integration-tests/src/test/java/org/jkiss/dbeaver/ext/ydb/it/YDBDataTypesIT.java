package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.*;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBDataTypesIT extends YDBBaseIT {

    private static final String TABLE = "data_types_test";

    @BeforeAll
    void createTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE " + TABLE + " (" +
                "    id Int32 NOT NULL," +
                "    int_val Int32," +
                "    text_val Utf8," +
                "    bool_val Bool," +
                "    double_val Double," +
                "    ts_val Timestamp," +
                "    PRIMARY KEY (id)" +
                ")"
            );
        }
    }

    @AfterAll
    void dropTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE " + TABLE);
        }
    }

    @Test
    void testBasicDataTypes() throws Exception {
        Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        Timestamp ts = Timestamp.from(now);

        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, int_val, text_val, bool_val, double_val, ts_val) " +
                "VALUES (?, ?, ?, ?, ?, ?)")) {
            insert.setInt(1, 42);
            insert.setInt(2, 12345);
            insert.setString(3, "Hello, YDB!");
            insert.setBoolean(4, true);
            insert.setDouble(5, 3.14159);
            insert.setTimestamp(6, ts);
            insert.execute();
        }

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, int_val, text_val, bool_val, double_val, ts_val " +
                     "FROM " + TABLE + " WHERE id = 42")) {

            assertTrue(rs.next(), "Expected a row for id=42");

            assertEquals(42, rs.getInt("id"));
            assertEquals(12345, rs.getInt("int_val"));
            assertEquals("Hello, YDB!", rs.getString("text_val"));
            assertTrue(rs.getBoolean("bool_val"));
            assertEquals(3.14159, rs.getDouble("double_val"), 1e-9);
            assertEquals(ts, rs.getTimestamp("ts_val"));
        }

        // Cleanup
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM " + TABLE + " WHERE id = 42");
        }
    }
}
