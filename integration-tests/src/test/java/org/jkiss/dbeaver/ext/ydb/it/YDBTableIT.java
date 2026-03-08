package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class YDBTableIT extends YDBBaseIT {

    @BeforeAll
    void createTables() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE it_row_table (" +
                "    id Int32 NOT NULL," +
                "    name Utf8," +
                "    value Double," +
                "    PRIMARY KEY (id)" +
                ")"
            );
            stmt.execute(
                "CREATE TABLE it_col_table (" +
                "    id Int32 NOT NULL," +
                "    val Int64," +
                "    PRIMARY KEY (id)" +
                ") WITH (STORE = COLUMN)"
            );
        }
    }

    @AfterAll
    void dropTables() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE it_row_table");
            stmt.execute("DROP TABLE it_col_table");
        }
    }

    @Test
    void testRowTableEnumerated() throws Exception {
        DatabaseMetaData meta = connection.getMetaData();
        boolean found = false;
        try (ResultSet rs = meta.getTables(null, null, "it_row_table", null)) {
            while (rs.next()) {
                if ("it_row_table".equals(rs.getString("TABLE_NAME"))) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found, "it_row_table should be enumerated by getTables()");
    }

    @Test
    void testRowTableColumns() throws Exception {
        DatabaseMetaData meta = connection.getMetaData();
        List<String> cols = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, null, "it_row_table", null)) {
            while (rs.next()) {
                cols.add(rs.getString("COLUMN_NAME"));
            }
        }
        assertEquals(3, cols.size(), "it_row_table should have 3 columns, got: " + cols);
        assertTrue(cols.contains("id"), "Column 'id' not found");
        assertTrue(cols.contains("name"), "Column 'name' not found");
        assertTrue(cols.contains("value"), "Column 'value' not found");
    }

    @Test
    void testRowTableQueryable() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM it_row_table LIMIT 0")) {
            assertNotNull(rs.getMetaData(), "ResultSet metadata should not be null");
        }
    }

    @Test
    void testColTableQueryable() throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM it_col_table LIMIT 0")) {
            assertNotNull(rs.getMetaData(), "ResultSet metadata should not be null");
        }
    }
}
