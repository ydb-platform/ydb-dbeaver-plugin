/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.ydb.model;

import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Tests for YDBTable name parsing, hierarchical names, folder path, and type.
 * Uses reflection to set internal fields since GenericTable constructor is complex.
 */
public class YDBTableNameTest {

    private YDBTable table;

    private YDBTable createTable(String tableName, boolean columnTable) throws Exception {
        YDBTable t = allocateInstance();
        Field fullNameField = YDBTable.class.getDeclaredField("fullName");
        fullNameField.setAccessible(true);
        fullNameField.set(t, tableName);

        Field shortNameField = YDBTable.class.getDeclaredField("shortName");
        shortNameField.setAccessible(true);
        if (tableName != null && tableName.contains("/")) {
            shortNameField.set(t, tableName.substring(tableName.lastIndexOf('/') + 1));
        } else {
            shortNameField.set(t, tableName);
        }

        Field columnTableField = YDBTable.class.getDeclaredField("columnTable");
        columnTableField.setAccessible(true);
        columnTableField.setBoolean(t, columnTable);

        return t;
    }

    @SuppressWarnings("unchecked")
    private static YDBTable allocateInstance() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
        return (YDBTable) unsafe.allocateInstance(YDBTable.class);
    }

    // getName tests

    @Test
    public void testGetNameHierarchical() throws Exception {
        table = createTable("folder/sub/myTable", false);
        assertEquals("myTable", table.getName());
    }

    @Test
    public void testGetNameSimple() throws Exception {
        table = createTable("simpleTable", false);
        assertEquals("simpleTable", table.getName());
    }

    @Test
    public void testGetNameNull() throws Exception {
        table = createTable(null, false);
        assertEquals("", table.getName());
    }

    // hasHierarchicalName tests

    @Test
    public void testHasHierarchicalNameTrue() throws Exception {
        table = createTable("folder/sub/myTable", false);
        assertTrue(table.hasHierarchicalName());
    }

    @Test
    public void testHasHierarchicalNameFalse() throws Exception {
        table = createTable("simpleTable", false);
        assertFalse(table.hasHierarchicalName());
    }

    @Test
    public void testHasHierarchicalNameNull() throws Exception {
        table = createTable(null, false);
        assertFalse(table.hasHierarchicalName());
    }

    // getFolderPath tests

    @Test
    public void testGetFolderPath() throws Exception {
        table = createTable("folder/sub/myTable", false);
        assertEquals("folder/sub", table.getFolderPath());
    }

    @Test
    public void testGetFolderPathSimple() throws Exception {
        table = createTable("simpleTable", false);
        assertNull(table.getFolderPath());
    }

    // getFullTableName tests

    @Test
    public void testGetFullTableName() throws Exception {
        table = createTable("folder/sub/myTable", false);
        assertEquals("folder/sub/myTable", table.getFullTableName());
    }

    @Test
    public void testGetFullTableNameNull() throws Exception {
        table = createTable(null, false);
        assertEquals("", table.getFullTableName());
    }

    // getUniqueName

    @Test
    public void testGetUniqueName() throws Exception {
        table = createTable("folder/myTable", false);
        assertEquals("folder/myTable", table.getUniqueName());
    }

    // Table type

    @Test
    public void testRowTableType() throws Exception {
        table = createTable("t", false);
        assertEquals("Row", table.getTableType());
        assertFalse(table.isColumnTable());
    }

    @Test
    public void testColumnTableType() throws Exception {
        table = createTable("t", true);
        assertEquals("Column", table.getTableType());
        assertTrue(table.isColumnTable());
    }

    // getFullyQualifiedName

    @Test
    public void testFullyQualifiedNameDML() throws Exception {
        table = createTable("folder/myTable", false);
        assertEquals("`folder/myTable`", table.getFullyQualifiedName(DBPEvaluationContext.DML));
    }

    @Test
    public void testFullyQualifiedNameDDL() throws Exception {
        table = createTable("folder/myTable", false);
        assertEquals("`folder/myTable`", table.getFullyQualifiedName(DBPEvaluationContext.DDL));
    }

    // toString

    @Test
    public void testToString() throws Exception {
        table = createTable("folder/myTable", false);
        assertEquals("myTable", table.toString());
    }
}
