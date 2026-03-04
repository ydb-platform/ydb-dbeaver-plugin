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

import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests for YDBTransfer name extraction from path and stripPrefix logic.
 */
public class YDBTransferNameTest {

    private DBSObject mockParent;

    @Before
    public void setUp() {
        mockParent = new DBSObject() {
            @Override public String getName() { return "parent"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };
    }

    // Constructor name extraction

    @Test
    public void testNameExtractedFromPath() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "folder/myTransfer");
        assertEquals("myTransfer", transfer.getName());
    }

    @Test
    public void testNameExtractedFromDeepPath() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "a/b/c/deep");
        assertEquals("deep", transfer.getName());
    }

    @Test
    public void testSimpleNameNoSlash() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "simpleName");
        assertEquals("simpleName", transfer.getName());
    }

    @Test
    public void testFullPathPreserved() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "folder/myTransfer");
        assertEquals("folder/myTransfer", transfer.getFullPath());
    }

    @Test
    public void testExplicitNameConstructor() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "customName", "folder/original");
        assertEquals("customName", transfer.getName());
        assertEquals("folder/original", transfer.getFullPath());
    }

    @Test
    public void testIsPersisted() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        assertTrue(transfer.isPersisted());
    }

    @Test
    public void testToString() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "folder/myTransfer");
        assertEquals("myTransfer", transfer.toString());
    }

    @Test
    public void testGetParentObject() {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        assertSame(mockParent, transfer.getParentObject());
    }

    // stripPrefix tests via reflection

    private String invokeStripPrefix(YDBTransfer transfer, String path) throws Exception {
        Method method = YDBTransfer.class.getDeclaredMethod("stripPrefix", String.class);
        method.setAccessible(true);
        return (String) method.invoke(transfer, path);
    }

    private void setPrefixPath(YDBTransfer transfer, String prefix) throws Exception {
        Field field = YDBTransfer.class.getDeclaredField("prefixPath");
        field.setAccessible(true);
        field.set(transfer, prefix);
    }

    @Test
    public void testStripPrefixNull() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        assertNull(invokeStripPrefix(transfer, null));
    }

    @Test
    public void testStripPrefixLeadingSlash() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        assertEquals("folder/table", invokeStripPrefix(transfer, "/folder/table"));
    }

    @Test
    public void testStripPrefixDoubleLeadingSlash() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        assertEquals("folder/table", invokeStripPrefix(transfer, "//folder/table"));
    }

    @Test
    public void testStripPrefixWithPrefixPath() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        setPrefixPath(transfer, "/ydb/mydb");
        assertEquals("table", invokeStripPrefix(transfer, "/ydb/mydb/table"));
    }

    @Test
    public void testStripPrefixNormalizesDoubleSlash() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        setPrefixPath(transfer, "//ydb/mydb");
        assertEquals("table", invokeStripPrefix(transfer, "//ydb/mydb/table"));
    }

    @Test
    public void testStripPrefixNoMatch() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        setPrefixPath(transfer, "/other/db");
        // path starts with "/" but prefix doesn't match — fallback strips leading slashes
        assertEquals("ydb/mydb/table", invokeStripPrefix(transfer, "/ydb/mydb/table"));
    }

    @Test
    public void testStripPrefixPlainPath() throws Exception {
        YDBTransfer transfer = new YDBTransfer(mockParent, "t");
        assertEquals("folder/table", invokeStripPrefix(transfer, "folder/table"));
    }
}
