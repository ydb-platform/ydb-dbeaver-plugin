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

import org.jkiss.dbeaver.model.DBPDataKind;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Tests for YDBDataSource.resolveDataKind(), getDefaultDataTypeName(), and getLocalDataType().
 * Uses Unsafe allocation to create YDBDataSource without a DB connection.
 */
public class YDBDataKindResolutionTest {

    private YDBDataSource dataSource;

    @Before
    public void setUp() throws Exception {
        dataSource = allocateInstance();
    }

    @SuppressWarnings("unchecked")
    private static YDBDataSource allocateInstance() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
        return (YDBDataSource) unsafe.allocateInstance(YDBDataSource.class);
    }

    // resolveDataKind — container types

    @Test
    public void testListAngleBracket() {
        assertEquals(DBPDataKind.ARRAY, dataSource.resolveDataKind("List<Int32>", 0));
    }

    @Test
    public void testListParen() {
        assertEquals(DBPDataKind.ARRAY, dataSource.resolveDataKind("List(String)", 0));
    }

    @Test
    public void testStructAngleBracket() {
        assertEquals(DBPDataKind.STRUCT, dataSource.resolveDataKind("Struct<a:Int32,b:Utf8>", 0));
    }

    @Test
    public void testStructParen() {
        assertEquals(DBPDataKind.STRUCT, dataSource.resolveDataKind("Struct(a:Int32)", 0));
    }

    @Test
    public void testTupleAngleBracket() {
        assertEquals(DBPDataKind.STRUCT, dataSource.resolveDataKind("Tuple<Int32,Utf8>", 0));
    }

    @Test
    public void testTupleParen() {
        assertEquals(DBPDataKind.STRUCT, dataSource.resolveDataKind("Tuple(Int32)", 0));
    }

    @Test
    public void testDictAngleBracket() {
        assertEquals(DBPDataKind.OBJECT, dataSource.resolveDataKind("Dict<Utf8,Int32>", 0));
    }

    @Test
    public void testDictParen() {
        assertEquals(DBPDataKind.OBJECT, dataSource.resolveDataKind("Dict(Utf8,Int32)", 0));
    }

    // resolveDataKind — JSON types

    @Test
    public void testJsonContent() {
        assertEquals(DBPDataKind.CONTENT, dataSource.resolveDataKind("Json", 0));
    }

    @Test
    public void testJsonDocumentContent() {
        assertEquals(DBPDataKind.CONTENT, dataSource.resolveDataKind("JsonDocument", 0));
    }

    @Test
    public void testYsonContent() {
        assertEquals(DBPDataKind.CONTENT, dataSource.resolveDataKind("Yson", 0));
    }

    // resolveDataKind — special types

    @Test
    public void testUuidString() {
        assertEquals(DBPDataKind.STRING, dataSource.resolveDataKind("Uuid", 0));
    }

    @Test
    public void testIntervalString() {
        assertEquals(DBPDataKind.STRING, dataSource.resolveDataKind("Interval", 0));
    }

    @Test
    public void testDyNumberNumeric() {
        assertEquals(DBPDataKind.NUMERIC, dataSource.resolveDataKind("DyNumber", 0));
    }

    // resolveDataKind — case insensitivity

    @Test
    public void testCaseInsensitiveJson() {
        assertEquals(DBPDataKind.CONTENT, dataSource.resolveDataKind("JSON", 0));
    }

    @Test
    public void testCaseInsensitiveList() {
        assertEquals(DBPDataKind.ARRAY, dataSource.resolveDataKind("LIST<Int32>", 0));
    }

    @Test
    public void testCaseInsensitiveUuid() {
        assertEquals(DBPDataKind.STRING, dataSource.resolveDataKind("UUID", 0));
    }

    // getDefaultDataTypeName

    @Test
    public void testDefaultStringType() {
        assertEquals("Utf8", dataSource.getDefaultDataTypeName(DBPDataKind.STRING));
    }

    @Test
    public void testDefaultNumericType() {
        assertEquals("Int64", dataSource.getDefaultDataTypeName(DBPDataKind.NUMERIC));
    }

    @Test
    public void testDefaultBooleanType() {
        assertEquals("Bool", dataSource.getDefaultDataTypeName(DBPDataKind.BOOLEAN));
    }

    @Test
    public void testDefaultDatetimeType() {
        assertEquals("Timestamp", dataSource.getDefaultDataTypeName(DBPDataKind.DATETIME));
    }

    @Test
    public void testDefaultBinaryType() {
        assertEquals("String", dataSource.getDefaultDataTypeName(DBPDataKind.BINARY));
    }

    // getLocalDataType — JSON types return null

    @Test
    public void testLocalDataTypeJsonReturnsNull() {
        assertNull(dataSource.getLocalDataType("JSON"));
    }

    @Test
    public void testLocalDataTypeJsonDocumentReturnsNull() {
        assertNull(dataSource.getLocalDataType("JsonDocument"));
    }

    @Test
    public void testLocalDataTypeJsonLowerCaseReturnsNull() {
        assertNull(dataSource.getLocalDataType("json"));
    }

    @Test
    public void testLocalDataTypeJsonDocumentLowerCaseReturnsNull() {
        assertNull(dataSource.getLocalDataType("jsondocument"));
    }
}
