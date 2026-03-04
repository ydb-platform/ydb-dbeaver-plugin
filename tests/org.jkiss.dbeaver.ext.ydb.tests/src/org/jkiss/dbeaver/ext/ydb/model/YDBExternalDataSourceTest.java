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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for YDBExternalDataSource name extraction and getPropertyIgnoreCase.
 */
public class YDBExternalDataSourceTest {

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

    // Name extraction

    @Test
    public void testNameFromPath() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "folder/myEds");
        assertEquals("myEds", eds.getName());
    }

    @Test
    public void testNameSimple() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "simpleEds");
        assertEquals("simpleEds", eds.getName());
    }

    @Test
    public void testFullPath() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "folder/myEds");
        assertEquals("folder/myEds", eds.getFullPath());
    }

    @Test
    public void testExplicitNameConstructor() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "customName", "folder/original");
        assertEquals("customName", eds.getName());
        assertEquals("folder/original", eds.getFullPath());
    }

    @Test
    public void testIsPersisted() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "e");
        assertTrue(eds.isPersisted());
    }

    @Test
    public void testToString() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "folder/myEds");
        assertEquals("myEds", eds.toString());
    }

    // getPropertyIgnoreCase via reflection

    private String invokeGetPropertyIgnoreCase(Map<String, String> props, String key) throws Exception {
        Method method = YDBExternalDataSource.class.getDeclaredMethod(
            "getPropertyIgnoreCase", Map.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, props, key);
    }

    @Test
    public void testGetPropertyExactMatch() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("DATABASE_NAME", "mydb");
        assertEquals("mydb", invokeGetPropertyIgnoreCase(props, "DATABASE_NAME"));
    }

    @Test
    public void testGetPropertyLowercaseFallback() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("database_name", "mydb");
        assertEquals("mydb", invokeGetPropertyIgnoreCase(props, "DATABASE_NAME"));
    }

    @Test
    public void testGetPropertyMissingKey() throws Exception {
        Map<String, String> props = new HashMap<>();
        assertNull(invokeGetPropertyIgnoreCase(props, "DATABASE_NAME"));
    }

    @Test
    public void testGetPropertyExactPreferred() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AUTH_METHOD", "TOKEN");
        props.put("auth_method", "password");
        // Exact match should be preferred
        assertEquals("TOKEN", invokeGetPropertyIgnoreCase(props, "AUTH_METHOD"));
    }
}
