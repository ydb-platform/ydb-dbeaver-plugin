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

import java.lang.reflect.Constructor;

import static org.junit.Assert.*;

/**
 * Tests for YDBLinkedObject — wrapper for clickable links in properties.
 * YDBLinkedObject is package-private and in a different OSGi classloader,
 * so we use reflection to construct instances but test through the DBSObject interface.
 */
public class YDBLinkedObjectTest {

    private DBSObject realObject;
    private DBSObject otherObject;

    @Before
    public void setUp() {
        realObject = new DBSObject() {
            @Override public String getName() { return "realName"; }
            @Override public String getDescription() { return "real description"; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };

        otherObject = new DBSObject() {
            @Override public String getName() { return "otherName"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return false; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };
    }

    private DBSObject createLinkedObject(DBSObject real, String displayPath) throws Exception {
        Class<?> clazz = Class.forName("org.jkiss.dbeaver.ext.ydb.model.YDBLinkedObject",
            true, YDBDataSource.class.getClassLoader());
        Constructor<?> ctor = clazz.getDeclaredConstructor(DBSObject.class, String.class);
        ctor.setAccessible(true);
        return (DBSObject) ctor.newInstance(real, displayPath);
    }

    @Test
    public void testGetNameReturnsDisplayPath() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "folder/sub/realName");
        assertEquals("folder/sub/realName", linked.getName());
    }

    @Test
    public void testGetNameDoesNotReturnRealName() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "custom/path");
        assertNotEquals("realName", linked.getName());
        assertEquals("custom/path", linked.getName());
    }

    @Test
    public void testGetDescriptionDelegatesToReal() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "path");
        assertEquals("real description", linked.getDescription());
    }

    @Test
    public void testIsPersistedDelegatesToReal() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "path");
        assertTrue(linked.isPersisted());
    }

    @Test
    public void testGetParentObjectDelegatesToReal() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "path");
        assertNull(linked.getParentObject());
    }

    @Test
    public void testEqualsWithSameRealObject() throws Exception {
        DBSObject linked1 = createLinkedObject(realObject, "path1");
        DBSObject linked2 = createLinkedObject(realObject, "path2");
        assertEquals(linked1, linked2);
    }

    @Test
    public void testEqualsWithRawRealObject() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "path");
        assertEquals(linked, realObject);
    }

    @Test
    public void testEqualsWithDifferentRealObject() throws Exception {
        DBSObject linked1 = createLinkedObject(realObject, "path");
        DBSObject linked2 = createLinkedObject(otherObject, "path");
        assertNotEquals(linked1, linked2);
    }

    @Test
    public void testEqualsWithNull() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "path");
        assertNotEquals(null, linked);
    }

    @Test
    public void testHashCodeMatchesReal() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "path");
        assertEquals(realObject.hashCode(), linked.hashCode());
    }

    @Test
    public void testToStringReturnsDisplayPath() throws Exception {
        DBSObject linked = createLinkedObject(realObject, "some/display/path");
        assertEquals("some/display/path", linked.toString());
    }
}
