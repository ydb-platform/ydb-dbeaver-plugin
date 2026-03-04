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
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.struct.DBSEntityType;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for YDBView name extraction, entity type, and fully qualified name.
 */
public class YDBViewNameTest {

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

    @Test
    public void testNameFromPath() {
        YDBView view = new YDBView(mockParent, "folder/myView");
        assertEquals("myView", view.getName());
    }

    @Test
    public void testNameSimple() {
        YDBView view = new YDBView(mockParent, "simpleView");
        assertEquals("simpleView", view.getName());
    }

    @Test
    public void testFullPath() {
        YDBView view = new YDBView(mockParent, "folder/myView");
        assertEquals("folder/myView", view.getFullPath());
    }

    @Test
    public void testExplicitNameConstructor() {
        YDBView view = new YDBView(mockParent, "customName", "folder/original");
        assertEquals("customName", view.getName());
        assertEquals("folder/original", view.getFullPath());
    }

    @Test
    public void testEntityType() {
        YDBView view = new YDBView(mockParent, "v");
        assertEquals(DBSEntityType.VIEW, view.getEntityType());
    }

    @Test
    public void testFullyQualifiedName() {
        YDBView view = new YDBView(mockParent, "folder/myView");
        assertEquals("`folder/myView`", view.getFullyQualifiedName(DBPEvaluationContext.DML));
    }

    @Test
    public void testIsPersisted() {
        YDBView view = new YDBView(mockParent, "v");
        assertTrue(view.isPersisted());
    }

    @Test
    public void testToString() {
        YDBView view = new YDBView(mockParent, "folder/myView");
        assertEquals("myView", view.toString());
    }
}
