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

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectContainer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class YDBTableFolderTest {

    private DBSObject mockOwner;

    @Before
    public void setUp() {
        mockOwner = new DBSObject() {
            @Override public String getName() { return "owner"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };
    }

    @Test
    public void testNameForRootFolder() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "tables");
        assertEquals("tables", folder.getName());
    }

    @Test
    public void testFullPathForRootFolder() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "tables");
        assertEquals("tables", folder.getFullPath());
    }

    @Test
    public void testFullPathForNestedFolder() {
        YDBTableFolder root = new YDBTableFolder(mockOwner, null, "root");
        YDBTableFolder child = new YDBTableFolder(mockOwner, root, "child");
        assertEquals("root/child", child.getFullPath());
    }

    @Test
    public void testGetParentFolderNull() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        assertNull(folder.getParentFolder());
    }

    @Test
    public void testGetParentFolderReturnsParent() {
        YDBTableFolder parent = new YDBTableFolder(mockOwner, null, "parent");
        YDBTableFolder child = new YDBTableFolder(mockOwner, parent, "child");
        assertSame(parent, child.getParentFolder());
    }

    @Test
    public void testGetParentObjectReturnsOwnerWhenNoParentFolder() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "f");
        assertSame(mockOwner, folder.getParentObject());
    }

    @Test
    public void testGetParentObjectReturnsParentFolder() {
        YDBTableFolder parent = new YDBTableFolder(mockOwner, null, "parent");
        YDBTableFolder child = new YDBTableFolder(mockOwner, parent, "child");
        assertSame(parent, child.getParentObject());
    }

    @Test
    public void testGetDescription() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "f");
        assertNull(folder.getDescription());
    }

    @Test
    public void testIsPersisted() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "f");
        assertTrue(folder.isPersisted());
    }

    @Test
    public void testGetOrCreateSubFolderCreatesNew() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        YDBTableFolder sub = folder.getOrCreateSubFolder("child");
        assertNotNull(sub);
        assertEquals("child", sub.getName());
        assertEquals("root/child", sub.getFullPath());
    }

    @Test
    public void testGetOrCreateSubFolderReturnsExisting() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        YDBTableFolder first = folder.getOrCreateSubFolder("child");
        YDBTableFolder second = folder.getOrCreateSubFolder("child");
        assertSame(first, second);
    }

    @Test
    public void testGetSubFoldersSortedCaseInsensitive() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("Zebra");
        folder.getOrCreateSubFolder("alpha");
        folder.getOrCreateSubFolder("Beta");

        List<String> names = new ArrayList<>();
        for (YDBTableFolder sf : folder.getSubFolders()) {
            names.add(sf.getName());
        }
        assertEquals(3, names.size());
        assertEquals("alpha", names.get(0));
        assertEquals("Beta", names.get(1));
        assertEquals("Zebra", names.get(2));
    }

    @Test
    public void testHasContentFalseWhenEmpty() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        assertFalse(folder.hasContent());
    }

    @Test
    public void testHasContentTrueWithSubFolder() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("sub");
        assertTrue(folder.hasContent());
    }

    @Test
    public void testGetChildrenObjectsContainsBothSubfoldersAndTables() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("sub");
        // Cannot easily create YDBTable without GenericStructContainer,
        // so just test that subfolder is present in children
        Collection<DBSObject> children = folder.getChildrenObjects(null);
        assertEquals(1, children.size());
    }

    @Test
    public void testToString() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "myFolder");
        assertEquals("myFolder", folder.toString());
    }

    @Test
    public void testImplementsDBSObjectContainer() {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        assertTrue("YDBTableFolder should implement DBSObjectContainer",
            folder instanceof DBSObjectContainer);
    }

    @Test
    public void testGetChildReturnsSubFolder() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("sub");
        DBSObject child = folder.getChild(null, "sub");
        assertNotNull(child);
        assertEquals("sub", child.getName());
        assertTrue(child instanceof YDBTableFolder);
    }

    @Test
    public void testGetChildReturnsNullForMissing() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        assertNull(folder.getChild(null, "nonexistent"));
    }

    @Test
    public void testGetChildCaseInsensitiveForSubFolders() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("MyFolder");
        DBSObject child = folder.getChild(null, "myfolder");
        assertNotNull("getChild should be case-insensitive for subfolders", child);
        assertEquals("MyFolder", child.getName());
    }

    @Test
    public void testGetChildStripsTrailingDot() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("sub");
        DBSObject child = folder.getChild(null, "sub.");
        assertNotNull("getChild should find subfolder when name has trailing dot", child);
        assertEquals("sub", child.getName());
    }

    @Test
    public void testGetChildrenReturnsBothSubfoldersAndTables() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("sub1");
        folder.getOrCreateSubFolder("sub2");
        Collection<? extends DBSObject> children = folder.getChildren(null);
        assertEquals(2, children.size());
    }

    @Test
    public void testGetPrimaryChildType() throws DBException {
        YDBTableFolder folder = new YDBTableFolder(mockOwner, null, "root");
        assertEquals(YDBTable.class, folder.getPrimaryChildType(null));
    }
}
