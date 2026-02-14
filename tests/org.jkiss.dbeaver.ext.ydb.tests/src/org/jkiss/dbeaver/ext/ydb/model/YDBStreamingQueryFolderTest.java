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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class YDBStreamingQueryFolderTest {

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

    private YDBStreamingQuery makeQuery(DBSObject parent, String name) {
        return new YDBStreamingQuery(parent, name, name, "RUNNING", null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testNameForRootFolder() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "myFolder");
        assertEquals("myFolder", folder.getName());
    }

    @Test
    public void testFullPathForRootFolder() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "root");
        assertEquals("root", folder.getFullPath());
    }

    @Test
    public void testFullPathForNestedFolder() {
        YDBStreamingQueryFolder root = new YDBStreamingQueryFolder(mockOwner, null, "root");
        YDBStreamingQueryFolder child = new YDBStreamingQueryFolder(mockOwner, root, "child");
        assertEquals("root/child", child.getFullPath());
    }

    @Test
    public void testFullPathForDeepNesting() {
        YDBStreamingQueryFolder root = new YDBStreamingQueryFolder(mockOwner, null, "a");
        YDBStreamingQueryFolder mid = new YDBStreamingQueryFolder(mockOwner, root, "b");
        YDBStreamingQueryFolder leaf = new YDBStreamingQueryFolder(mockOwner, mid, "c");
        assertEquals("a/b/c", leaf.getFullPath());
    }

    @Test
    public void testGetDescription() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "f");
        assertNull(folder.getDescription());
    }

    @Test
    public void testIsPersisted() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "f");
        assertTrue(folder.isPersisted());
    }

    @Test
    public void testGetParentObjectReturnsOwnerWhenNoParentFolder() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "f");
        assertSame(mockOwner, folder.getParentObject());
    }

    @Test
    public void testGetParentObjectReturnsParentFolder() {
        YDBStreamingQueryFolder parent = new YDBStreamingQueryFolder(mockOwner, null, "parent");
        YDBStreamingQueryFolder child = new YDBStreamingQueryFolder(mockOwner, parent, "child");
        assertSame(parent, child.getParentObject());
    }

    @Test
    public void testAddQueryAndGetQueries() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "f");
        YDBStreamingQuery q1 = makeQuery(folder, "beta");
        YDBStreamingQuery q2 = makeQuery(folder, "alpha");
        folder.addQuery(q1);
        folder.addQuery(q2);

        List<YDBStreamingQuery> queries = folder.getQueries();
        assertEquals(2, queries.size());
        // Should be sorted case-insensitive
        assertEquals("alpha", queries.get(0).getName());
        assertEquals("beta", queries.get(1).getName());
    }

    @Test
    public void testGetQueriesSortedCaseInsensitive() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "f");
        folder.addQuery(makeQuery(folder, "Zebra"));
        folder.addQuery(makeQuery(folder, "apple"));
        folder.addQuery(makeQuery(folder, "Banana"));

        List<YDBStreamingQuery> queries = folder.getQueries();
        assertEquals("apple", queries.get(0).getName());
        assertEquals("Banana", queries.get(1).getName());
        assertEquals("Zebra", queries.get(2).getName());
    }

    @Test
    public void testGetOrCreateSubFolderCreatesNew() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "root");
        YDBStreamingQueryFolder sub = folder.getOrCreateSubFolder("child");
        assertNotNull(sub);
        assertEquals("child", sub.getName());
        assertEquals("root/child", sub.getFullPath());
    }

    @Test
    public void testGetOrCreateSubFolderReturnsExisting() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "root");
        YDBStreamingQueryFolder first = folder.getOrCreateSubFolder("child");
        YDBStreamingQueryFolder second = folder.getOrCreateSubFolder("child");
        assertSame(first, second);
    }

    @Test
    public void testGetSubFoldersSortedCaseInsensitive() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("Zebra");
        folder.getOrCreateSubFolder("alpha");
        folder.getOrCreateSubFolder("Beta");

        List<String> names = new ArrayList<>();
        for (YDBStreamingQueryFolder sf : folder.getSubFolders()) {
            names.add(sf.getName());
        }
        assertEquals(3, names.size());
        assertEquals("alpha", names.get(0));
        assertEquals("Beta", names.get(1));
        assertEquals("Zebra", names.get(2));
    }

    @Test
    public void testGetChildrenObjectsContainsBothSubfoldersAndQueries() throws DBException {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "root");
        folder.getOrCreateSubFolder("sub");
        folder.addQuery(makeQuery(folder, "q1"));

        Collection<DBSObject> children = folder.getChildrenObjects(null);
        assertEquals(2, children.size());
    }

    @Test
    public void testToString() {
        YDBStreamingQueryFolder folder = new YDBStreamingQueryFolder(mockOwner, null, "myFolder");
        assertEquals("myFolder", folder.toString());
    }
}
