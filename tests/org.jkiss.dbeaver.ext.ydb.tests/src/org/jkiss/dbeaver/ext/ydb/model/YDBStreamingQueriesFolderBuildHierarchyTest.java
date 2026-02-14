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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

/**
 * Tests for the buildHierarchy logic in YDBStreamingQueriesFolder.
 * Uses reflection to access the private buildHierarchy method and internal fields.
 */
public class YDBStreamingQueriesFolderBuildHierarchyTest {

    private YDBStreamingQueriesFolder folder;

    @Before
    public void setUp() throws Exception {
        // YDBStreamingQueriesFolder requires a YDBDataSource in constructor,
        // but buildHierarchy only uses the folder itself as parent for queries.
        // Use reflection to create instance with null dataSource and set internal fields.
        folder = createFolderWithReflection();
    }

    private YDBStreamingQueriesFolder createFolderWithReflection() throws Exception {
        // Use sun.misc.Unsafe or just use the constructor with null and handle the NPE
        // Actually, let's use reflection to allocate without calling constructor
        java.lang.reflect.Constructor<YDBStreamingQueriesFolder> ctor =
            YDBStreamingQueriesFolder.class.getDeclaredConstructor(YDBDataSource.class);
        ctor.setAccessible(true);
        return ctor.newInstance((YDBDataSource) null);
    }

    private void initInternalFields() throws Exception {
        Field rootFoldersField = YDBStreamingQueriesFolder.class.getDeclaredField("rootFolders");
        rootFoldersField.setAccessible(true);
        rootFoldersField.set(folder, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));

        Field rootQueriesField = YDBStreamingQueriesFolder.class.getDeclaredField("rootQueries");
        rootQueriesField.setAccessible(true);
        rootQueriesField.set(folder, new ArrayList<>());
    }

    private void invokeBuildHierarchy(List<YDBStreamingQuery> allQueries) throws Exception {
        Method method = YDBStreamingQueriesFolder.class.getDeclaredMethod("buildHierarchy", List.class);
        method.setAccessible(true);
        method.invoke(folder, allQueries);
    }

    @SuppressWarnings("unchecked")
    private List<YDBStreamingQuery> getRootQueries() throws Exception {
        Field field = YDBStreamingQueriesFolder.class.getDeclaredField("rootQueries");
        field.setAccessible(true);
        return (List<YDBStreamingQuery>) field.get(folder);
    }

    @SuppressWarnings("unchecked")
    private Map<String, YDBStreamingQueryFolder> getRootFolders() throws Exception {
        Field field = YDBStreamingQueriesFolder.class.getDeclaredField("rootFolders");
        field.setAccessible(true);
        return (Map<String, YDBStreamingQueryFolder>) field.get(folder);
    }

    private YDBStreamingQuery makeQuery(String path) {
        return new YDBStreamingQuery(folder, path, path, "RUNNING", null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testRootQueryNoSlash() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        queries.add(makeQuery("simpleQuery"));

        invokeBuildHierarchy(queries);

        List<YDBStreamingQuery> roots = getRootQueries();
        assertEquals(1, roots.size());
        assertEquals("simpleQuery", roots.get(0).getName());
    }

    @Test
    public void testNestedQueryCreatesFolder() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        queries.add(makeQuery("folder/queryName"));

        invokeBuildHierarchy(queries);

        Map<String, YDBStreamingQueryFolder> folders = getRootFolders();
        assertEquals(1, folders.size());
        assertTrue(folders.containsKey("folder"));
        YDBStreamingQueryFolder f = folders.get("folder");
        assertEquals(1, f.getQueries().size());
        assertEquals("queryName", f.getQueries().get(0).getName());
    }

    @Test
    public void testDeepNesting() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        queries.add(makeQuery("a/b/c/deepQuery"));

        invokeBuildHierarchy(queries);

        Map<String, YDBStreamingQueryFolder> rootFolders = getRootFolders();
        assertEquals(1, rootFolders.size());
        assertTrue(rootFolders.containsKey("a"));

        YDBStreamingQueryFolder a = rootFolders.get("a");
        assertEquals(1, a.getSubFolders().size());
        YDBStreamingQueryFolder b = a.getOrCreateSubFolder("b");
        assertEquals(1, b.getSubFolders().size());
        YDBStreamingQueryFolder c = b.getOrCreateSubFolder("c");
        assertEquals(1, c.getQueries().size());
        assertEquals("deepQuery", c.getQueries().get(0).getName());
    }

    @Test
    public void testMixedRootAndNested() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        queries.add(makeQuery("rootQ"));
        queries.add(makeQuery("folder/nestedQ"));

        invokeBuildHierarchy(queries);

        List<YDBStreamingQuery> roots = getRootQueries();
        assertEquals(1, roots.size());
        assertEquals("rootQ", roots.get(0).getName());

        Map<String, YDBStreamingQueryFolder> folders = getRootFolders();
        assertEquals(1, folders.size());
        assertEquals(1, folders.get("folder").getQueries().size());
    }

    @Test
    public void testRootQueriesSorted() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        queries.add(makeQuery("Zebra"));
        queries.add(makeQuery("alpha"));
        queries.add(makeQuery("Beta"));

        invokeBuildHierarchy(queries);

        List<YDBStreamingQuery> roots = getRootQueries();
        assertEquals(3, roots.size());
        assertEquals("alpha", roots.get(0).getName());
        assertEquals("Beta", roots.get(1).getName());
        assertEquals("Zebra", roots.get(2).getName());
    }

    @Test
    public void testMultipleQueriesInSameFolder() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        queries.add(makeQuery("folder/q1"));
        queries.add(makeQuery("folder/q2"));

        invokeBuildHierarchy(queries);

        Map<String, YDBStreamingQueryFolder> folders = getRootFolders();
        assertEquals(1, folders.size());
        YDBStreamingQueryFolder f = folders.get("folder");
        assertEquals(2, f.getQueries().size());
    }

    @Test
    public void testEmptyPathSegmentsSkipped() throws Exception {
        initInternalFields();
        List<YDBStreamingQuery> queries = new ArrayList<>();
        // Path like "/folder/query" — leading empty segment should be skipped
        queries.add(makeQuery("/folder/query"));

        invokeBuildHierarchy(queries);

        Map<String, YDBStreamingQueryFolder> folders = getRootFolders();
        // "folder" should be created (empty segment "" is skipped)
        assertTrue(folders.containsKey("folder"));
    }

    @Test
    public void testEmptyList() throws Exception {
        initInternalFields();
        invokeBuildHierarchy(new ArrayList<>());

        List<YDBStreamingQuery> roots = getRootQueries();
        assertTrue(roots.isEmpty());
        Map<String, YDBStreamingQueryFolder> folders = getRootFolders();
        assertTrue(folders.isEmpty());
    }
}
