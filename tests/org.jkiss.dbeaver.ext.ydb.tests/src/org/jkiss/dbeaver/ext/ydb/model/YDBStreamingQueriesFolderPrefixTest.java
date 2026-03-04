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

import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests for YDBStreamingQueriesFolder.getDatabasePrefix() and stripDatabasePrefix().
 * Uses reflection to access private methods and set dataSource with mock container.
 */
public class YDBStreamingQueriesFolderPrefixTest {

    private YDBStreamingQueriesFolder createFolderWithDbName(String dbName) throws Exception {
        java.lang.reflect.Constructor<YDBStreamingQueriesFolder> ctor =
            YDBStreamingQueriesFolder.class.getDeclaredConstructor(YDBDataSource.class);
        ctor.setAccessible(true);

        // Create a minimal mock for the dataSource → container → connectionConfiguration chain
        sun.misc.Unsafe unsafe = getUnsafe();
        YDBDataSource ds = (YDBDataSource) unsafe.allocateInstance(YDBDataSource.class);

        // We need ds.getContainer().getConnectionConfiguration().getDatabaseName()
        // GenericDataSource stores the container in a field inherited from DBPDataSourceBase
        // Use a proxy approach: create a real DBPConnectionConfiguration and set it up
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setDatabaseName(dbName);

        // Create an anonymous container that returns our config
        DBPDataSourceContainer mockContainer = createMockContainer(config);

        // Set the container field on YDBDataSource (inherited from DBPDataSourceBase)
        setContainerViaReflection(ds, mockContainer);

        YDBStreamingQueriesFolder folder = ctor.newInstance((YDBDataSource) null);
        Field dsField = YDBStreamingQueriesFolder.class.getDeclaredField("dataSource");
        dsField.setAccessible(true);
        dsField.set(folder, ds);

        return folder;
    }

    private static sun.misc.Unsafe getUnsafe() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (sun.misc.Unsafe) f.get(null);
    }

    private void setContainerViaReflection(YDBDataSource ds, DBPDataSourceContainer container) throws Exception {
        // The container is stored in DBPDataSourceBase (or similar superclass)
        // Walk up the class hierarchy to find the 'container' field
        Class<?> clazz = ds.getClass();
        while (clazz != null) {
            try {
                Field containerField = clazz.getDeclaredField("container");
                containerField.setAccessible(true);
                containerField.set(ds, container);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("container field not found in hierarchy");
    }

    private DBPDataSourceContainer createMockContainer(DBPConnectionConfiguration config) {
        return (DBPDataSourceContainer) java.lang.reflect.Proxy.newProxyInstance(
            DBPDataSourceContainer.class.getClassLoader(),
            new Class[]{DBPDataSourceContainer.class},
            (proxy, method, args) -> {
                if ("getConnectionConfiguration".equals(method.getName())) {
                    return config;
                }
                if ("getActualConnectionConfiguration".equals(method.getName())) {
                    return config;
                }
                if ("getPreferredLocale".equals(method.getName())) {
                    return null;
                }
                return null;
            }
        );
    }

    private String invokeGetDatabasePrefix(YDBStreamingQueriesFolder folder) throws Exception {
        Method method = YDBStreamingQueriesFolder.class.getDeclaredMethod("getDatabasePrefix");
        method.setAccessible(true);
        return (String) method.invoke(folder);
    }

    private String invokeStripDatabasePrefix(YDBStreamingQueriesFolder folder, String path) throws Exception {
        Method method = YDBStreamingQueriesFolder.class.getDeclaredMethod("stripDatabasePrefix", String.class);
        method.setAccessible(true);
        return (String) method.invoke(folder, path);
    }

    // getDatabasePrefix tests

    @Test
    public void testGetDatabasePrefixNormal() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("/ydb-cluster/mydb");
        assertEquals("/ydb-cluster/mydb/", invokeGetDatabasePrefix(folder));
    }

    @Test
    public void testGetDatabasePrefixNoLeadingSlash() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("mydb");
        assertEquals("/mydb/", invokeGetDatabasePrefix(folder));
    }

    @Test
    public void testGetDatabasePrefixEmpty() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("");
        assertEquals("", invokeGetDatabasePrefix(folder));
    }

    @Test
    public void testGetDatabasePrefixNull() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName(null);
        assertEquals("", invokeGetDatabasePrefix(folder));
    }

    // stripDatabasePrefix tests

    @Test
    public void testStripPrefixMatching() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("/ydb-cluster/mydb");
        assertEquals("folder/query", invokeStripDatabasePrefix(folder, "/ydb-cluster/mydb/folder/query"));
    }

    @Test
    public void testStripPrefixNoMatch() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("/ydb-cluster/mydb");
        // Path doesn't match prefix — fallback strips leading "/"
        assertEquals("other/path", invokeStripDatabasePrefix(folder, "/other/path"));
    }

    @Test
    public void testStripPrefixEmptyDb() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("");
        // No prefix — strips leading "/"
        assertEquals("folder/query", invokeStripDatabasePrefix(folder, "/folder/query"));
    }

    @Test
    public void testStripPrefixNoLeadingSlash() throws Exception {
        YDBStreamingQueriesFolder folder = createFolderWithDbName("");
        assertEquals("folder/query", invokeStripDatabasePrefix(folder, "folder/query"));
    }
}
