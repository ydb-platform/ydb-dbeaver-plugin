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

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests for YDB version parsing and topic reading support logic.
 * Uses reflection to set the ydbVersion field and invoke isTopicReadingSupported
 * without a real database connection.
 */
public class YDBVersionParsingTest {

    private YDBDataSource dataSource;

    @Before
    public void setUp() throws Exception {
        java.lang.reflect.Constructor<YDBDataSource> ctor =
            YDBDataSource.class.getDeclaredConstructor(
                org.jkiss.dbeaver.model.runtime.DBRProgressMonitor.class,
                org.jkiss.dbeaver.model.DBPDataSourceContainer.class,
                org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel.class
            );
        // Cannot construct without a real connection — use Unsafe to allocate
        dataSource = allocateInstance();
    }

    @SuppressWarnings("unchecked")
    private static YDBDataSource allocateInstance() throws Exception {
        // Allocate without calling constructor to avoid DB connection
        sun.misc.Unsafe unsafe = getUnsafe();
        return (YDBDataSource) unsafe.allocateInstance(YDBDataSource.class);
    }

    private static sun.misc.Unsafe getUnsafe() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (sun.misc.Unsafe) f.get(null);
    }

    private void setVersion(String version) throws Exception {
        Field field = YDBDataSource.class.getDeclaredField("ydbVersion");
        field.setAccessible(true);
        field.set(dataSource, version);
    }

    private boolean isTopicReadingSupported() throws Exception {
        Method method = YDBDataSource.class.getDeclaredMethod(
            "isTopicReadingSupported",
            org.jkiss.dbeaver.model.runtime.DBRProgressMonitor.class
        );
        // ydbVersion is already set, so getYDBVersion won't call DB
        return (boolean) method.invoke(dataSource, (org.jkiss.dbeaver.model.runtime.DBRProgressMonitor) null);
    }

    // Supported versions (major > 26, or major == 26 && minor >= 1)

    @Test
    public void testStable26_1_Supported() throws Exception {
        setVersion("stable-26-1");
        assertTrue(isTopicReadingSupported());
    }

    @Test
    public void testStable26_1_Hotfix_Supported() throws Exception {
        setVersion("stable-26-1-hotfix-2");
        assertTrue(isTopicReadingSupported());
    }

    @Test
    public void testStable27_0_Supported() throws Exception {
        setVersion("stable-27-0");
        assertTrue(isTopicReadingSupported());
    }

    @Test
    public void testStable100_0_Supported() throws Exception {
        setVersion("stable-100-0");
        assertTrue(isTopicReadingSupported());
    }

    @Test
    public void testStable26_5_Supported() throws Exception {
        setVersion("stable-26-5");
        assertTrue(isTopicReadingSupported());
    }

    // Not supported versions (major < 26, or major == 26 && minor < 1)

    @Test
    public void testStable26_0_NotSupported() throws Exception {
        setVersion("stable-26-0");
        assertFalse(isTopicReadingSupported());
    }

    @Test
    public void testStable25_9_NotSupported() throws Exception {
        setVersion("stable-25-9");
        assertFalse(isTopicReadingSupported());
    }

    @Test
    public void testStable24_4_NotSupported() throws Exception {
        setVersion("stable-24-4");
        assertFalse(isTopicReadingSupported());
    }

    // Non-stable versions — assume supported

    @Test
    public void testMainDev_Supported() throws Exception {
        setVersion("main-dev");
        assertTrue(isTopicReadingSupported());
    }

    @Test
    public void testTrunk_Supported() throws Exception {
        setVersion("trunk");
        assertTrue(isTopicReadingSupported());
    }

    @Test
    public void testEmptyVersion_Supported() throws Exception {
        setVersion("");
        assertTrue(isTopicReadingSupported());
    }

    // Malformed stable versions — should return false

    @Test
    public void testStableDash_NotSupported() throws Exception {
        setVersion("stable-");
        assertFalse(isTopicReadingSupported());
    }

    @Test
    public void testStableAbcDef_NotSupported() throws Exception {
        setVersion("stable-abc-def");
        assertFalse(isTopicReadingSupported());
    }

    @Test
    public void testStable26Only_NotSupported() throws Exception {
        // "stable-26" — regex requires major-minor, so no match
        setVersion("stable-26");
        assertFalse(isTopicReadingSupported());
    }
}
