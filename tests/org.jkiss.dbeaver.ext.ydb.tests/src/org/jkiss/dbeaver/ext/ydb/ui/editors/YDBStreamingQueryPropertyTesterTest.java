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
package org.jkiss.dbeaver.ext.ydb.ui.editors;

import org.jkiss.dbeaver.ext.ydb.model.YDBStreamingQuery;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests for YDBStreamingQueryPropertyTester.
 * Uses OSGi bundle classloader + Unsafe to create instance since it extends
 * PropertyTester which requires Eclipse runtime.
 */
public class YDBStreamingQueryPropertyTesterTest {

    private Object tester;
    private Method testMethod;
    private DBSObject mockParent;

    @Before
    public void setUp() throws Exception {
        ClassLoader uiLoader = getUiBundleClassLoader();
        Class<?> testerClass = uiLoader.loadClass(
            "org.jkiss.dbeaver.ext.ydb.ui.editors.YDBStreamingQueryPropertyTester");

        // Allocate instance without calling constructor (avoids PropertyTester init)
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
        tester = unsafe.allocateInstance(testerClass);

        testMethod = testerClass.getMethod(
            "test", Object.class, String.class, Object[].class, Object.class);

        mockParent = new DBSObject() {
            @Override public String getName() { return "parent"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };
    }

    private static ClassLoader getUiBundleClassLoader() throws Exception {
        org.osgi.framework.Bundle[] bundles = org.osgi.framework.FrameworkUtil
            .getBundle(YDBStreamingQueryPropertyTesterTest.class)
            .getBundleContext()
            .getBundles();
        for (org.osgi.framework.Bundle bundle : bundles) {
            if ("org.jkiss.dbeaver.ext.ydb.ui".equals(bundle.getSymbolicName())) {
                if (bundle.getState() != org.osgi.framework.Bundle.ACTIVE) {
                    bundle.start();
                }
                return bundle.adapt(org.osgi.framework.wiring.BundleWiring.class).getClassLoader();
            }
        }
        throw new ClassNotFoundException("UI bundle not found in OSGi runtime");
    }

    private boolean invokeTest(Object receiver, String property, Object[] args, Object expectedValue) throws Exception {
        return (boolean) testMethod.invoke(tester, receiver, property, args, expectedValue);
    }

    private YDBStreamingQuery makeQuery(String status) {
        return new YDBStreamingQuery(mockParent, "q", "q", status, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testRunningQueryExpectTrue() throws Exception {
        assertTrue(invokeTest(makeQuery("RUNNING"), "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testRunningQueryExpectFalse() throws Exception {
        assertFalse(invokeTest(makeQuery("RUNNING"), "isRunning", new Object[0], Boolean.FALSE));
    }

    @Test
    public void testStoppedQueryExpectTrue() throws Exception {
        assertFalse(invokeTest(makeQuery("STOPPED"), "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testStoppedQueryExpectFalse() throws Exception {
        assertTrue(invokeTest(makeQuery("STOPPED"), "isRunning", new Object[0], Boolean.FALSE));
    }

    @Test
    public void testNullStatusExpectFalse() throws Exception {
        assertTrue(invokeTest(makeQuery(null), "isRunning", new Object[0], Boolean.FALSE));
    }

    @Test
    public void testNullStatusExpectTrue() throws Exception {
        assertFalse(invokeTest(makeQuery(null), "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testCaseInsensitiveRunning() throws Exception {
        assertTrue(invokeTest(makeQuery("running"), "isRunning", new Object[0], Boolean.TRUE));
        assertTrue(invokeTest(makeQuery("Running"), "isRunning", new Object[0], Boolean.TRUE));
        assertTrue(invokeTest(makeQuery("RUNNING"), "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testNonYDBStreamingQueryReceiverReturnsFalse() throws Exception {
        assertFalse(invokeTest("not a query", "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testUnknownPropertyReturnsFalse() throws Exception {
        assertFalse(invokeTest(makeQuery("RUNNING"), "unknownProperty", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testStringExpectedValue() throws Exception {
        assertTrue(invokeTest(makeQuery("RUNNING"), "isRunning", new Object[0], "true"));
    }

    @Test
    public void testStringExpectedValueFalse() throws Exception {
        assertFalse(invokeTest(makeQuery("RUNNING"), "isRunning", new Object[0], "false"));
    }
}
