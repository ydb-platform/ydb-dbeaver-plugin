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

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for YDBStreamingQueryIssueNode.
 * Uses reflection to load from UI bundle classloader since OSGi classloader
 * isolation prevents direct access in headless test runtime.
 */
public class YDBStreamingQueryIssueNodeTest {

    private static final String CLASS_NAME = "org.jkiss.dbeaver.ext.ydb.ui.editors.YDBStreamingQueryIssueNode";

    private Class<?> nodeClass;
    private Constructor<?> nodeConstructor;
    private Method getSeverityLabel;
    private Method getMessage;
    private Method getSeverity;
    private Method hasChildren;
    private Method getChildren;
    private Method getParent;
    private Method addChild;

    @Before
    public void setUp() throws Exception {
        ClassLoader uiLoader = getUiBundleClassLoader();
        nodeClass = uiLoader.loadClass(CLASS_NAME);
        nodeConstructor = nodeClass.getConstructor(nodeClass, int.class, String.class);
        getSeverityLabel = nodeClass.getMethod("getSeverityLabel");
        getMessage = nodeClass.getMethod("getMessage");
        getSeverity = nodeClass.getMethod("getSeverity");
        hasChildren = nodeClass.getMethod("hasChildren");
        getChildren = nodeClass.getMethod("getChildren");
        getParent = nodeClass.getMethod("getParent");
        addChild = nodeClass.getMethod("addChild", nodeClass);
    }

    private static ClassLoader getUiBundleClassLoader() throws Exception {
        org.osgi.framework.Bundle[] bundles = org.osgi.framework.FrameworkUtil
            .getBundle(YDBStreamingQueryIssueNodeTest.class)
            .getBundleContext()
            .getBundles();
        for (org.osgi.framework.Bundle bundle : bundles) {
            if ("org.jkiss.dbeaver.ext.ydb.ui".equals(bundle.getSymbolicName())) {
                // Ensure the bundle is started
                if (bundle.getState() != org.osgi.framework.Bundle.ACTIVE) {
                    bundle.start();
                }
                return bundle.adapt(org.osgi.framework.wiring.BundleWiring.class).getClassLoader();
            }
        }
        throw new ClassNotFoundException("UI bundle not found in OSGi runtime");
    }

    private Object createNode(Object parent, int severity, String message) throws Exception {
        return nodeConstructor.newInstance(parent, severity, message);
    }

    @Test
    public void testSeverityLabelInfo() throws Exception {
        assertEquals("INFO", getSeverityLabel.invoke(createNode(null, 1, "msg")));
    }

    @Test
    public void testSeverityLabelWarning() throws Exception {
        assertEquals("WARNING", getSeverityLabel.invoke(createNode(null, 2, "msg")));
    }

    @Test
    public void testSeverityLabelError() throws Exception {
        assertEquals("ERROR", getSeverityLabel.invoke(createNode(null, 3, "msg")));
    }

    @Test
    public void testSeverityLabelFatal() throws Exception {
        assertEquals("FATAL", getSeverityLabel.invoke(createNode(null, 4, "msg")));
    }

    @Test
    public void testSeverityLabelUnknown() throws Exception {
        assertEquals("UNKNOWN(0)", getSeverityLabel.invoke(createNode(null, 0, "msg")));
    }

    @Test
    public void testSeverityLabelUnknownNegative() throws Exception {
        assertEquals("UNKNOWN(-1)", getSeverityLabel.invoke(createNode(null, -1, "msg")));
    }

    @Test
    public void testSeverityLabelUnknownHigh() throws Exception {
        assertEquals("UNKNOWN(99)", getSeverityLabel.invoke(createNode(null, 99, "msg")));
    }

    @Test
    public void testGetMessage() throws Exception {
        assertEquals("test message", getMessage.invoke(createNode(null, 1, "test message")));
    }

    @Test
    public void testGetSeverity() throws Exception {
        assertEquals(3, getSeverity.invoke(createNode(null, 3, "msg")));
    }

    @Test
    public void testHasChildrenFalseInitially() throws Exception {
        Object node = createNode(null, 1, "msg");
        assertFalse((Boolean) hasChildren.invoke(node));
        assertTrue(((List<?>) getChildren.invoke(node)).isEmpty());
    }

    @Test
    public void testAddChildAndHasChildren() throws Exception {
        Object parent = createNode(null, 3, "parent");
        Object child = createNode(parent, 1, "child");
        addChild.invoke(parent, child);
        assertTrue((Boolean) hasChildren.invoke(parent));
        assertEquals(1, ((List<?>) getChildren.invoke(parent)).size());
        assertSame(child, ((List<?>) getChildren.invoke(parent)).get(0));
    }

    @Test
    public void testGetParentNullForRoot() throws Exception {
        assertNull(getParent.invoke(createNode(null, 1, "root")));
    }

    @Test
    public void testGetParentSetForChild() throws Exception {
        Object parent = createNode(null, 3, "parent");
        Object child = createNode(parent, 1, "child");
        assertSame(parent, getParent.invoke(child));
    }
}
