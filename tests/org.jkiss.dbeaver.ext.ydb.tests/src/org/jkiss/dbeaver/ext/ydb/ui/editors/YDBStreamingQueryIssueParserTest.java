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

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for YDBStreamingQueryIssueParser.
 * Uses reflection to load from UI bundle classloader.
 */
public class YDBStreamingQueryIssueParserTest {

    private Method parseMethod;
    private Method getSeverity;
    private Method getMessage;
    private Method hasChildren;
    private Method getChildren;
    private Method getParent;

    @Before
    public void setUp() throws Exception {
        ClassLoader uiLoader = getUiBundleClassLoader();

        Class<?> parserClass = uiLoader.loadClass(
            "org.jkiss.dbeaver.ext.ydb.ui.editors.YDBStreamingQueryIssueParser");
        Class<?> nodeClass = uiLoader.loadClass(
            "org.jkiss.dbeaver.ext.ydb.ui.editors.YDBStreamingQueryIssueNode");

        parseMethod = parserClass.getMethod("parse", String.class);
        getSeverity = nodeClass.getMethod("getSeverity");
        getMessage = nodeClass.getMethod("getMessage");
        hasChildren = nodeClass.getMethod("hasChildren");
        getChildren = nodeClass.getMethod("getChildren");
        getParent = nodeClass.getMethod("getParent");
    }

    private static ClassLoader getUiBundleClassLoader() throws Exception {
        org.osgi.framework.Bundle[] bundles = org.osgi.framework.FrameworkUtil
            .getBundle(YDBStreamingQueryIssueParserTest.class)
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

    @SuppressWarnings("unchecked")
    private List<Object> parse(String json) throws Exception {
        return (List<Object>) parseMethod.invoke(null, json);
    }

    @Test
    public void testParseNull() throws Exception {
        assertTrue(parse(null).isEmpty());
    }

    @Test
    public void testParseEmpty() throws Exception {
        assertTrue(parse("").isEmpty());
    }

    @Test
    public void testParseNotJson() throws Exception {
        assertTrue(parse("not json").isEmpty());
    }

    @Test
    public void testParseSingleObject() throws Exception {
        List<Object> result = parse("{\"severity\": 1, \"message\": \"test issue\"}");
        assertEquals(1, result.size());
        assertEquals(1, getSeverity.invoke(result.get(0)));
        assertEquals("test issue", getMessage.invoke(result.get(0)));
    }

    @Test
    public void testParseArray() throws Exception {
        List<Object> result = parse("[{\"severity\": 2, \"message\": \"warn\"}, {\"severity\": 3, \"message\": \"err\"}]");
        assertEquals(2, result.size());
        assertEquals(2, getSeverity.invoke(result.get(0)));
        assertEquals("warn", getMessage.invoke(result.get(0)));
        assertEquals(3, getSeverity.invoke(result.get(1)));
        assertEquals("err", getMessage.invoke(result.get(1)));
    }

    @Test
    public void testParseNestedIssues() throws Exception {
        String json = "{\"severity\": 3, \"message\": \"parent\", \"issues\": ["
            + "{\"severity\": 1, \"message\": \"child1\"},"
            + "{\"severity\": 2, \"message\": \"child2\"}"
            + "]}";
        List<Object> result = parse(json);
        assertEquals(1, result.size());
        Object root = result.get(0);
        assertEquals("parent", getMessage.invoke(root));
        assertTrue((Boolean) hasChildren.invoke(root));
        List<?> children = (List<?>) getChildren.invoke(root);
        assertEquals(2, children.size());
        assertEquals("child1", getMessage.invoke(children.get(0)));
        assertEquals("child2", getMessage.invoke(children.get(1)));
        assertSame(root, getParent.invoke(children.get(0)));
    }

    @Test
    public void testParseMissingSeverityDefaultsToZero() throws Exception {
        List<Object> result = parse("{\"message\": \"no severity\"}");
        assertEquals(1, result.size());
        assertEquals(0, getSeverity.invoke(result.get(0)));
    }

    @Test
    public void testParseMissingMessageDefaultsToEmpty() throws Exception {
        List<Object> result = parse("{\"severity\": 4}");
        assertEquals(1, result.size());
        assertEquals("", getMessage.invoke(result.get(0)));
    }

    @Test
    public void testParseRootNodeHasNullParent() throws Exception {
        List<Object> result = parse("{\"severity\": 1, \"message\": \"root\"}");
        assertNull(getParent.invoke(result.get(0)));
    }
}
