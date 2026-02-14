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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class YDBStreamingQueryIssueParserTest {

    @Test
    public void testParseNull() {
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseEmpty() {
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse("");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseNotJson() {
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse("not json");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseSingleObject() {
        String json = "{\"severity\": 1, \"message\": \"test issue\"}";
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(json);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSeverity());
        assertEquals("test issue", result.get(0).getMessage());
    }

    @Test
    public void testParseArray() {
        String json = "[{\"severity\": 2, \"message\": \"warn\"}, {\"severity\": 3, \"message\": \"err\"}]";
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(json);
        assertEquals(2, result.size());
        assertEquals(2, result.get(0).getSeverity());
        assertEquals("warn", result.get(0).getMessage());
        assertEquals(3, result.get(1).getSeverity());
        assertEquals("err", result.get(1).getMessage());
    }

    @Test
    public void testParseNestedIssues() {
        String json = "{\"severity\": 3, \"message\": \"parent\", \"issues\": ["
            + "{\"severity\": 1, \"message\": \"child1\"},"
            + "{\"severity\": 2, \"message\": \"child2\"}"
            + "]}";
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(json);
        assertEquals(1, result.size());
        YDBStreamingQueryIssueNode root = result.get(0);
        assertEquals("parent", root.getMessage());
        assertTrue(root.hasChildren());
        assertEquals(2, root.getChildren().size());
        assertEquals("child1", root.getChildren().get(0).getMessage());
        assertEquals("child2", root.getChildren().get(1).getMessage());
        assertSame(root, root.getChildren().get(0).getParent());
    }

    @Test
    public void testParseMissingSeverityDefaultsToZero() {
        String json = "{\"message\": \"no severity\"}";
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(json);
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getSeverity());
    }

    @Test
    public void testParseMissingMessageDefaultsToEmpty() {
        String json = "{\"severity\": 4}";
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("", result.get(0).getMessage());
    }

    @Test
    public void testParseRootNodeHasNullParent() {
        String json = "{\"severity\": 1, \"message\": \"root\"}";
        List<YDBStreamingQueryIssueNode> result = YDBStreamingQueryIssueParser.parse(json);
        assertNull(result.get(0).getParent());
    }
}
