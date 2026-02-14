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

import static org.junit.Assert.*;

public class YDBStreamingQueryIssueNodeTest {

    @Test
    public void testSeverityLabelInfo() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 1, "msg");
        assertEquals("INFO", node.getSeverityLabel());
    }

    @Test
    public void testSeverityLabelWarning() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 2, "msg");
        assertEquals("WARNING", node.getSeverityLabel());
    }

    @Test
    public void testSeverityLabelError() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 3, "msg");
        assertEquals("ERROR", node.getSeverityLabel());
    }

    @Test
    public void testSeverityLabelFatal() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 4, "msg");
        assertEquals("FATAL", node.getSeverityLabel());
    }

    @Test
    public void testSeverityLabelUnknown() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 0, "msg");
        assertEquals("UNKNOWN(0)", node.getSeverityLabel());
    }

    @Test
    public void testSeverityLabelUnknownNegative() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, -1, "msg");
        assertEquals("UNKNOWN(-1)", node.getSeverityLabel());
    }

    @Test
    public void testSeverityLabelUnknownHigh() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 99, "msg");
        assertEquals("UNKNOWN(99)", node.getSeverityLabel());
    }

    @Test
    public void testGetMessage() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 1, "test message");
        assertEquals("test message", node.getMessage());
    }

    @Test
    public void testGetSeverity() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 3, "msg");
        assertEquals(3, node.getSeverity());
    }

    @Test
    public void testHasChildrenFalseInitially() {
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(null, 1, "msg");
        assertFalse(node.hasChildren());
        assertTrue(node.getChildren().isEmpty());
    }

    @Test
    public void testAddChildAndHasChildren() {
        YDBStreamingQueryIssueNode parent = new YDBStreamingQueryIssueNode(null, 3, "parent");
        YDBStreamingQueryIssueNode child = new YDBStreamingQueryIssueNode(parent, 1, "child");
        parent.addChild(child);

        assertTrue(parent.hasChildren());
        assertEquals(1, parent.getChildren().size());
        assertSame(child, parent.getChildren().get(0));
    }

    @Test
    public void testGetParentNullForRoot() {
        YDBStreamingQueryIssueNode root = new YDBStreamingQueryIssueNode(null, 1, "root");
        assertNull(root.getParent());
    }

    @Test
    public void testGetParentSetForChild() {
        YDBStreamingQueryIssueNode parent = new YDBStreamingQueryIssueNode(null, 3, "parent");
        YDBStreamingQueryIssueNode child = new YDBStreamingQueryIssueNode(parent, 1, "child");
        assertSame(parent, child.getParent());
    }
}
