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

import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlanNode;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlanParser;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class YDBPlanParserTest {

    @Test
    public void testParseNull() {
        List<YDBPlanNode> result = YDBPlanParser.parse(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseEmpty() {
        List<YDBPlanNode> result = YDBPlanParser.parse("");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseNotJson() {
        List<YDBPlanNode> result = YDBPlanParser.parse("not json at all");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseJsonArray() {
        // Root is array, not object
        List<YDBPlanNode> result = YDBPlanParser.parse("[1, 2, 3]");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseSimpleNodeWithNodeType() {
        String json = "{\"Node Type\": \"TableScan\"}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("TableScan", result.get(0).getNodeType());
    }

    @Test
    public void testParseFallbackToPlanNodeType() {
        String json = "{\"PlanNodeType\": \"ResultSet\"}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("ResultSet", result.get(0).getNodeType());
    }

    @Test
    public void testParseUnknownNodeType() {
        // No "Node Type" or "PlanNodeType" keys
        String json = "{\"SomeOtherKey\": \"value\"}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("Unknown", result.get(0).getNodeType());
    }

    @Test
    public void testParsePlanWrapperKey() {
        String json = "{\"Plan\": {\"Node Type\": \"HashJoin\"}}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("HashJoin", result.get(0).getNodeType());
    }

    @Test
    public void testParseTablesArray() {
        String json = "{\"Node Type\": \"Scan\", \"Tables\": [\"users\", \"orders\"]}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("users, orders", result.get(0).getNodeName());
    }

    @Test
    public void testParseSingleTable() {
        String json = "{\"Node Type\": \"Scan\", \"Table\": \"myTable\"}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("myTable", result.get(0).getNodeName());
    }

    @Test
    public void testParseOperatorsArray() {
        String json = "{\"Node Type\": \"Stage\", \"Operators\": [{\"Name\": \"Filter\"}, {\"Name\": \"Map\"}]}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("Filter, Map", result.get(0).getOperators());
    }

    @Test
    public void testParseAttributes() {
        String json = "{\"Node Type\": \"Scan\", \"CpuTime\": \"100ms\"}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        assertEquals("100ms", result.get(0).getPropertyValue(null, "CpuTime"));
    }

    @Test
    public void testParseStatsFlattened() {
        String json = "{\"Node Type\": \"Scan\", \"Stats\": {\"Rows\": \"1000\", \"Bytes\": \"4096\"}}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());
        YDBPlanNode node = result.get(0);
        assertEquals("1000", node.getPropertyValue(null, "Stats.Rows"));
        assertEquals("4096", node.getPropertyValue(null, "Stats.Bytes"));
    }

    @Test
    public void testParseNestedPlansRecursion() {
        String json = "{\"Node Type\": \"Join\", \"Plans\": ["
            + "{\"Node Type\": \"TableScan\"},"
            + "{\"Node Type\": \"IndexScan\"}"
            + "]}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertEquals(1, result.size());

        YDBPlanNode root = result.get(0);
        assertEquals("Join", root.getNodeType());
        assertEquals(2, root.getNested().size());

        List<YDBPlanNode> children = new java.util.ArrayList<>(root.getNested());
        assertEquals("TableScan", children.get(0).getNodeType());
        assertEquals("IndexScan", children.get(1).getNodeType());
        assertSame(root, children.get(0).getParent());
        assertSame(root, children.get(1).getParent());
    }

    @Test
    public void testParseRootNodeHasNullParent() {
        String json = "{\"Node Type\": \"Result\"}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        assertNull(result.get(0).getParent());
    }

    @Test
    public void testParseOperatorsSkippedFromAttributes() {
        String json = "{\"Node Type\": \"Stage\", \"Operators\": [{\"Name\": \"Op1\"}]}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        YDBPlanNode node = result.get(0);
        // "Operators" key should not be in attributes
        assertNull(node.getPropertyValue(null, "Operators"));
    }

    @Test
    public void testParsePlansSkippedFromAttributes() {
        String json = "{\"Node Type\": \"Stage\", \"Plans\": [{\"Node Type\": \"Child\"}]}";
        List<YDBPlanNode> result = YDBPlanParser.parse(json);
        YDBPlanNode node = result.get(0);
        // "Plans" key should not be in attributes
        assertNull(node.getPropertyValue(null, "Plans"));
    }
}
