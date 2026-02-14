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
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNodeKind;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class YDBPlanNodeTest {

    private YDBPlanNode makeNode(String nodeType) {
        return new YDBPlanNode(null, nodeType, null, null, Collections.emptyMap());
    }

    // getNodeKind mapping tests

    @Test
    public void testNodeKindTableScan() {
        assertEquals(DBCPlanNodeKind.TABLE_SCAN, makeNode("TableScan").getNodeKind());
    }

    @Test
    public void testNodeKindSource() {
        assertEquals(DBCPlanNodeKind.TABLE_SCAN, makeNode("source").getNodeKind());
    }

    @Test
    public void testNodeKindReadTable() {
        assertEquals(DBCPlanNodeKind.TABLE_SCAN, makeNode("ReadTable").getNodeKind());
    }

    @Test
    public void testNodeKindIndexScan() {
        assertEquals(DBCPlanNodeKind.INDEX_SCAN, makeNode("IndexScan").getNodeKind());
    }

    @Test
    public void testNodeKindIndex() {
        assertEquals(DBCPlanNodeKind.INDEX_SCAN, makeNode("index lookup").getNodeKind());
    }

    @Test
    public void testNodeKindUpsert() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("Upsert").getNodeKind());
    }

    @Test
    public void testNodeKindWrite() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("write").getNodeKind());
    }

    @Test
    public void testNodeKindModify() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("modify").getNodeKind());
    }

    @Test
    public void testNodeKindDelete() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("delete rows").getNodeKind());
    }

    @Test
    public void testNodeKindInsert() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("insert").getNodeKind());
    }

    @Test
    public void testNodeKindUpdate() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("update").getNodeKind());
    }

    @Test
    public void testNodeKindSink() {
        assertEquals(DBCPlanNodeKind.MODIFY, makeNode("sink").getNodeKind());
    }

    @Test
    public void testNodeKindHash() {
        assertEquals(DBCPlanNodeKind.HASH, makeNode("hash").getNodeKind());
    }

    @Test
    public void testNodeKindJoin() {
        assertEquals(DBCPlanNodeKind.JOIN, makeNode("join").getNodeKind());
    }

    @Test
    public void testNodeKindSort() {
        assertEquals(DBCPlanNodeKind.SORT, makeNode("sort").getNodeKind());
    }

    @Test
    public void testNodeKindOrder() {
        assertEquals(DBCPlanNodeKind.SORT, makeNode("order by").getNodeKind());
    }

    @Test
    public void testNodeKindFilter() {
        assertEquals(DBCPlanNodeKind.FILTER, makeNode("filter").getNodeKind());
    }

    @Test
    public void testNodeKindWhere() {
        assertEquals(DBCPlanNodeKind.FILTER, makeNode("where clause").getNodeKind());
    }

    @Test
    public void testNodeKindAggregate() {
        assertEquals(DBCPlanNodeKind.AGGREGATE, makeNode("aggregate").getNodeKind());
    }

    @Test
    public void testNodeKindAgg() {
        assertEquals(DBCPlanNodeKind.AGGREGATE, makeNode("agg").getNodeKind());
    }

    @Test
    public void testNodeKindUnion() {
        assertEquals(DBCPlanNodeKind.UNION, makeNode("union").getNodeKind());
    }

    @Test
    public void testNodeKindMerge() {
        assertEquals(DBCPlanNodeKind.MERGE, makeNode("merge").getNodeKind());
    }

    @Test
    public void testNodeKindGroup() {
        assertEquals(DBCPlanNodeKind.GROUP, makeNode("group").getNodeKind());
    }

    @Test
    public void testNodeKindResult() {
        assertEquals(DBCPlanNodeKind.RESULT, makeNode("result").getNodeKind());
    }

    @Test
    public void testNodeKindMaterialize() {
        assertEquals(DBCPlanNodeKind.MATERIALIZE, makeNode("materialize").getNodeKind());
    }

    @Test
    public void testNodeKindDefault() {
        assertEquals(DBCPlanNodeKind.DEFAULT, makeNode("SomethingElse").getNodeKind());
    }

    // Constructor and getter tests

    @Test
    public void testGetNodeType() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", "myTable", "Filter, Map", Collections.emptyMap());
        assertEquals("Scan", node.getNodeType());
    }

    @Test
    public void testGetNodeName() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", "myTable", null, Collections.emptyMap());
        assertEquals("myTable", node.getNodeName());
    }

    @Test
    public void testGetNodeNameNull() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, Collections.emptyMap());
        assertNull(node.getNodeName());
    }

    @Test
    public void testGetOperators() {
        YDBPlanNode node = new YDBPlanNode(null, "Stage", null, "Filter, Map", Collections.emptyMap());
        assertEquals("Filter, Map", node.getOperators());
    }

    @Test
    public void testGetParentNull() {
        YDBPlanNode node = new YDBPlanNode(null, "Root", null, null, Collections.emptyMap());
        assertNull(node.getParent());
    }

    @Test
    public void testGetParent() {
        YDBPlanNode parent = new YDBPlanNode(null, "Root", null, null, Collections.emptyMap());
        YDBPlanNode child = new YDBPlanNode(parent, "Child", null, null, Collections.emptyMap());
        assertSame(parent, child.getParent());
    }

    // addChild / getNested tests

    @Test
    public void testAddChildAndGetNested() {
        YDBPlanNode parent = new YDBPlanNode(null, "Root", null, null, Collections.emptyMap());
        YDBPlanNode child1 = new YDBPlanNode(parent, "Child1", null, null, Collections.emptyMap());
        YDBPlanNode child2 = new YDBPlanNode(parent, "Child2", null, null, Collections.emptyMap());
        parent.addChild(child1);
        parent.addChild(child2);

        assertEquals(2, parent.getNested().size());
    }

    @Test
    public void testGetNestedEmpty() {
        YDBPlanNode node = new YDBPlanNode(null, "Leaf", null, null, Collections.emptyMap());
        assertTrue(node.getNested().isEmpty());
    }

    // Property source tests

    @Test
    public void testGetProperties() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("CpuTime", "100ms");
        attrs.put("Rows", "1000");
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, attrs);

        DBPPropertyDescriptor[] props = node.getProperties();
        assertEquals(2, props.length);
        assertEquals("CpuTime", props[0].getId());
        assertEquals("Rows", props[1].getId());
    }

    @Test
    public void testGetPropertiesEmpty() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, Collections.emptyMap());
        assertEquals(0, node.getProperties().length);
    }

    @Test
    public void testGetPropertyValue() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("key", "value");
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, attrs);
        assertEquals("value", node.getPropertyValue(null, "key"));
    }

    @Test
    public void testGetPropertyValueMissing() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, Collections.emptyMap());
        assertNull(node.getPropertyValue(null, "nonexistent"));
    }

    @Test
    public void testIsPropertySet() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("key", "value");
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, attrs);
        assertTrue(node.isPropertySet("key"));
        assertFalse(node.isPropertySet("other"));
    }

    @Test
    public void testIsPropertyResettable() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, Collections.emptyMap());
        assertFalse(node.isPropertyResettable("any"));
    }

    @Test
    public void testGetEditableValue() {
        YDBPlanNode node = new YDBPlanNode(null, "Scan", null, null, Collections.emptyMap());
        assertSame(node, node.getEditableValue());
    }
}
