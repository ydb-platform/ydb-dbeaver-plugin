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

import static org.junit.Assert.*;

/**
 * Tests defensive programming: all model constructors handle null/empty fields gracefully.
 * Critical for compatibility with different YDB versions where proto may return default values.
 */
public class YDBVersionCompatibilityTest {

    private DBSObject mockParent;

    @Before
    public void setUp() {
        mockParent = new DBSObject() {
            @Override public String getName() { return "parent"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };
    }

    // YDBStreamingQuery with all null optional fields

    @Test
    public void testStreamingQueryAllNulls() {
        YDBStreamingQuery q = new YDBStreamingQuery(
            mockParent, "q", "q",
            null, null, null, null, null, null, null, null, null, null
        );
        assertEquals("q", q.getName());
        assertNull(q.getStatus());
        assertNull(q.getQueryText());
        assertNull(q.getIssues());
        assertNull(q.getPlan());
        assertNull(q.getAst());
        assertNull(q.getRun());
        assertNull(q.getResourcePool());
        assertNull(q.getRetryCount());
        assertNull(q.getLastFailAt());
        assertNull(q.getSuspendedUntil());
        assertNull(q.getDescription());
        assertTrue(q.isPersisted());
    }

    // YDBTransfer with null-safe operations

    @Test
    public void testTransferBasicProperties() {
        YDBTransfer t = new YDBTransfer(mockParent, "transfer1");
        assertEquals("transfer1", t.getName());
        assertEquals("transfer1", t.getFullPath());
        assertNull(t.getDescription());
        assertNull(t.getState());
        assertNull(t.getOwner());
        assertTrue(t.isPersisted());
    }

    @Test
    public void testTransferEmptyPath() {
        // Empty path — name should be empty
        YDBTransfer t = new YDBTransfer(mockParent, "");
        assertEquals("", t.getName());
        assertEquals("", t.getFullPath());
    }

    // YDBResourcePool with boundary values

    @Test
    public void testResourcePoolUnlimitedValues() {
        YDBResourcePool pool = new YDBResourcePool(
            null, "default", -1, -1, -1.0, -1.0, -1.0, -1.0, -1.0
        );
        assertEquals("default", pool.getName());
        assertEquals("unlimited", pool.getConcurrentQueryLimit());
        assertEquals("unlimited", pool.getQueueSize());
        assertEquals("unlimited", pool.getDatabaseLoadCpuThreshold());
        assertEquals("unlimited", pool.getResourceWeight());
        assertEquals("unlimited", pool.getTotalCpuLimitPercentPerNode());
        assertEquals("unlimited", pool.getQueryCpuLimitPercentPerNode());
        assertEquals("unlimited", pool.getQueryMemoryLimitPercentPerNode());
        assertNull(pool.getDescription());
        assertTrue(pool.isPersisted());
    }

    @Test
    public void testResourcePoolZeroValues() {
        YDBResourcePool pool = new YDBResourcePool(
            null, "pool", 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0
        );
        assertEquals("0", pool.getConcurrentQueryLimit());
        assertEquals("0", pool.getQueueSize());
        assertEquals("0.0", pool.getDatabaseLoadCpuThreshold());
        assertEquals("0.0", pool.getResourceWeight());
    }

    // YDBTopic with various paths

    @Test
    public void testTopicWithPath() {
        YDBTopic topic = new YDBTopic(mockParent, "folder/myTopic");
        assertEquals("myTopic", topic.getName());
        assertEquals("folder/myTopic", topic.getFullPath());
        assertTrue(topic.isPersisted());
        assertNull(topic.getDescription());
    }

    @Test
    public void testTopicSimpleName() {
        YDBTopic topic = new YDBTopic(mockParent, "simpleTopic");
        assertEquals("simpleTopic", topic.getName());
    }

    // YDBView with various paths

    @Test
    public void testViewWithPath() {
        YDBView view = new YDBView(mockParent, "folder/myView");
        assertEquals("myView", view.getName());
        assertEquals("folder/myView", view.getFullPath());
        assertTrue(view.isPersisted());
        assertNull(view.getDescription());
    }

    @Test
    public void testViewSimpleName() {
        YDBView view = new YDBView(mockParent, "simpleView");
        assertEquals("simpleView", view.getName());
    }

    // YDBExternalDataSource

    @Test
    public void testExternalDataSourceWithPath() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "folder/myEds");
        assertEquals("myEds", eds.getName());
        assertEquals("folder/myEds", eds.getFullPath());
        assertTrue(eds.isPersisted());
        assertNull(eds.getDescription());
    }

    @Test
    public void testExternalDataSourceSimpleName() {
        YDBExternalDataSource eds = new YDBExternalDataSource(mockParent, "simpleEds");
        assertEquals("simpleEds", eds.getName());
    }
}
