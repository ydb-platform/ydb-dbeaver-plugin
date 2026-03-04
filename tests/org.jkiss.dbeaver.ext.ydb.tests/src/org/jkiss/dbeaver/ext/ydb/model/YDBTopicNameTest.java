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
 * Tests for YDBTopic name extraction from path.
 */
public class YDBTopicNameTest {

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

    @Test
    public void testNameFromPath() {
        YDBTopic topic = new YDBTopic(mockParent, "folder/myTopic");
        assertEquals("myTopic", topic.getName());
    }

    @Test
    public void testNameSimple() {
        YDBTopic topic = new YDBTopic(mockParent, "simpleTopic");
        assertEquals("simpleTopic", topic.getName());
    }

    @Test
    public void testFullPath() {
        YDBTopic topic = new YDBTopic(mockParent, "folder/myTopic");
        assertEquals("folder/myTopic", topic.getFullPath());
    }

    @Test
    public void testExplicitNameConstructor() {
        YDBTopic topic = new YDBTopic(mockParent, "customName", "folder/original");
        assertEquals("customName", topic.getName());
        assertEquals("folder/original", topic.getFullPath());
    }

    @Test
    public void testIsPersisted() {
        YDBTopic topic = new YDBTopic(mockParent, "t");
        assertTrue(topic.isPersisted());
    }

    @Test
    public void testToString() {
        YDBTopic topic = new YDBTopic(mockParent, "folder/myTopic");
        assertEquals("myTopic", topic.toString());
    }

    @Test
    public void testGetParentObject() {
        YDBTopic topic = new YDBTopic(mockParent, "t");
        assertSame(mockParent, topic.getParentObject());
    }

    @Test
    public void testDeepPath() {
        YDBTopic topic = new YDBTopic(mockParent, "a/b/c/deepTopic");
        assertEquals("deepTopic", topic.getName());
        assertEquals("a/b/c/deepTopic", topic.getFullPath());
    }
}
