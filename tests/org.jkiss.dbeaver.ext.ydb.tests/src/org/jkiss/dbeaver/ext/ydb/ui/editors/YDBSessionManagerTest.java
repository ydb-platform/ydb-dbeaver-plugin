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

import org.jkiss.dbeaver.ext.ydb.model.session.YDBSessionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class YDBSessionManagerTest {

    private YDBSessionManager manager;

    @Before
    public void setUp() {
        // YDBSessionManager constructor takes YDBDataSource, but the methods we test
        // don't require it. Pass null and test the pure-logic methods.
        manager = new YDBSessionManager(null);
    }

    @Test
    public void testGenerateSessionReadQuery() {
        String sql = manager.generateSessionReadQuery(Map.of());
        assertEquals("SELECT * FROM `.sys/query_sessions` ORDER BY SessionStartAt ASC", sql);
    }

    @Test
    public void testGenerateSessionReadQueryIgnoresOptions() {
        String sql = manager.generateSessionReadQuery(Map.of("hideIdle", true));
        assertEquals("SELECT * FROM `.sys/query_sessions` ORDER BY SessionStartAt ASC", sql);
    }

    @Test
    public void testCanGenerateSessionReadQuery() {
        assertTrue(manager.canGenerateSessionReadQuery());
    }

    @Test
    public void testGetTerminateOptionsEmpty() {
        Map<String, Object> options = manager.getTerminateOptions();
        assertNotNull(options);
        assertTrue(options.isEmpty());
    }
}
