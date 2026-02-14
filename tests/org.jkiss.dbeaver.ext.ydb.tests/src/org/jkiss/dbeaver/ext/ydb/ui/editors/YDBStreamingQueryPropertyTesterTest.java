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

import static org.junit.Assert.*;

public class YDBStreamingQueryPropertyTesterTest {

    private YDBStreamingQueryPropertyTester tester;
    private DBSObject mockParent;

    @Before
    public void setUp() {
        tester = new YDBStreamingQueryPropertyTester();
        mockParent = new DBSObject() {
            @Override public String getName() { return "parent"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };
    }

    private YDBStreamingQuery makeQuery(String status) {
        return new YDBStreamingQuery(mockParent, "q", "q", status, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testRunningQueryExpectTrue() {
        YDBStreamingQuery q = makeQuery("RUNNING");
        assertTrue(tester.test(q, "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testRunningQueryExpectFalse() {
        YDBStreamingQuery q = makeQuery("RUNNING");
        assertFalse(tester.test(q, "isRunning", new Object[0], Boolean.FALSE));
    }

    @Test
    public void testStoppedQueryExpectTrue() {
        YDBStreamingQuery q = makeQuery("STOPPED");
        assertFalse(tester.test(q, "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testStoppedQueryExpectFalse() {
        YDBStreamingQuery q = makeQuery("STOPPED");
        assertTrue(tester.test(q, "isRunning", new Object[0], Boolean.FALSE));
    }

    @Test
    public void testNullStatusExpectFalse() {
        YDBStreamingQuery q = makeQuery(null);
        assertTrue(tester.test(q, "isRunning", new Object[0], Boolean.FALSE));
    }

    @Test
    public void testNullStatusExpectTrue() {
        YDBStreamingQuery q = makeQuery(null);
        assertFalse(tester.test(q, "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testCaseInsensitiveRunning() {
        assertTrue(tester.test(makeQuery("running"), "isRunning", new Object[0], Boolean.TRUE));
        assertTrue(tester.test(makeQuery("Running"), "isRunning", new Object[0], Boolean.TRUE));
        assertTrue(tester.test(makeQuery("RUNNING"), "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testNonYDBStreamingQueryReceiverReturnsFalse() {
        assertFalse(tester.test("not a query", "isRunning", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testUnknownPropertyReturnsFalse() {
        YDBStreamingQuery q = makeQuery("RUNNING");
        assertFalse(tester.test(q, "unknownProperty", new Object[0], Boolean.TRUE));
    }

    @Test
    public void testStringExpectedValue() {
        YDBStreamingQuery q = makeQuery("RUNNING");
        // expectedValue as String "true" should be parsed via Boolean.parseBoolean
        assertTrue(tester.test(q, "isRunning", new Object[0], "true"));
    }

    @Test
    public void testStringExpectedValueFalse() {
        YDBStreamingQuery q = makeQuery("RUNNING");
        assertFalse(tester.test(q, "isRunning", new Object[0], "false"));
    }
}
