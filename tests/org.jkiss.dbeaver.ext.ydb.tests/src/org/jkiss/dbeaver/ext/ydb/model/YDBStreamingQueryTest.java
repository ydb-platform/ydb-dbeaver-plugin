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

import org.jkiss.dbeaver.model.DBIcon;
import org.jkiss.dbeaver.model.DBIconComposite;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPImage;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class YDBStreamingQueryTest {

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

    private YDBStreamingQuery createQuery(String name, String fullPath, String status) {
        return new YDBStreamingQuery(
            mockParent, name, fullPath, status,
            "issues-json", "plan-json", "ast-json",
            "SELECT 1", "run-id", "pool-1",
            "3", "2026-01-01", "2026-02-01"
        );
    }

    @Test
    public void testGetName() {
        YDBStreamingQuery q = createQuery("myQuery", "folder/myQuery", "RUNNING");
        assertEquals("myQuery", q.getName());
    }

    @Test
    public void testGetFullPath() {
        YDBStreamingQuery q = createQuery("myQuery", "folder/myQuery", "RUNNING");
        assertEquals("folder/myQuery", q.getFullPath());
    }

    @Test
    public void testGetStatus() {
        YDBStreamingQuery q = createQuery("q", "q", "STOPPED");
        assertEquals("STOPPED", q.getStatus());
    }

    @Test
    public void testGetStatusNull() {
        YDBStreamingQuery q = new YDBStreamingQuery(
            mockParent, "q", "q", null, null, null, null, null, null, null, null, null, null
        );
        assertNull(q.getStatus());
    }

    @Test
    public void testGetQueryText() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("SELECT 1", q.getQueryText());
    }

    @Test
    public void testGetResourcePool() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("pool-1", q.getResourcePool());
    }

    @Test
    public void testGetRun() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("run-id", q.getRun());
    }

    @Test
    public void testGetRetryCount() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("3", q.getRetryCount());
    }

    @Test
    public void testGetLastFailAt() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("2026-01-01", q.getLastFailAt());
    }

    @Test
    public void testGetSuspendedUntil() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("2026-02-01", q.getSuspendedUntil());
    }

    @Test
    public void testGetIssues() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("issues-json", q.getIssues());
    }

    @Test
    public void testGetPlan() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("plan-json", q.getPlan());
    }

    @Test
    public void testGetAst() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertEquals("ast-json", q.getAst());
    }

    @Test
    public void testGetDescription() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertNull(q.getDescription());
    }

    @Test
    public void testIsPersisted() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertTrue(q.isPersisted());
    }

    @Test
    public void testGetParentObject() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertSame(mockParent, q.getParentObject());
    }

    // getObjectToolTip tests

    @Test
    public void testGetObjectToolTipReturnsNullWhenRunning() {
        YDBStreamingQuery q = createQuery("q", "q", "RUNNING");
        assertNull(q.getObjectToolTip());
    }

    @Test
    public void testGetObjectToolTipReturnsNullWhenRunningCaseInsensitive() {
        YDBStreamingQuery q = createQuery("q", "q", "running");
        assertNull(q.getObjectToolTip());
    }

    @Test
    public void testGetObjectToolTipReturnsStatusWhenStopped() {
        YDBStreamingQuery q = createQuery("q", "q", "STOPPED");
        assertEquals("STOPPED", q.getObjectToolTip());
    }

    @Test
    public void testGetObjectToolTipReturnsStatusWhenError() {
        YDBStreamingQuery q = createQuery("q", "q", "ERROR");
        assertEquals("ERROR", q.getObjectToolTip());
    }

    @Test
    public void testGetObjectToolTipReturnsNullWhenStatusIsNull() {
        YDBStreamingQuery q = new YDBStreamingQuery(
            mockParent, "q", "q", null, null, null, null, null, null, null, null, null, null
        );
        assertNull(q.getObjectToolTip());
    }

    // hasHierarchicalPath tests

    @Test
    public void testHasHierarchicalPathTrue() {
        YDBStreamingQuery q = createQuery("q", "folder/subfolder/q", "RUNNING");
        assertTrue(q.hasHierarchicalPath());
    }

    @Test
    public void testHasHierarchicalPathFalse() {
        YDBStreamingQuery q = createQuery("q", "simple_query", "RUNNING");
        assertFalse(q.hasHierarchicalPath());
    }

    // Copy constructor tests

    @Test
    public void testCopyConstructorPreservesFields() {
        YDBStreamingQuery original = createQuery("originalName", "folder/originalName", "RUNNING");
        DBSObject newParent = new DBSObject() {
            @Override public String getName() { return "newParent"; }
            @Override public String getDescription() { return null; }
            @Override public boolean isPersisted() { return true; }
            @Override public DBSObject getParentObject() { return null; }
            @Override public DBPDataSource getDataSource() { return null; }
        };

        YDBStreamingQuery copy = new YDBStreamingQuery(newParent, "newName", original);

        assertEquals("newName", copy.getName());
        assertSame(newParent, copy.getParentObject());
        assertEquals("folder/originalName", copy.getFullPath());
        assertEquals("RUNNING", copy.getStatus());
        assertEquals("issues-json", copy.getIssues());
        assertEquals("plan-json", copy.getPlan());
        assertEquals("ast-json", copy.getAst());
        assertEquals("SELECT 1", copy.getQueryText());
        assertEquals("run-id", copy.getRun());
        assertEquals("pool-1", copy.getResourcePool());
        assertEquals("3", copy.getRetryCount());
        assertEquals("2026-01-01", copy.getLastFailAt());
        assertEquals("2026-02-01", copy.getSuspendedUntil());
    }

    // Error state / icon tests

    @Test
    public void testIsInErrorStateTrueForError() {
        assertTrue(createQuery("q", "q", "ERROR").isInErrorState());
    }

    @Test
    public void testIsInErrorStateTrueForLowerCaseFailed() {
        assertTrue(createQuery("q", "q", "failed").isInErrorState());
    }

    @Test
    public void testIsInErrorStateTrueForMixedCaseFailed() {
        assertTrue(createQuery("q", "q", "Failed").isInErrorState());
    }

    @Test
    public void testIsInErrorStateTrueForSuspended() {
        assertTrue(createQuery("q", "q", "SUSPENDED").isInErrorState());
    }

    @Test
    public void testIsInErrorStateTrueForMixedCaseSuspended() {
        assertTrue(createQuery("q", "q", "Suspended").isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseForRunning() {
        assertFalse(createQuery("q", "q", "RUNNING").isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseForStopped() {
        assertFalse(createQuery("q", "q", "STOPPED").isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseForNullStatus() {
        YDBStreamingQuery q = new YDBStreamingQuery(
            mockParent, "q", "q", null, null, null, null, null, null, null, null, null, null
        );
        assertFalse(q.isInErrorState());
    }

    @Test
    public void testGetObjectImageReturnsNullWhenHealthy() {
        assertNull(createQuery("q", "q", "RUNNING").getObjectImage());
    }

    @Test
    public void testGetObjectImageReturnsCompositeWithErrorOverlayWhenFailed() {
        DBPImage img = createQuery("q", "q", "ERROR").getObjectImage();
        assertNotNull(img);
        assertTrue(img instanceof DBIconComposite);
        DBIconComposite composite = (DBIconComposite) img;
        assertSame(YDBStreamingQuery.ICON, composite.getMain());
        assertSame(DBIcon.OVER_ERROR, composite.getBottomRight());
    }
}
