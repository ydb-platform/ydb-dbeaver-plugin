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

import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Tests for YDBTransfer error-state icon overlay.
 */
public class YDBTransferErrorIconTest {

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

    private void setState(YDBTransfer transfer, String state) throws Exception {
        Field field = YDBTransfer.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(transfer, state);
    }

    @Test
    public void testIsInErrorStateTrueForError() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "Error");
        assertTrue(t.isInErrorState());
    }

    @Test
    public void testIsInErrorStateCaseInsensitive() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "ERROR");
        assertTrue(t.isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseForRunning() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "Running");
        assertFalse(t.isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseForPaused() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "Paused");
        assertFalse(t.isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseForDone() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "Done");
        assertFalse(t.isInErrorState());
    }

    @Test
    public void testIsInErrorStateFalseWhenStateNotLoaded() {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        assertFalse(t.isInErrorState());
    }

    @Test
    public void testGetObjectImageNullWhenHealthy() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "Running");
        assertNull(t.getObjectImage());
    }

    @Test
    public void testGetObjectImageNullWhenStateNotLoaded() {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        assertNull(t.getObjectImage());
    }

    @Test
    public void testGetObjectImageHasErrorOverlayWhenErrorState() throws Exception {
        YDBTransfer t = new YDBTransfer(mockParent, "t");
        setState(t, "Error");
        DBPImage img = t.getObjectImage();
        assertNotNull(img);
        assertTrue(img instanceof DBIconComposite);
        DBIconComposite composite = (DBIconComposite) img;
        assertSame(YDBTransfer.ICON, composite.getMain());
        assertSame(DBIcon.OVER_ERROR, composite.getBottomRight());
    }
}
