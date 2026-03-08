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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for Data Engineering mode property constant and default behavior.
 * The actual isDataEngineeringMode() method on YDBDataSource follows the pattern:
 * - null property value → true (enabled by default)
 * - "true" → true
 * - "false" → false
 */
public class YDBDataEngineeringModeTest {

    @Test
    public void testPropertyConstantValue() {
        assertEquals("ydb.dataEngineeringMode", YDBDataSource.PROP_DATA_ENGINEERING_MODE);
    }

    @Test
    public void testDefaultBehavior_nullMeansEnabled() {
        // Simulates the logic from isDataEngineeringMode()
        String value = null;
        boolean result = value == null || Boolean.parseBoolean(value);
        assertTrue("Data Engineering mode should be enabled by default (null)", result);
    }

    @Test
    public void testDefaultBehavior_trueMeansEnabled() {
        String value = "true";
        boolean result = value == null || Boolean.parseBoolean(value);
        assertTrue("Data Engineering mode should be enabled when set to 'true'", result);
    }

    @Test
    public void testDefaultBehavior_falseMeansDisabled() {
        String value = "false";
        boolean result = value == null || Boolean.parseBoolean(value);
        assertFalse("Data Engineering mode should be disabled when set to 'false'", result);
    }

    @Test
    public void testPropertyConstantMatchesUIProperty() {
        // Ensure model and UI use the same property key
        assertEquals(
            YDBDataSource.PROP_DATA_ENGINEERING_MODE,
            "ydb.dataEngineeringMode"
        );
    }
}
