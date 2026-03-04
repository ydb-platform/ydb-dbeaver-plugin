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
 * Tests for YDBResourcePool display formatting of property values.
 */
public class YDBResourcePoolDisplayTest {

    private YDBResourcePool createPool(int concurrentQueryLimit, int queueSize,
                                        double databaseLoadCpuThreshold, double resourceWeight,
                                        double totalCpu, double queryCpu, double queryMemory) {
        return new YDBResourcePool(null, "testPool",
            concurrentQueryLimit, queueSize, databaseLoadCpuThreshold,
            resourceWeight, totalCpu, queryCpu, queryMemory);
    }

    // concurrentQueryLimit

    @Test
    public void testConcurrentQueryLimitUnlimited() {
        assertEquals("unlimited", createPool(-1, 0, 0, 0, 0, 0, 0).getConcurrentQueryLimit());
    }

    @Test
    public void testConcurrentQueryLimitZero() {
        assertEquals("0", createPool(0, 0, 0, 0, 0, 0, 0).getConcurrentQueryLimit());
    }

    @Test
    public void testConcurrentQueryLimitPositive() {
        assertEquals("100", createPool(100, 0, 0, 0, 0, 0, 0).getConcurrentQueryLimit());
    }

    // queueSize

    @Test
    public void testQueueSizeUnlimited() {
        assertEquals("unlimited", createPool(0, -1, 0, 0, 0, 0, 0).getQueueSize());
    }

    @Test
    public void testQueueSizeZero() {
        assertEquals("0", createPool(0, 0, 0, 0, 0, 0, 0).getQueueSize());
    }

    @Test
    public void testQueueSizePositive() {
        assertEquals("50", createPool(0, 50, 0, 0, 0, 0, 0).getQueueSize());
    }

    // databaseLoadCpuThreshold

    @Test
    public void testDatabaseLoadCpuThresholdUnlimited() {
        assertEquals("unlimited", createPool(0, 0, -1.0, 0, 0, 0, 0).getDatabaseLoadCpuThreshold());
    }

    @Test
    public void testDatabaseLoadCpuThresholdZero() {
        assertEquals("0.0", createPool(0, 0, 0.0, 0, 0, 0, 0).getDatabaseLoadCpuThreshold());
    }

    @Test
    public void testDatabaseLoadCpuThresholdPositive() {
        assertEquals("80.5", createPool(0, 0, 80.5, 0, 0, 0, 0).getDatabaseLoadCpuThreshold());
    }

    // resourceWeight

    @Test
    public void testResourceWeightUnlimited() {
        assertEquals("unlimited", createPool(0, 0, 0, -1.0, 0, 0, 0).getResourceWeight());
    }

    @Test
    public void testResourceWeightPositive() {
        assertEquals("5.0", createPool(0, 0, 0, 5.0, 0, 0, 0).getResourceWeight());
    }

    // totalCpuLimitPercentPerNode

    @Test
    public void testTotalCpuUnlimited() {
        assertEquals("unlimited", createPool(0, 0, 0, 0, -1.0, 0, 0).getTotalCpuLimitPercentPerNode());
    }

    @Test
    public void testTotalCpuPositive() {
        assertEquals("75.0", createPool(0, 0, 0, 0, 75.0, 0, 0).getTotalCpuLimitPercentPerNode());
    }

    // queryCpuLimitPercentPerNode

    @Test
    public void testQueryCpuUnlimited() {
        assertEquals("unlimited", createPool(0, 0, 0, 0, 0, -1.0, 0).getQueryCpuLimitPercentPerNode());
    }

    @Test
    public void testQueryCpuPositive() {
        assertEquals("25.0", createPool(0, 0, 0, 0, 0, 25.0, 0).getQueryCpuLimitPercentPerNode());
    }

    // queryMemoryLimitPercentPerNode

    @Test
    public void testQueryMemoryUnlimited() {
        assertEquals("unlimited", createPool(0, 0, 0, 0, 0, 0, -1.0).getQueryMemoryLimitPercentPerNode());
    }

    @Test
    public void testQueryMemoryPositive() {
        assertEquals("50.0", createPool(0, 0, 0, 0, 0, 0, 50.0).getQueryMemoryLimitPercentPerNode());
    }

    // Basic properties

    @Test
    public void testGetName() {
        assertEquals("testPool", createPool(0, 0, 0, 0, 0, 0, 0).getName());
    }

    @Test
    public void testGetDescription() {
        assertNull(createPool(0, 0, 0, 0, 0, 0, 0).getDescription());
    }

    @Test
    public void testIsPersisted() {
        assertTrue(createPool(0, 0, 0, 0, 0, 0, 0).isPersisted());
    }
}
