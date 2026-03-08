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

import org.jkiss.dbeaver.ext.ydb.model.data.YDBChartDataConverter;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Date;

import static org.junit.Assert.*;

public class YDBChartDataConverterTest {

    // --- getCellValue ---

    @Test
    public void testGetCellValue_normal() {
        Object[] values = {"a", 42, 3.14};
        assertEquals("a", YDBChartDataConverter.getCellValue(values, 0));
        assertEquals(42, YDBChartDataConverter.getCellValue(values, 1));
        assertEquals(3.14, YDBChartDataConverter.getCellValue(values, 2));
    }

    @Test
    public void testGetCellValue_nullArray() {
        assertNull(YDBChartDataConverter.getCellValue(null, 0));
    }

    @Test
    public void testGetCellValue_outOfBounds() {
        Object[] values = {"x"};
        assertNull(YDBChartDataConverter.getCellValue(values, 1));
        assertNull(YDBChartDataConverter.getCellValue(values, -1));
    }

    @Test
    public void testGetCellValue_nullElement() {
        Object[] values = {null, "y"};
        assertNull(YDBChartDataConverter.getCellValue(values, 0));
        assertEquals("y", YDBChartDataConverter.getCellValue(values, 1));
    }

    // --- formatValue ---

    @Test
    public void testFormatValue_null() {
        assertEquals("", YDBChartDataConverter.formatValue(null));
    }

    @Test
    public void testFormatValue_string() {
        assertEquals("hello", YDBChartDataConverter.formatValue("hello"));
    }

    @Test
    public void testFormatValue_integer() {
        assertEquals("42", YDBChartDataConverter.formatValue(42));
    }

    // --- toDouble ---

    @Test
    public void testToDouble_null() {
        assertTrue(Double.isNaN(YDBChartDataConverter.toDouble(null)));
    }

    @Test
    public void testToDouble_integer() {
        assertEquals(42.0, YDBChartDataConverter.toDouble(42), 1e-9);
    }

    @Test
    public void testToDouble_long() {
        assertEquals(1000000.0, YDBChartDataConverter.toDouble(1000000L), 1e-9);
    }

    @Test
    public void testToDouble_double() {
        assertEquals(3.14, YDBChartDataConverter.toDouble(3.14), 1e-9);
    }

    @Test
    public void testToDouble_stringNumeric() {
        assertEquals(123.456, YDBChartDataConverter.toDouble("123.456"), 1e-9);
    }

    @Test
    public void testToDouble_stringInvalid() {
        assertTrue(Double.isNaN(YDBChartDataConverter.toDouble("not_a_number")));
    }

    @Test
    public void testToDouble_zero() {
        assertEquals(0.0, YDBChartDataConverter.toDouble(0), 1e-9);
    }

    @Test
    public void testToDouble_negative() {
        assertEquals(-5.5, YDBChartDataConverter.toDouble(-5.5), 1e-9);
    }

    // --- toDate ---

    @Test
    public void testToDate_null() {
        assertNull(YDBChartDataConverter.toDate(null));
    }

    @Test
    public void testToDate_date() {
        Date d = new Date(1000000L);
        assertSame(d, YDBChartDataConverter.toDate(d));
    }

    @Test
    public void testToDate_timestamp() {
        Timestamp ts = new Timestamp(1234567890L);
        Date result = YDBChartDataConverter.toDate(ts);
        assertNotNull(result);
        assertEquals(1234567890L, result.getTime());
    }

    @Test
    public void testToDate_otherType() {
        assertNull(YDBChartDataConverter.toDate("2024-01-01"));
        assertNull(YDBChartDataConverter.toDate(42));
    }
}
