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
package org.jkiss.dbeaver.ext.ydb.model.data;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Pure data conversion utilities for chart building.
 * Placed in non-UI module to allow unit testing without SWT dependencies.
 */
public class YDBChartDataConverter {

    private YDBChartDataConverter() {}

    public static Object getCellValue(Object[] values, int ordinalPosition) {
        if (values == null || ordinalPosition < 0 || ordinalPosition >= values.length) {
            return null;
        }
        return values[ordinalPosition];
    }

    public static String formatValue(Object val) {
        return val == null ? "" : val.toString();
    }

    public static double toDouble(Object val) {
        if (val == null) {
            return Double.NaN;
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        try {
            return Double.parseDouble(val.toString());
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    public static Date toDate(Object val) {
        if (val instanceof Date) {
            return (Date) val;
        }
        if (val instanceof Timestamp) {
            return new Date(((Timestamp) val).getTime());
        }
        return null;
    }
}
