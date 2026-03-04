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
package org.jkiss.dbeaver.ext.ydb.ui.dashboard;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * SWT Canvas that renders a simple time-series area chart for YDB dashboard metrics.
 */
public class YDBDashboardChartComposite extends Canvas {

    private static final int MAX_DATA_POINTS = 60;
    private static final int MARGIN_LEFT = 60;
    private static final int MARGIN_RIGHT = 10;
    private static final int MARGIN_TOP = 30;
    private static final int MARGIN_BOTTOM = 10;

    private static final Color COLOR_BG = new Color(255, 255, 255);
    private static final Color COLOR_GRID = new Color(230, 230, 230);
    private static final Color COLOR_LINE = new Color(59, 171, 82);
    private static final Color COLOR_FILL = new Color(200, 235, 200);
    private static final Color COLOR_TEXT = new Color(80, 80, 80);
    private static final Color COLOR_VALUE = new Color(30, 30, 30);

    private final List<DataPoint> dataPoints = new ArrayList<>();
    private String unit = "";
    private String valueFormat = "%.1f";
    private boolean isBytes;

    public YDBDashboardChartComposite(Composite parent, int style) {
        super(parent, style | SWT.DOUBLE_BUFFERED);
        setBackground(COLOR_BG);
        addPaintListener(this::paint);
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public void setValueFormat(String format) {
        this.valueFormat = format;
    }

    public void setIsBytes(boolean isBytes) {
        this.isBytes = isBytes;
    }

    public synchronized void addDataPoint(long timestamp, double value) {
        dataPoints.add(new DataPoint(timestamp, value));
        while (dataPoints.size() > MAX_DATA_POINTS) {
            dataPoints.remove(0);
        }
    }

    public synchronized void clearData() {
        dataPoints.clear();
    }

    private void paint(PaintEvent e) {
        GC gc = e.gc;
        gc.setAntialias(SWT.ON);
        Rectangle bounds = getClientArea();

        gc.setBackground(COLOR_BG);
        gc.fillRectangle(bounds);

        List<DataPoint> points;
        synchronized (this) {
            points = new ArrayList<>(dataPoints);
        }

        int chartX = MARGIN_LEFT;
        int chartY = MARGIN_TOP;
        int chartW = bounds.width - MARGIN_LEFT - MARGIN_RIGHT;
        int chartH = bounds.height - MARGIN_TOP - MARGIN_BOTTOM;

        if (chartW <= 0 || chartH <= 0) {
            return;
        }

        // Find Y range
        double minVal = 0;
        double maxVal = 1;
        for (DataPoint dp : points) {
            if (dp.value > maxVal) {
                maxVal = dp.value;
            }
        }
        maxVal = niceMax(maxVal);

        // Draw grid lines (4 horizontal lines)
        gc.setForeground(COLOR_GRID);
        gc.setLineWidth(1);
        gc.setLineStyle(SWT.LINE_DOT);
        Font smallFont = new Font(e.display, "Arial", 8, SWT.NORMAL);
        gc.setFont(smallFont);
        gc.setForeground(COLOR_TEXT);
        for (int i = 0; i <= 4; i++) {
            int y = chartY + chartH - (int) ((double) i / 4 * chartH);
            gc.setForeground(COLOR_GRID);
            gc.drawLine(chartX, y, chartX + chartW, y);
            double val = minVal + (maxVal - minVal) * i / 4;
            gc.setForeground(COLOR_TEXT);
            String label = formatValue(val);
            Point extent = gc.textExtent(label);
            gc.drawText(label, chartX - extent.x - 4, y - extent.y / 2, true);
        }
        gc.setLineStyle(SWT.LINE_SOLID);

        // Draw data
        if (points.size() >= 2) {
            int[] xCoords = new int[points.size()];
            int[] yCoords = new int[points.size()];

            for (int i = 0; i < points.size(); i++) {
                xCoords[i] = chartX + (int) ((double) i / (points.size() - 1) * chartW);
                double ratio = (points.get(i).value - minVal) / (maxVal - minVal);
                yCoords[i] = chartY + chartH - (int) (ratio * chartH);
            }

            // Fill area
            int[] fillPoly = new int[(points.size() + 2) * 2];
            fillPoly[0] = xCoords[0];
            fillPoly[1] = chartY + chartH;
            for (int i = 0; i < points.size(); i++) {
                fillPoly[(i + 1) * 2] = xCoords[i];
                fillPoly[(i + 1) * 2 + 1] = yCoords[i];
            }
            fillPoly[(points.size() + 1) * 2] = xCoords[points.size() - 1];
            fillPoly[(points.size() + 1) * 2 + 1] = chartY + chartH;

            gc.setBackground(COLOR_FILL);
            gc.fillPolygon(fillPoly);

            // Draw line
            gc.setForeground(COLOR_LINE);
            gc.setLineWidth(2);
            for (int i = 1; i < points.size(); i++) {
                gc.drawLine(xCoords[i - 1], yCoords[i - 1], xCoords[i], yCoords[i]);
            }
        }

        // Draw current value in top-left
        if (!points.isEmpty()) {
            double currentVal = points.get(points.size() - 1).value;
            String valText = formatValue(currentVal) + " " + unit;
            Font boldFont = new Font(e.display, "Arial", 11, SWT.BOLD);
            gc.setFont(boldFont);
            gc.setForeground(COLOR_VALUE);
            gc.drawText(valText, chartX + 4, 4, true);
            boldFont.dispose();
        }

        smallFont.dispose();
    }

    private String formatValue(double val) {
        if (isBytes) {
            return formatBytes(val);
        }
        return String.format(Locale.US, valueFormat, val);
    }

    private static String formatBytes(double bytes) {
        if (bytes <= 0) {
            return "0";
        }
        double tb = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
        if (tb >= 1) {
            return String.format(Locale.US, "%.1f TB", tb);
        }
        double gb = bytes / (1024.0 * 1024.0 * 1024.0);
        if (gb >= 1) {
            return String.format(Locale.US, "%.1f GB", gb);
        }
        double mb = bytes / (1024.0 * 1024.0);
        if (mb >= 1) {
            return String.format(Locale.US, "%.0f MB", mb);
        }
        double kb = bytes / 1024.0;
        return String.format(Locale.US, "%.0f KB", kb);
    }

    private static double niceMax(double val) {
        if (val <= 0) {
            return 1;
        }
        if (val <= 1) {
            return 1;
        }
        if (val <= 10) {
            return Math.ceil(val);
        }
        if (val <= 100) {
            return Math.ceil(val / 10) * 10;
        }
        double magnitude = Math.pow(10, Math.floor(Math.log10(val)));
        return Math.ceil(val / magnitude) * magnitude;
    }

    private static class DataPoint {
        final long timestamp;
        final double value;

        DataPoint(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}
