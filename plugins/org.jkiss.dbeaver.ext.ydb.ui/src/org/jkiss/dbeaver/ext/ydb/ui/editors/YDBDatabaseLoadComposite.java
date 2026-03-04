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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.*;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBDatabaseLoadInfo;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBNodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class YDBDatabaseLoadComposite extends Composite {

    private static final int DONUT_SIZE = 48;
    private static final int DONUT_THICKNESS = 6;
    private static final Color COLOR_GREEN = new Color(59, 171, 82);
    private static final Color COLOR_TRACK = new Color(230, 230, 230);
    private static final Color COLOR_CARD_BG = new Color(255, 255, 255);
    private static final Color COLOR_BORDER = new Color(220, 220, 220);
    private static final Color COLOR_TITLE = new Color(100, 100, 100);
    private static final Color COLOR_LOAD_BAR_BG = new Color(230, 245, 230);

    private final List<MetricCard> metricCards = new ArrayList<>();
    private Table nodesTable;
    private Label statusLabel;

    public YDBDatabaseLoadComposite(Composite parent, int style) {
        super(parent, style);
        createContents();
    }

    private void createContents() {
        GridLayout mainLayout = new GridLayout(1, false);
        mainLayout.marginWidth = 16;
        mainLayout.marginHeight = 16;
        mainLayout.verticalSpacing = 12;
        setLayout(mainLayout);

        // Metrics cards row
        Composite cardsRow = new Composite(this, SWT.NONE);
        cardsRow.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
        GridLayout cardsLayout = new GridLayout(4, true);
        cardsLayout.horizontalSpacing = 12;
        cardsLayout.marginWidth = 0;
        cardsLayout.marginHeight = 0;
        cardsRow.setLayout(cardsLayout);

        metricCards.add(createMetricCard(cardsRow, "CPU load"));
        metricCards.add(createMetricCard(cardsRow, "Storage"));
        metricCards.add(createMetricCard(cardsRow, "Memory used"));
        metricCards.add(createMetricCard(cardsRow, "Network usage"));

        // Nodes section
        Composite nodesSection = new Composite(this, SWT.BORDER);
        nodesSection.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
        nodesSection.setBackground(COLOR_CARD_BG);
        GridLayout nodesLayout = new GridLayout(1, false);
        nodesLayout.marginWidth = 16;
        nodesLayout.marginHeight = 12;
        nodesSection.setLayout(nodesLayout);

        Label nodesTitle = new Label(nodesSection, SWT.NONE);
        nodesTitle.setText("Top nodes by load");
        Font boldFont = createBoldFont(nodesTitle);
        nodesTitle.setFont(boldFont);
        nodesTitle.setBackground(COLOR_CARD_BG);
        nodesTitle.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
        nodesTitle.addDisposeListener(e -> boldFont.dispose());

        nodesTable = new Table(nodesSection, SWT.FULL_SELECTION | SWT.V_SCROLL);
        nodesTable.setHeaderVisible(true);
        nodesTable.setLinesVisible(false);
        nodesTable.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

        createColumn("Load", 80);
        createColumn("#", 70);
        createColumn("Host", 280);
        createColumn("Version", 200);

        // Status bar
        statusLabel = new Label(this, SWT.NONE);
        statusLabel.setLayoutData(new GridData(SWT.FILL, SWT.BOTTOM, true, false));
        statusLabel.setText("Loading...");
    }

    private MetricCard createMetricCard(Composite parent, String title) {
        Composite card = new Composite(parent, SWT.BORDER);
        card.setBackground(COLOR_CARD_BG);
        GridData cardGd = new GridData(SWT.FILL, SWT.FILL, true, false);
        cardGd.heightHint = 80;
        card.setLayoutData(cardGd);
        GridLayout cardLayout = new GridLayout(2, false);
        cardLayout.marginWidth = 12;
        cardLayout.marginHeight = 12;
        cardLayout.horizontalSpacing = 12;
        card.setLayout(cardLayout);

        MetricCard mc = new MetricCard();
        mc.title = title;

        // Donut canvas
        Canvas donut = new Canvas(card, SWT.DOUBLE_BUFFERED);
        donut.setBackground(COLOR_CARD_BG);
        GridData donutGd = new GridData(SWT.CENTER, SWT.CENTER, false, false);
        donutGd.widthHint = DONUT_SIZE;
        donutGd.heightHint = DONUT_SIZE;
        donut.setLayoutData(donutGd);
        mc.donutCanvas = donut;

        donut.addPaintListener(e -> paintDonut(e, mc));

        // Text area (value + title)
        Composite textArea = new Composite(card, SWT.NONE);
        textArea.setBackground(COLOR_CARD_BG);
        textArea.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
        GridLayout textLayout = new GridLayout(1, false);
        textLayout.marginWidth = 0;
        textLayout.marginHeight = 0;
        textLayout.verticalSpacing = 2;
        textArea.setLayout(textLayout);

        Label valueLabel = new Label(textArea, SWT.NONE);
        valueLabel.setBackground(COLOR_CARD_BG);
        valueLabel.setText("—");
        Font boldValueFont = createBoldFont(valueLabel);
        valueLabel.setFont(boldValueFont);
        valueLabel.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
        valueLabel.addDisposeListener(e -> boldValueFont.dispose());
        mc.valueLabel = valueLabel;

        Label titleLabel = new Label(textArea, SWT.NONE);
        titleLabel.setBackground(COLOR_CARD_BG);
        titleLabel.setText(title);
        titleLabel.setForeground(COLOR_TITLE);
        titleLabel.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
        mc.titleLabel = titleLabel;

        return mc;
    }

    private void paintDonut(PaintEvent e, MetricCard mc) {
        GC gc = e.gc;
        gc.setAntialias(SWT.ON);

        int size = DONUT_SIZE;
        int thickness = DONUT_THICKNESS;
        int arcSize = size - thickness;

        // Track (background ring)
        gc.setForeground(COLOR_TRACK);
        gc.setLineWidth(thickness);
        gc.setLineCap(SWT.CAP_ROUND);
        gc.drawArc(thickness / 2, thickness / 2, arcSize, arcSize, 0, 360);

        // Value arc
        if (mc.percent > 0) {
            gc.setForeground(COLOR_GREEN);
            int arcAngle = (int) Math.round(mc.percent / 100.0 * 360);
            if (arcAngle > 0) {
                gc.drawArc(thickness / 2, thickness / 2, arcSize, arcSize, 90, -arcAngle);
            }
        }

        // Percent text in center
        String text = String.format(Locale.US, "%.0f%%", mc.percent);
        gc.setForeground(e.display.getSystemColor(SWT.COLOR_BLACK));
        Font smallFont = new Font(e.display, "Arial", 9, SWT.BOLD);
        gc.setFont(smallFont);
        Point textExtent = gc.textExtent(text);
        gc.drawText(text, (size - textExtent.x) / 2, (size - textExtent.y) / 2, true);
        smallFont.dispose();
    }

    private void createColumn(String name, int width) {
        TableColumn column = new TableColumn(nodesTable, SWT.LEFT);
        column.setText(name);
        column.setWidth(width);
    }

    private Font createBoldFont(Control control) {
        FontData[] fd = control.getFont().getFontData();
        for (FontData f : fd) {
            f.setStyle(f.getStyle() | SWT.BOLD);
        }
        return new Font(control.getDisplay(), fd);
    }

    public void updateData(YDBDatabaseLoadInfo info) {
        if (isDisposed()) {
            return;
        }

        if (info.getErrorMessage() != null) {
            statusLabel.setText("Error: " + info.getErrorMessage());
            for (MetricCard mc : metricCards) {
                mc.percent = 0;
                mc.valueLabel.setText("—");
                mc.donutCanvas.redraw();
            }
            nodesTable.removeAll();
            return;
        }

        // CPU
        MetricCard cpu = metricCards.get(0);
        cpu.percent = info.getCpuPercent();
        cpu.valueLabel.setText(String.format(Locale.US, "%.0f of %.0f cores",
            info.getCoresUsed(), info.getCoresTotal()));
        cpu.donutCanvas.redraw();

        // Storage
        MetricCard storage = metricCards.get(1);
        double storagePercent = info.getStorageTotal() > 0
            ? (double) info.getStorageUsed() / info.getStorageTotal() * 100 : 0;
        storage.percent = storagePercent;
        storage.valueLabel.setText(formatBytes(info.getStorageUsed()) + " of " + formatBytes(info.getStorageTotal()));
        storage.donutCanvas.redraw();

        // Memory
        MetricCard memory = metricCards.get(2);
        double memPercent = info.getMemoryTotal() > 0
            ? (double) info.getMemoryUsed() / info.getMemoryTotal() * 100 : 0;
        memory.percent = memPercent;
        memory.valueLabel.setText(formatBytes(info.getMemoryUsed()) + " of " + formatBytes(info.getMemoryTotal()));
        memory.donutCanvas.redraw();

        // Network
        MetricCard network = metricCards.get(3);
        network.percent = 0; // network doesn't have a meaningful percentage
        double kbps = info.getNetworkKBPerSec();
        if (kbps > 1024) {
            network.valueLabel.setText(String.format(Locale.US, "%.0f MB/s", kbps / 1024));
        } else {
            network.valueLabel.setText(String.format(Locale.US, "%.0f KB/s", kbps));
        }
        network.donutCanvas.redraw();

        // Nodes table
        nodesTable.removeAll();
        for (YDBNodeInfo node : info.getNodes()) {
            TableItem item = new TableItem(nodesTable, SWT.NONE);
            item.setText(0, String.format(Locale.US, "%.0f%%", node.getLoadPercent()));
            item.setText(1, String.valueOf(node.getNodeId()));
            item.setText(2, node.getHost());
            item.setText(3, node.getVersion());
        }

        // Status
        String status = info.getOverallStatus();
        if (status == null) {
            status = "Unknown";
        }
        statusLabel.setText(String.format("Status: %s | Nodes: %d / %d alive",
            status, info.getNodesAlive(), info.getNodesTotal()));

        layout(true, true);
    }

    private static String formatBytes(long bytes) {
        if (bytes <= 0) {
            return "0";
        }
        double tb = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
        if (tb >= 1) {
            return String.format(Locale.US, "%.0f TB", tb);
        }
        double gb = bytes / (1024.0 * 1024.0 * 1024.0);
        if (gb >= 1) {
            return String.format(Locale.US, "%.1f GB", gb);
        }
        double mb = bytes / (1024.0 * 1024.0);
        return String.format(Locale.US, "%.0f MB", mb);
    }

    private static class MetricCard {
        Canvas donutCanvas;
        Label valueLabel;
        Label titleLabel;
        String title;
        double percent;
    }
}
