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

import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.chart.ui.RectangleEdge;
import org.jfree.chart.ui.RectangleInsets;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.data.YDBChartDataConverter;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.charts.BaseChartComposite;
import org.jkiss.dbeaver.ui.controls.resultset.ResultSetRow;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.geom.Ellipse2D;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class YDBChartDialog extends Dialog {

    private static final Log log = Log.getLog(YDBChartDialog.class);
    private static final int EXPORT_PNG_ID = IDialogConstants.CLIENT_ID + 1;

    private static final Color[] MODERN_PALETTE = {
        new Color(59, 130, 246),   // blue
        new Color(239, 68, 68),    // red
        new Color(16, 185, 129),   // emerald
        new Color(245, 158, 11),   // amber
        new Color(139, 92, 246),   // violet
        new Color(236, 72, 153),   // pink
        new Color(6, 182, 212),    // cyan
        new Color(249, 115, 22),   // orange
        new Color(34, 197, 94),    // green
        new Color(99, 102, 241),   // indigo
    };

    private static final Color CHART_BG = Color.WHITE;
    private static final Color GRID_COLOR = new Color(241, 245, 249);
    private static final Color AXIS_COLOR = new Color(148, 163, 184);
    private static final Color AXIS_LABEL_COLOR = new Color(71, 85, 105);
    private static final Font AXIS_LABEL_FONT = new Font("SansSerif", Font.PLAIN, 12);
    private static final Font AXIS_TICK_FONT = new Font("SansSerif", Font.PLAIN, 11);
    private static final Font LEGEND_FONT = new Font("SansSerif", Font.PLAIN, 12);
    private static final Font PIE_LABEL_FONT = new Font("SansSerif", Font.PLAIN, 11);

    private final List<DBDAttributeBinding> attributes;
    private final List<ResultSetRow> rows;

    private Combo chartTypeCombo;

    // Pie chart controls
    private Composite piePanel;
    private Combo pieNamesCombo;
    private Combo pieValuesCombo;

    // Line chart controls
    private Composite linePanel;
    private Combo lineXCombo;
    private Combo lineYCombo;
    private Combo lineLabelsCombo;

    private BaseChartComposite chartComposite;

    public YDBChartDialog(Shell parentShell, List<DBDAttributeBinding> attributes,
                          List<ResultSetRow> rows) {
        super(parentShell);
        this.attributes = attributes;
        this.rows = rows;
        setShellStyle(getShellStyle() | SWT.RESIZE | SWT.MAX);
    }

    @Override
    protected void configureShell(Shell newShell) {
        super.configureShell(newShell);
        newShell.setText("YDB Chart");
        newShell.setSize(1000, 700);
    }

    @Override
    protected Control createDialogArea(Composite parent) {
        Composite container = (Composite) super.createDialogArea(parent);
        container.setLayout(new GridLayout(1, false));

        createSettingsPanel(container);
        createChartArea(container);

        // Build initial chart after controls are ready
        container.getDisplay().asyncExec(this::rebuildChart);

        return container;
    }

    private final SelectionAdapter rebuildListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
            rebuildChart();
        }
    };

    private void createSettingsPanel(Composite parent) {
        Composite settingsPanel = new Composite(parent, SWT.NONE);
        settingsPanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        settingsPanel.setLayout(new GridLayout(2, false));

        new Label(settingsPanel, SWT.NONE).setText("Chart Type:");
        chartTypeCombo = new Combo(settingsPanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        chartTypeCombo.setItems("Pie Chart", "Line Chart");
        chartTypeCombo.select(0);

        // Pie chart panel
        piePanel = new Composite(parent, SWT.NONE);
        piePanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        piePanel.setLayout(new GridLayout(4, false));

        new Label(piePanel, SWT.NONE).setText("Names:");
        pieNamesCombo = new Combo(piePanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        fillCombo(pieNamesCombo, null);
        pieNamesCombo.addSelectionListener(rebuildListener);

        new Label(piePanel, SWT.NONE).setText("Values:");
        pieValuesCombo = new Combo(piePanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        fillCombo(pieValuesCombo, DBPDataKind.NUMERIC);
        pieValuesCombo.addSelectionListener(rebuildListener);

        // Line chart panel
        linePanel = new Composite(parent, SWT.NONE);
        linePanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        linePanel.setLayout(new GridLayout(6, false));

        new Label(linePanel, SWT.NONE).setText("X Axis:");
        lineXCombo = new Combo(linePanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        fillCombo(lineXCombo, null);
        lineXCombo.addSelectionListener(rebuildListener);

        new Label(linePanel, SWT.NONE).setText("Y Axis:");
        lineYCombo = new Combo(linePanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        fillCombo(lineYCombo, DBPDataKind.NUMERIC);
        lineYCombo.addSelectionListener(rebuildListener);

        new Label(linePanel, SWT.NONE).setText("Labels:");
        lineLabelsCombo = new Combo(linePanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        fillCombo(lineLabelsCombo, null);
        lineLabelsCombo.addSelectionListener(rebuildListener);

        // Initially hide line panel
        linePanel.setVisible(false);
        ((GridData) linePanel.getLayoutData()).exclude = true;

        chartTypeCombo.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                boolean isPie = chartTypeCombo.getSelectionIndex() == 0;
                piePanel.setVisible(isPie);
                ((GridData) piePanel.getLayoutData()).exclude = !isPie;
                linePanel.setVisible(!isPie);
                ((GridData) linePanel.getLayoutData()).exclude = isPie;
                parent.layout(true, true);
                rebuildChart();
            }
        });
    }

    private void fillCombo(Combo combo, DBPDataKind filterKind) {
        for (DBDAttributeBinding attr : attributes) {
            if (filterKind == null || attr.getDataKind() == filterKind) {
                combo.add(attr.getName());
                combo.setData(attr.getName(), attr);
            }
        }
        if (combo.getItemCount() > 0) {
            combo.select(0);
        }
    }

    private void createChartArea(Composite parent) {
        chartComposite = new BaseChartComposite(parent, SWT.DOUBLE_BUFFERED, new Point(800, 500));
        GridData gd = new GridData(GridData.FILL_BOTH);
        gd.widthHint = 800;
        gd.heightHint = 500;
        chartComposite.setLayoutData(gd);
    }

    private void rebuildChart() {
        if (chartTypeCombo.getSelectionIndex() == 0) {
            buildPieChart();
        } else {
            buildLineChart();
        }
    }

    private void updateChart(JFreeChart chart) {
        chartComposite.setChart(chart);
        chartComposite.forceRedraw();
        Canvas canvas = chartComposite.getChartCanvas();
        if (canvas != null && !canvas.isDisposed()) {
            canvas.redraw();
            canvas.update();
        }
    }

    private void buildPieChart() {
        DBDAttributeBinding namesAttr = getSelectedAttribute(pieNamesCombo);
        DBDAttributeBinding valuesAttr = getSelectedAttribute(pieValuesCombo);
        if (namesAttr == null || valuesAttr == null) {
            return;
        }

        DefaultPieDataset<String> dataset = new DefaultPieDataset<>();
        for (ResultSetRow row : rows) {
            String name = formatValue(getCellValue(namesAttr, row));
            double value = toDouble(getCellValue(valuesAttr, row));
            dataset.setValue(name, value);
        }

        JFreeChart chart = ChartFactory.createPieChart(null, dataset, true, true, false);
        styleChart(chart);

        PiePlot<?> plot = (PiePlot<?>) chart.getPlot();
        plot.setBackgroundPaint(CHART_BG);
        plot.setOutlinePaint(null);
        plot.setShadowPaint(null);
        plot.setInteriorGap(0.02);
        plot.setLabelGenerator(new StandardPieSectionLabelGenerator("{0}: {1} ({2})"));
        plot.setLabelFont(PIE_LABEL_FONT);
        plot.setLabelOutlinePaint(null);
        plot.setLabelShadowPaint(null);
        plot.setLabelBackgroundPaint(new Color(255, 255, 255, 200));
        plot.setDefaultSectionOutlinePaint(Color.WHITE);
        plot.setDefaultSectionOutlineStroke(new BasicStroke(2.0f));

        for (int i = 0; i < dataset.getItemCount(); i++) {
            Comparable<?> key = dataset.getKey(i);
            plot.setSectionPaint(key, MODERN_PALETTE[i % MODERN_PALETTE.length]);
        }

        updateChart(chart);
    }

    private void buildLineChart() {
        DBDAttributeBinding xAttr = getSelectedAttribute(lineXCombo);
        DBDAttributeBinding yAttr = getSelectedAttribute(lineYCombo);
        DBDAttributeBinding labelsAttr = getSelectedAttribute(lineLabelsCombo);
        if (xAttr == null || yAttr == null) {
            return;
        }

        List<String> tooltipLabels = new ArrayList<>();
        if (labelsAttr != null) {
            for (ResultSetRow row : rows) {
                tooltipLabels.add(formatValue(getCellValue(labelsAttr, row)));
            }
        }

        boolean isDatetimeX = xAttr.getDataKind() == DBPDataKind.DATETIME;

        JFreeChart chart;
        if (isDatetimeX) {
            chart = buildTimeSeriesChart(xAttr, yAttr);
        } else {
            chart = buildXYChart(xAttr, yAttr);
        }

        styleChart(chart);

        if (!tooltipLabels.isEmpty()) {
            XYPlot plot = chart.getXYPlot();
            List<String> labels = tooltipLabels;
            plot.getRenderer().setDefaultToolTipGenerator(
                (XYDataset dataset, int series, int item) -> labels.get(item));
        }

        updateChart(chart);
    }

    private JFreeChart buildTimeSeriesChart(DBDAttributeBinding xAttr, DBDAttributeBinding yAttr) {
        TimeSeries series = new TimeSeries(yAttr.getName());
        for (ResultSetRow row : rows) {
            Date date = toDate(getCellValue(xAttr, row));
            double value = toDouble(getCellValue(yAttr, row));
            if (date != null) {
                series.addOrUpdate(new Millisecond(date), value);
            }
        }

        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(series);

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            null, xAttr.getName(), yAttr.getName(), dataset, true, true, false);

        XYPlot plot = chart.getXYPlot();
        configurePlot(plot);

        DateAxis domainAxis = (DateAxis) plot.getDomainAxis();
        domainAxis.setDateFormatOverride(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

        return chart;
    }

    private JFreeChart buildXYChart(DBDAttributeBinding xAttr, DBDAttributeBinding yAttr) {
        XYSeries series = new XYSeries(yAttr.getName());
        for (ResultSetRow row : rows) {
            double x = toDouble(getCellValue(xAttr, row));
            double y = toDouble(getCellValue(yAttr, row));
            series.add(x, y);
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);

        JFreeChart chart = ChartFactory.createXYLineChart(
            null, xAttr.getName(), yAttr.getName(), dataset,
            PlotOrientation.VERTICAL, true, true, false);

        XYPlot plot = chart.getXYPlot();
        configurePlot(plot);

        return chart;
    }

    private void styleChart(JFreeChart chart) {
        chart.setBackgroundPaint(CHART_BG);
        chart.setAntiAlias(true);
        chart.setTextAntiAlias(true);
        chart.setPadding(new RectangleInsets(10, 10, 10, 10));

        LegendTitle legend = chart.getLegend();
        if (legend != null) {
            legend.setItemFont(LEGEND_FONT);
            legend.setBackgroundPaint(CHART_BG);
            legend.setFrame(org.jfree.chart.block.BlockBorder.NONE);
            legend.setPosition(RectangleEdge.BOTTOM);
            legend.setPadding(new RectangleInsets(10, 0, 0, 0));
        }
    }

    private void styleAxis(ValueAxis axis) {
        axis.setAxisLinePaint(AXIS_COLOR);
        axis.setTickLabelPaint(AXIS_LABEL_COLOR);
        axis.setTickLabelFont(AXIS_TICK_FONT);
        axis.setLabelPaint(AXIS_LABEL_COLOR);
        axis.setLabelFont(AXIS_LABEL_FONT);
        axis.setTickMarkPaint(AXIS_COLOR);
    }

    private void configurePlot(XYPlot plot) {
        plot.setBackgroundPaint(CHART_BG);
        plot.setOutlinePaint(null);
        plot.setDomainGridlinePaint(GRID_COLOR);
        plot.setRangeGridlinePaint(GRID_COLOR);
        plot.setDomainGridlineStroke(new BasicStroke(1.0f));
        plot.setRangeGridlineStroke(new BasicStroke(1.0f));
        plot.setAxisOffset(new RectangleInsets(0, 0, 0, 0));

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer(true, true);
        renderer.setDefaultStroke(new BasicStroke(2.5f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
        renderer.setSeriesPaint(0, MODERN_PALETTE[0]);
        renderer.setSeriesShape(0, new Ellipse2D.Double(-4, -4, 8, 8));
        renderer.setSeriesShapesFilled(0, true);
        renderer.setSeriesOutlinePaint(0, Color.WHITE);
        renderer.setSeriesOutlineStroke(0, new BasicStroke(2.0f));
        renderer.setUseFillPaint(true);
        renderer.setSeriesFillPaint(0, MODERN_PALETTE[0]);
        plot.setRenderer(renderer);

        styleAxis(plot.getDomainAxis());
        styleAxis(plot.getRangeAxis());

        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setAutoRangeIncludesZero(false);
    }

    private DBDAttributeBinding getSelectedAttribute(Combo combo) {
        int idx = combo.getSelectionIndex();
        if (idx < 0) {
            return null;
        }
        String name = combo.getItem(idx);
        return (DBDAttributeBinding) combo.getData(name);
    }

    private Object getCellValue(DBDAttributeBinding attr, ResultSetRow row) {
        return YDBChartDataConverter.getCellValue(row.getValues(), attr.getOrdinalPosition());
    }

    private String formatValue(Object val) {
        return YDBChartDataConverter.formatValue(val);
    }

    private double toDouble(Object val) {
        return YDBChartDataConverter.toDouble(val);
    }

    private Date toDate(Object val) {
        return YDBChartDataConverter.toDate(val);
    }

    @Override
    protected void createButtonsForButtonBar(Composite parent) {
        createButton(parent, EXPORT_PNG_ID, "Export PNG", false);
        createButton(parent, IDialogConstants.OK_ID, IDialogConstants.CLOSE_LABEL, true);
    }

    @Override
    protected void buttonPressed(int buttonId) {
        if (buttonId == EXPORT_PNG_ID) {
            exportPng();
        } else {
            super.buttonPressed(buttonId);
        }
    }

    private void exportPng() {
        JFreeChart chart = chartComposite.getChart();
        if (chart == null) {
            DBWorkbench.getPlatformUI().showError("Export PNG", "No chart to export.");
            return;
        }

        FileDialog fileDialog = new FileDialog(getShell(), SWT.SAVE);
        fileDialog.setFilterExtensions(new String[]{"*.png"});
        fileDialog.setFilterNames(new String[]{"PNG Image (*.png)"});
        fileDialog.setFileName("ydb_chart.png");
        fileDialog.setOverwrite(true);

        String filePath = fileDialog.open();
        if (filePath == null) {
            return;
        }

        try {
            org.eclipse.swt.graphics.Rectangle bounds = chartComposite.getBounds();
            int width = Math.max(bounds.width, 800);
            int height = Math.max(bounds.height, 600);
            org.jfree.chart.ChartUtils.writeChartAsPNG(
                new FileOutputStream(filePath), chart, width, height);
        } catch (IOException e) {
            log.error("Failed to save PNG file", e);
            DBWorkbench.getPlatformUI().showError("Export PNG", "Failed to save PNG: " + e.getMessage());
        }
    }
}
