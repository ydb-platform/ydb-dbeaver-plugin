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
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBDatabaseLoadInfo;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.dashboard.data.DashboardDataset;
import org.jkiss.dbeaver.model.dashboard.data.DashboardDatasetRow;
import org.jkiss.dbeaver.ui.dashboard.control.DashboardRendererAbstract;
import org.jkiss.dbeaver.ui.dashboard.control.DashboardViewItem;
import org.jkiss.dbeaver.ui.dashboard.model.DashboardContainer;
import org.jkiss.dbeaver.ui.dashboard.model.DashboardItemContainer;

import java.util.Date;

/**
 * Custom dashboard renderer for YDB metrics fetched from HTTP Viewer API.
 * Since YDB cluster metrics are not available via SQL, this renderer handles
 * its own data fetching through the shared YDBDashboardDataCache.
 */
public class YDBDashboardRenderer extends DashboardRendererAbstract implements YDBDashboardDataCache.Listener {

    private static final Log log = Log.getLog(YDBDashboardRenderer.class);

    private YDBDashboardChartComposite chartComposite;
    private DashboardItemContainer container;
    private DBPDataSourceContainer dsContainer;
    private String dashboardId;

    @Override
    public Composite createDashboard(
        @NotNull Composite parent,
        @NotNull DashboardItemContainer container,
        @NotNull DashboardContainer viewContainer,
        @NotNull Point preferredSize
    ) {
        this.container = container;
        this.dsContainer = container.getDataSourceContainer();
        this.dashboardId = container.getItemDescriptor().getId();

        Composite wrapper = new Composite(parent, SWT.NONE);
        wrapper.setLayout(new FillLayout());

        chartComposite = new YDBDashboardChartComposite(wrapper, SWT.NONE);
        configureChart();

        YDBDashboardDataCache.register(dsContainer, this);

        return wrapper;
    }

    private void configureChart() {
        switch (dashboardId) {
            case "ydb.cpu":
                chartComposite.setUnit("%");
                chartComposite.setValueFormat("%.1f");
                break;
            case "ydb.storage":
                chartComposite.setUnit("");
                chartComposite.setIsBytes(true);
                break;
            case "ydb.memory":
                chartComposite.setUnit("");
                chartComposite.setIsBytes(true);
                break;
            case "ydb.network":
                chartComposite.setUnit("KB/s");
                chartComposite.setValueFormat("%.0f");
                break;
            case "ydb.queries":
                chartComposite.setUnit("");
                chartComposite.setValueFormat("%.0f");
                break;
            default:
                chartComposite.setUnit("");
                break;
        }
    }

    @Override
    public void onDataUpdate(YDBDatabaseLoadInfo info) {
        if (chartComposite == null || chartComposite.isDisposed()) {
            return;
        }
        if (info.getErrorMessage() != null) {
            return;
        }

        double value = extractMetric(info);
        long timestamp = System.currentTimeMillis();

        Display display = chartComposite.getDisplay();
        if (display.isDisposed()) {
            return;
        }
        display.asyncExec(() -> {
            if (chartComposite != null && !chartComposite.isDisposed()) {
                chartComposite.addDataPoint(timestamp, value);
                chartComposite.redraw();
            }
        });
    }

    private double extractMetric(YDBDatabaseLoadInfo info) {
        switch (dashboardId) {
            case "ydb.cpu":
                return info.getCpuPercent();
            case "ydb.storage":
                return info.getStorageUsed();
            case "ydb.memory":
                return info.getMemoryUsed();
            case "ydb.network":
                return info.getNetworkKBPerSec();
            case "ydb.queries":
                return info.getRunningQueries();
            default:
                return 0;
        }
    }

    @Override
    public void updateDashboardData(
        @NotNull DashboardItemContainer container,
        @Nullable Date lastUpdateTime,
        @NotNull DashboardDataset dataset
    ) {
        // Data is pushed directly from the cache listener, not through the standard pipeline.
        // This method is called if someone externally calls container.updateDashboardData().
        if (chartComposite == null || chartComposite.isDisposed()) {
            return;
        }
        for (DashboardDatasetRow row : dataset.getRows()) {
            if (row.getValues().length > 0 && row.getValues()[0] instanceof Number) {
                chartComposite.addDataPoint(
                    row.getTimestamp().getTime(),
                    ((Number) row.getValues()[0]).doubleValue()
                );
            }
        }
        chartComposite.redraw();
    }

    @Override
    public void resetDashboardData(@NotNull DashboardItemContainer dashboardItem, @Nullable Date lastUpdateTime) {
        if (chartComposite != null && !chartComposite.isDisposed()) {
            chartComposite.clearData();
            chartComposite.redraw();
        }
    }

    @Override
    public void moveDashboardView(@NotNull DashboardViewItem toItem, @NotNull DashboardViewItem fromItem, boolean clearOriginal) {
        // Not supported for YDB dashboards
    }

    @Override
    public void updateDashboardView(@NotNull DashboardViewItem dashboardItem) {
        // Nothing to update
    }

    @Override
    public void disposeDashboard(@NotNull DashboardItemContainer container) {
        if (dsContainer != null) {
            YDBDashboardDataCache.unregister(dsContainer, this);
        }
        chartComposite = null;
        this.container = null;
    }
}
