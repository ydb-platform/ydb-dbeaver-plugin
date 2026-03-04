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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.model.dashboard.registry.DashboardItemConfiguration;
import org.jkiss.dbeaver.ui.IObjectPropertyConfigurator;

public class YDBDashboardConfigEditor implements IObjectPropertyConfigurator<DashboardItemConfiguration, DashboardItemConfiguration> {
    @Override
    public void createControl(@NotNull Composite composite, DashboardItemConfiguration config, @NotNull Runnable propertyChangeListener) {
        Label label = new Label(composite, SWT.NONE);
        label.setText("This dashboard is auto-configured by the YDB plugin.");
    }

    @Override
    public void loadSettings(@NotNull DashboardItemConfiguration config) {
    }

    @Override
    public void saveSettings(@NotNull DashboardItemConfiguration config) {
    }

    @Override
    public void resetSettings(@NotNull DashboardItemConfiguration config) {
    }

    @Override
    public boolean isComplete() {
        return true;
    }
}
