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

import org.eclipse.swt.widgets.Composite;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.ui.IObjectPropertyConfigurator;
import org.jkiss.dbeaver.ui.dashboard.model.DashboardItemViewSettings;

public class YDBDashboardViewSettingsEditor implements IObjectPropertyConfigurator<DashboardItemViewSettings, DashboardItemViewSettings> {
    @Override
    public void createControl(@NotNull Composite composite, DashboardItemViewSettings config, @NotNull Runnable propertyChangeListener) {
    }

    @Override
    public void loadSettings(@NotNull DashboardItemViewSettings settings) {
    }

    @Override
    public void saveSettings(@NotNull DashboardItemViewSettings settings) {
    }

    @Override
    public void resetSettings(@NotNull DashboardItemViewSettings settings) {
    }

    @Override
    public boolean isComplete() {
        return true;
    }
}
