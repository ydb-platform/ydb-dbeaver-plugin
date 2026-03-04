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

import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IWorkbenchPart;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.sql.SQLQuery;
import org.jkiss.dbeaver.ui.editors.sql.SQLPlanSaveProvider;

/**
 * Plan view provider for SVG-based statistics visualization.
 * Registered as an additional tab in the EXPLAIN panel for YDB connections.
 */
public class YDBStatisticsSvgViewProvider extends SQLPlanSaveProvider {

    @Override
    public Viewer createPlanViewer(IWorkbenchPart workbenchPart, Composite parent) {
        return new YDBStatisticsSvgJFaceViewer(parent);
    }

    @Override
    public void visualizeQueryPlan(Viewer viewer, SQLQuery query, DBCPlan plan) {
        fillPlan(query, plan);
        showPlan(viewer, query, plan);
    }

    @Override
    protected void showPlan(Viewer viewer, SQLQuery query, DBCPlan plan) {
        ((YDBStatisticsSvgJFaceViewer) viewer).showPlan(query, plan);
    }
}
