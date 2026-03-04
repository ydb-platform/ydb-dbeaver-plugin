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

import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.IWorkbenchPart;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlanNode;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.jkiss.dbeaver.model.sql.SQLQuery;
import org.jkiss.dbeaver.ui.UIUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * JFace Viewer wrapper around {@link YDBPlanDiagramViewer} and a properties panel.
 * Used as a plan view in the SQL editor's EXPLAIN panel.
 */
public class YDBPlanDiagramJFaceViewer extends Viewer {

    private final Composite composite;
    private final YDBPlanDiagramViewer diagramViewer;
    private final Tree propsTree;

    public YDBPlanDiagramJFaceViewer(IWorkbenchPart workbenchPart, Composite parent) {
        composite = UIUtils.createPlaceholder(parent, 1);
        composite.setLayoutData(new GridData(GridData.FILL_BOTH));

        SashForm sash = UIUtils.createPartDivider(workbenchPart, composite, SWT.HORIZONTAL);
        sash.setLayoutData(new GridData(GridData.FILL_BOTH));

        // Left: graphical diagram
        diagramViewer = new YDBPlanDiagramViewer(sash);

        // Right: properties table
        Composite rightPanel = UIUtils.createPlaceholder(sash, 1);

        propsTree = new Tree(rightPanel, SWT.BORDER | SWT.FULL_SELECTION | SWT.H_SCROLL | SWT.V_SCROLL);
        propsTree.setHeaderVisible(true);
        propsTree.setLinesVisible(true);
        propsTree.setLayoutData(new GridData(GridData.FILL_BOTH));

        TreeColumn keyCol = new TreeColumn(propsTree, SWT.LEFT);
        keyCol.setText("Property");
        keyCol.setWidth(150);

        TreeColumn valCol = new TreeColumn(propsTree, SWT.LEFT);
        valCol.setText("Value");
        valCol.setWidth(250);

        sash.setWeights(new int[]{70, 30});

        diagramViewer.setNodeSelectionListener(this::showNodeProperties);
    }

    public void showPlan(SQLQuery query, DBCPlan plan) {
        List<? extends DBCPlanNode> planNodes = plan.getPlanNodes(null);
        List<YDBPlanNode> ydbNodes = new ArrayList<>();
        for (DBCPlanNode node : planNodes) {
            if (node instanceof YDBPlanNode) {
                ydbNodes.add((YDBPlanNode) node);
            }
        }
        diagramViewer.setInput(ydbNodes);
        propsTree.removeAll();
    }

    private void showNodeProperties(YDBPlanNode node) {
        propsTree.removeAll();
        if (node == null) {
            return;
        }

        addRow("Node Type", node.getNodeType(), false);
        if (node.getNodeName() != null && !node.getNodeName().isEmpty()) {
            addRow("Tables", node.getNodeName(), false);
        }
        if (node.getOperators() != null && !node.getOperators().isEmpty()) {
            addRow("Operators", node.getOperators(), false);
        }

        DBPPropertyDescriptor[] props = node.getProperties();
        for (DBPPropertyDescriptor prop : props) {
            String key = prop.getId().toString();
            if ("Node Type".equals(key) || "PlanNodeType".equals(key)
                || "Tables".equals(key) || "Table".equals(key)
                || "Operators".equals(key)) {
                continue;
            }
            Object value = node.getPropertyValue(null, key);
            if (value != null) {
                String valStr = value.toString();
                if (!valStr.isEmpty()) {
                    addRow(key, valStr, valStr.length() > 100 || valStr.contains("\n"));
                }
            }
        }
    }

    private void addRow(String key, String value, boolean expandable) {
        TreeItem item = new TreeItem(propsTree, SWT.NONE);
        item.setText(0, key);
        if (expandable) {
            String truncated = truncate(value, 80);
            item.setText(1, truncated);
            TreeItem child = new TreeItem(item, SWT.NONE);
            child.setText(0, "");
            child.setText(1, value);
        } else {
            item.setText(1, value);
        }
    }

    private static String truncate(String text, int maxLen) {
        String oneLine = text.replace('\n', ' ');
        if (oneLine.length() <= maxLen) {
            return oneLine;
        }
        return oneLine.substring(0, maxLen) + "...";
    }

    @Override
    public Control getControl() {
        return composite;
    }

    @Override
    public Object getInput() {
        return null;
    }

    @Override
    public ISelection getSelection() {
        return null;
    }

    @Override
    public void refresh() {
    }

    @Override
    public void setInput(Object input) {
    }

    @Override
    public void setSelection(ISelection selection, boolean reveal) {
    }
}
