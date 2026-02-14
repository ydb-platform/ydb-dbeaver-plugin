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
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.jkiss.dbeaver.ext.ydb.model.YDBStreamingQuery;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlanNode;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlanParser;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.jkiss.dbeaver.ui.IRefreshablePart;
import org.jkiss.dbeaver.ui.editors.AbstractDatabaseObjectEditor;

import java.util.List;

/**
 * Editor tab that displays a YDB streaming query execution plan
 * as a graphical diagram on the left and a properties table on the right.
 */
public class YDBStreamingQueryPlanEditor extends AbstractDatabaseObjectEditor<YDBStreamingQuery> {

    private YDBPlanDiagramViewer diagramViewer;
    private Tree propsTree;
    private boolean loaded;

    @Override
    public void createPartControl(Composite parent) {
        parent.setLayout(new FillLayout());

        SashForm sash = new SashForm(parent, SWT.HORIZONTAL);

        // Left: diagram
        diagramViewer = new YDBPlanDiagramViewer(sash);

        // Right: properties table
        Composite rightPanel = new Composite(sash, SWT.NONE);
        GridLayout gl = new GridLayout(1, false);
        gl.marginWidth = 0;
        gl.marginHeight = 0;
        rightPanel.setLayout(gl);

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

        // Wire node selection from diagram to properties
        diagramViewer.setNodeSelectionListener(this::showNodeProperties);
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
            // Show truncated value in the row, full value as child
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
    public void activatePart() {
        if (!loaded) {
            loaded = true;
            YDBStreamingQuery query = getDatabaseObject();
            if (query != null && query.getPlan() != null) {
                List<YDBPlanNode> nodes = YDBPlanParser.parse(query.getPlan());
                diagramViewer.setInput(nodes);
            }
        }
    }

    @Override
    public RefreshResult refreshPart(Object source, boolean force) {
        loaded = false;
        activatePart();
        return IRefreshablePart.RefreshResult.REFRESHED;
    }

    @Override
    public void setFocus() {
        if (diagramViewer != null) {
            diagramViewer.setFocus();
        }
    }
}
