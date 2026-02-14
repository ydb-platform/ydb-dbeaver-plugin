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

import org.eclipse.jface.viewers.*;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.jkiss.dbeaver.ext.ydb.model.YDBStreamingQuery;
import org.jkiss.dbeaver.ui.IRefreshablePart;
import org.jkiss.dbeaver.ui.editors.AbstractDatabaseObjectEditor;

import java.util.List;

/**
 * Editor tab that displays YDB streaming query issues as a tree table.
 */
public class YDBStreamingQueryIssuesEditor extends AbstractDatabaseObjectEditor<YDBStreamingQuery> {

    private TreeViewer treeViewer;
    private boolean loaded;

    @Override
    public void createPartControl(Composite parent) {
        Composite container = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout(1, false);
        layout.marginWidth = 0;
        layout.marginHeight = 0;
        container.setLayout(layout);

        treeViewer = new TreeViewer(container, SWT.FULL_SELECTION | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
        Tree tree = treeViewer.getTree();
        tree.setHeaderVisible(true);
        tree.setLinesVisible(true);
        tree.setLayoutData(new GridData(GridData.FILL_BOTH));

        TreeColumn severityColumn = new TreeColumn(tree, SWT.LEFT);
        severityColumn.setText("Severity");
        severityColumn.setWidth(120);

        TreeColumn messageColumn = new TreeColumn(tree, SWT.LEFT);
        messageColumn.setText("Message");
        messageColumn.setWidth(600);

        treeViewer.setContentProvider(new IssueTreeContentProvider());
        treeViewer.setLabelProvider(new IssueTreeLabelProvider());
    }

    @Override
    public void activatePart() {
        if (!loaded) {
            loaded = true;
            YDBStreamingQuery query = getDatabaseObject();
            if (query != null) {
                List<YDBStreamingQueryIssueNode> issues = YDBStreamingQueryIssueParser.parse(query.getIssues());
                treeViewer.setInput(issues);
                treeViewer.expandAll();
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
        if (treeViewer != null) {
            treeViewer.getTree().setFocus();
        }
    }

    private static class IssueTreeContentProvider implements ITreeContentProvider {

        @Override
        public Object[] getElements(Object inputElement) {
            if (inputElement instanceof List) {
                return ((List<?>) inputElement).toArray();
            }
            return new Object[0];
        }

        @Override
        public Object[] getChildren(Object parentElement) {
            if (parentElement instanceof YDBStreamingQueryIssueNode) {
                return ((YDBStreamingQueryIssueNode) parentElement).getChildren().toArray();
            }
            return new Object[0];
        }

        @Override
        public Object getParent(Object element) {
            if (element instanceof YDBStreamingQueryIssueNode) {
                return ((YDBStreamingQueryIssueNode) element).getParent();
            }
            return null;
        }

        @Override
        public boolean hasChildren(Object element) {
            if (element instanceof YDBStreamingQueryIssueNode) {
                return ((YDBStreamingQueryIssueNode) element).hasChildren();
            }
            return false;
        }
    }

    private static class IssueTreeLabelProvider extends LabelProvider implements ITableLabelProvider {

        @Override
        public Image getColumnImage(Object element, int columnIndex) {
            return null;
        }

        @Override
        public String getColumnText(Object element, int columnIndex) {
            if (element instanceof YDBStreamingQueryIssueNode) {
                YDBStreamingQueryIssueNode node = (YDBStreamingQueryIssueNode) element;
                switch (columnIndex) {
                    case 0:
                        return node.getSeverityLabel();
                    case 1:
                        return node.getMessage();
                    default:
                        return "";
                }
            }
            return "";
        }
    }
}
