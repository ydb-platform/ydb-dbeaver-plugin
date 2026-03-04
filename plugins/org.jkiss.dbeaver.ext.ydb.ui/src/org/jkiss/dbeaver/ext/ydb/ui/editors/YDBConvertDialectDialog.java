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

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.*;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.YDBSqlglotClient;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UIUtils;

import java.util.Collections;
import java.util.List;

public class YDBConvertDialectDialog extends Dialog {

    private static final Log log = Log.getLog(YDBConvertDialectDialog.class);

    private Combo dialectCombo;
    private Text inputText;
    private Text resultText;
    private Button convertButton;
    private Button copyButton;

    public YDBConvertDialectDialog(Shell parentShell) {
        super(parentShell);
        setShellStyle(getShellStyle() | SWT.RESIZE | SWT.MAX);
    }

    @Override
    protected void configureShell(Shell newShell) {
        super.configureShell(newShell);
        newShell.setText("Convert SQL Dialect");
        newShell.setSize(700, 600);
    }

    @Override
    protected Control createDialogArea(Composite parent) {
        Composite container = (Composite) super.createDialogArea(parent);
        container.setLayout(new GridLayout(1, false));

        // Dialect selector
        Composite dialectPanel = new Composite(container, SWT.NONE);
        dialectPanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        dialectPanel.setLayout(new GridLayout(2, false));

        new Label(dialectPanel, SWT.NONE).setText("Source Dialect:");
        dialectCombo = new Combo(dialectPanel, SWT.DROP_DOWN | SWT.READ_ONLY);
        dialectCombo.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        List<String> dialects;
        try {
            dialects = YDBSqlglotClient.getDialects();
        } catch (DBException e) {
            log.error("Failed to fetch dialects from sqlglot service", e);
            dialects = Collections.emptyList();
        }
        for (String dialect : dialects) {
            dialectCombo.add(dialect);
        }
        if (dialectCombo.getItemCount() > 0) {
            dialectCombo.select(0);
        }

        // Input SQL
        new Label(container, SWT.NONE).setText("Input SQL:");
        inputText = new Text(container, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.WRAP);
        GridData inputGd = new GridData(GridData.FILL_BOTH);
        inputGd.heightHint = 150;
        inputText.setLayoutData(inputGd);

        // Convert and Copy buttons
        Composite buttonPanel = new Composite(container, SWT.NONE);
        buttonPanel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        buttonPanel.setLayout(new GridLayout(2, false));

        convertButton = new Button(buttonPanel, SWT.PUSH);
        convertButton.setText("Convert");
        convertButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                doConvert();
            }
        });

        copyButton = new Button(buttonPanel, SWT.PUSH);
        copyButton.setText("Copy");
        copyButton.setEnabled(false);
        copyButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                doCopy();
            }
        });

        // Result SQL
        new Label(container, SWT.NONE).setText("Result (YDB SQL):");
        resultText = new Text(container, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.WRAP | SWT.READ_ONLY);
        GridData resultGd = new GridData(GridData.FILL_BOTH);
        resultGd.heightHint = 150;
        resultText.setLayoutData(resultGd);

        return container;
    }

    private void doConvert() {
        String sql = inputText.getText().trim();
        if (sql.isEmpty()) {
            DBWorkbench.getPlatformUI().showError("Convert Dialect", "Please enter SQL to convert.");
            return;
        }

        int idx = dialectCombo.getSelectionIndex();
        if (idx < 0) {
            return;
        }
        String dialect = dialectCombo.getItem(idx);

        convertButton.setEnabled(false);
        resultText.setText("Converting...");

        Job job = new Job("Converting SQL dialect") {
            @Override
            protected IStatus run(IProgressMonitor monitor) {
                try {
                    String result = YDBSqlglotClient.convertSql(sql, dialect);
                    UIUtils.asyncExec(() -> {
                        if (resultText.isDisposed()) {
                            return;
                        }
                        resultText.setText(result);
                        convertButton.setEnabled(true);
                        copyButton.setEnabled(true);
                    });
                    return Status.OK_STATUS;
                } catch (Exception e) {
                    log.error("Failed to convert SQL", e);
                    UIUtils.asyncExec(() -> {
                        if (resultText.isDisposed()) {
                            return;
                        }
                        resultText.setText("Error: " + e.getMessage());
                        convertButton.setEnabled(true);
                    });
                    return Status.CANCEL_STATUS;
                }
            }
        };
        job.setUser(false);
        job.schedule();
    }

    private void doCopy() {
        String text = resultText.getText();
        if (text.isEmpty()) {
            return;
        }
        Clipboard clipboard = new Clipboard(getShell().getDisplay());
        try {
            clipboard.setContents(
                new Object[]{text},
                new Transfer[]{TextTransfer.getInstance()}
            );
        } finally {
            clipboard.dispose();
        }
    }

    @Override
    protected void createButtonsForButtonBar(Composite parent) {
        createButton(parent, IDialogConstants.OK_ID, IDialogConstants.CLOSE_LABEL, true);
    }
}
