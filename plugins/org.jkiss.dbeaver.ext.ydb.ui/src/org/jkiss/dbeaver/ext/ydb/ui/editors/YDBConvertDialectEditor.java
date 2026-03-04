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
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.*;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.PartInitException;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.YDBSqlglotClient;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.sql.BasicSQLDialect;
import org.jkiss.dbeaver.model.sql.SQLDialect;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.editors.SinglePageDatabaseEditor;
import org.jkiss.dbeaver.ui.editors.StringEditorInput;
import org.jkiss.dbeaver.ui.editors.SubEditorSite;
import org.jkiss.dbeaver.ui.editors.sql.SQLEditorBase;
import org.jkiss.dbeaver.utils.GeneralUtils;

import java.util.Collections;
import java.util.List;

public class YDBConvertDialectEditor extends SinglePageDatabaseEditor<IEditorInput> {

    private static final Log log = Log.getLog(YDBConvertDialectEditor.class);

    private Combo dialectCombo;
    private SQLEditorBase inputEditor;
    private SQLEditorBase resultEditor;
    private Button convertButton;
    private Button copyButton;

    @Override
    public void createEditorControl(Composite parent) {
        Composite container = new Composite(parent, SWT.NONE);
        container.setLayout(new GridLayout(1, false));
        container.setLayoutData(new GridData(GridData.FILL_BOTH));

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

        // Input SQL with generic SQL highlighting
        new Label(container, SWT.NONE).setText("Input SQL:");
        inputEditor = createSqlEditor(container, "Input SQL", false, BasicSQLDialect.INSTANCE);

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

        // Result SQL with YDB SQL highlighting
        new Label(container, SWT.NONE).setText("Result (YDB SQL):");
        resultEditor = createSqlEditor(container, "Result SQL", true, null);

        // Report bug link
        Link reportLink = new Link(container, SWT.NONE);
        reportLink.setText("<a>Report a conversion issue</a>");
        reportLink.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, true, false));
        reportLink.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                org.eclipse.swt.program.Program.launch("https://forms.yandex.ru/u/697b4f569029021fd618b1f0/");
            }
        });
    }

    @NotNull
    private SQLEditorBase createSqlEditor(Composite parent, String title, boolean readOnly, @Nullable SQLDialect dialect) {
        Composite editorPH = new Composite(parent, SWT.NONE);
        GridData gd = new GridData(GridData.FILL_BOTH);
        gd.heightHint = 150;
        gd.minimumHeight = 80;
        editorPH.setLayoutData(gd);
        editorPH.setLayout(new FillLayout());

        SQLEditorBase editor = new SQLEditorBase() {
            @NotNull
            @Override
            public SQLDialect getSQLDialect() {
                if (dialect != null) {
                    return dialect;
                }
                DBCExecutionContext ctx = getExecutionContext();
                if (ctx != null) {
                    return ctx.getDataSource().getSQLDialect();
                }
                return BasicSQLDialect.INSTANCE;
            }

            @Nullable
            @Override
            public DBCExecutionContext getExecutionContext() {
                return YDBConvertDialectEditor.this.getExecutionContext();
            }

            @Override
            protected boolean isAnnotationRulerVisible() {
                return false;
            }
        };

        StringEditorInput editorInput = new StringEditorInput(title, "", readOnly, GeneralUtils.getDefaultFileEncoding());
        try {
            editor.init(new SubEditorSite(getSite()), editorInput);
        } catch (PartInitException e) {
            log.error("Failed to init SQL editor", e);
        }
        editor.createPartControl(editorPH);
        editor.reloadSyntaxRules();

        Object control = editor.getAdapter(Control.class);
        if (control instanceof StyledText) {
            ((StyledText) control).setWordWrap(true);
        }

        return editor;
    }

    private String getEditorText(SQLEditorBase editor) {
        if (editor != null && editor.getTextViewer() != null && editor.getTextViewer().getDocument() != null) {
            return editor.getTextViewer().getDocument().get();
        }
        return "";
    }

    private void setEditorText(SQLEditorBase editor, String text) {
        if (editor != null && editor.getTextViewer() != null && editor.getTextViewer().getDocument() != null) {
            editor.getTextViewer().getDocument().set(text);
        }
    }

    private void doConvert() {
        String sql = getEditorText(inputEditor).trim();
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
        setEditorText(resultEditor, "Converting...");

        Job job = new Job("Converting SQL dialect") {
            @Override
            protected IStatus run(IProgressMonitor monitor) {
                try {
                    String result = YDBSqlglotClient.convertSql(sql, dialect);
                    UIUtils.asyncExec(() -> {
                        if (convertButton.isDisposed()) {
                            return;
                        }
                        setEditorText(resultEditor, result);
                        convertButton.setEnabled(true);
                        copyButton.setEnabled(true);
                    });
                    return Status.OK_STATUS;
                } catch (Exception e) {
                    log.error("Failed to convert SQL", e);
                    UIUtils.asyncExec(() -> {
                        if (convertButton.isDisposed()) {
                            return;
                        }
                        setEditorText(resultEditor, "Error: " + e.getMessage());
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
        String text = getEditorText(resultEditor);
        if (text.isEmpty()) {
            return;
        }
        Clipboard clipboard = new Clipboard(copyButton.getDisplay());
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
    public void setFocus() {
        if (inputEditor != null) {
            Object control = inputEditor.getAdapter(Control.class);
            if (control instanceof Control && !((Control) control).isDisposed()) {
                ((Control) control).setFocus();
            }
        }
    }

    @Override
    public void dispose() {
        if (inputEditor != null) {
            inputEditor.dispose();
        }
        if (resultEditor != null) {
            resultEditor.dispose();
        }
        super.dispose();
    }

    @Override
    public RefreshResult refreshPart(Object source, boolean force) {
        return RefreshResult.REFRESHED;
    }
}
