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

import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.dialogs.InputDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.YDBPermissionHolder;
import org.jkiss.dbeaver.ext.ydb.model.YDBPermissionHolder.PermissionAction;
import org.jkiss.dbeaver.model.runtime.AbstractJob;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.ui.IRefreshablePart;
import org.jkiss.dbeaver.ui.editors.AbstractDatabaseObjectEditor;

import java.util.*;

/**
 * Editor tab for managing YDB object access rights (ACL permissions).
 * Shows a table with Subject, Explicit rights, and Effective rights columns.
 * Double-click on a subject opens a dialog with Groups/Granular tabs for editing.
 */
public class YDBRightsEditor extends AbstractDatabaseObjectEditor<YDBPermissionHolder> {

    private static final Log log = Log.getLog(YDBRightsEditor.class);

    // UI widgets
    private Label ownerLabel;
    private Table permTable;

    // State
    private boolean loaded;
    private final Map<String, Set<String>> currentPermissions = new LinkedHashMap<>();
    private final Map<String, Set<String>> originalPermissions = new LinkedHashMap<>();
    private final Map<String, Set<String>> effectivePermissions = new LinkedHashMap<>();
    private String currentOwner;
    private String originalOwner;

    @Override
    public void createPartControl(Composite parent) {
        Composite mainContainer = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout(1, false);
        layout.marginWidth = 5;
        layout.marginHeight = 5;
        mainContainer.setLayout(layout);
        mainContainer.setLayoutData(new GridData(GridData.FILL_BOTH));

        createOwnerSection(mainContainer);
        createPermissionsTable(mainContainer);
        createButtonsBar(mainContainer);
    }

    private void createOwnerSection(Composite parent) {
        Group ownerGroup = new Group(parent, SWT.NONE);
        ownerGroup.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        ownerGroup.setLayout(new GridLayout(3, false));

        ownerLabel = new Label(ownerGroup, SWT.NONE);
        ownerLabel.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        Label ownerHint = new Label(ownerGroup, SWT.WRAP);
        ownerHint.setText("The owner has complete management authority over the object");
        ownerHint.setForeground(parent.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
        GridData hintGD = new GridData(GridData.FILL_HORIZONTAL);
        hintGD.widthHint = 300;
        ownerHint.setLayoutData(hintGD);

        Button changeOwnerBtn = new Button(ownerGroup, SWT.PUSH);
        changeOwnerBtn.setText("Change Owner");
        changeOwnerBtn.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                onChangeOwner();
            }
        });
    }

    private void createPermissionsTable(Composite parent) {
        permTable = new Table(parent, SWT.FULL_SELECTION | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
        permTable.setHeaderVisible(true);
        permTable.setLinesVisible(true);
        GridData tableGD = new GridData(GridData.FILL_BOTH);
        tableGD.heightHint = 300;
        permTable.setLayoutData(tableGD);

        TableColumn subjectCol = new TableColumn(permTable, SWT.LEFT);
        subjectCol.setText("Subject");
        subjectCol.setWidth(250);

        TableColumn explicitCol = new TableColumn(permTable, SWT.LEFT);
        explicitCol.setText("Explicit rights");
        explicitCol.setWidth(300);

        TableColumn effectiveCol = new TableColumn(permTable, SWT.LEFT);
        effectiveCol.setText("Effective rights");
        effectiveCol.setWidth(400);

        // Double-click opens edit dialog
        permTable.addListener(SWT.MouseDoubleClick, event -> {
            TableItem item = permTable.getItem(new org.eclipse.swt.graphics.Point(event.x, event.y));
            if (item != null) {
                String subject = item.getText(0);
                onEditSubject(subject);
            }
        });
    }

    private void createButtonsBar(Composite parent) {
        Composite buttonsBar = new Composite(parent, SWT.NONE);
        buttonsBar.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        GridLayout btnLayout = new GridLayout(6, false);
        btnLayout.marginWidth = 0;
        buttonsBar.setLayout(btnLayout);

        Button grantBtn = new Button(buttonsBar, SWT.PUSH);
        grantBtn.setText("Grant Access");
        grantBtn.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                onGrantAccess();
            }
        });

        Button removeBtn = new Button(buttonsBar, SWT.PUSH);
        removeBtn.setText("Revoke All");
        removeBtn.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                onRevokeAll();
            }
        });

        // Spacer
        Label spacer = new Label(buttonsBar, SWT.NONE);
        spacer.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        Button applyBtn = new Button(buttonsBar, SWT.PUSH);
        applyBtn.setText("Apply");
        applyBtn.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                onApply();
            }
        });

        Button revertBtn = new Button(buttonsBar, SWT.PUSH);
        revertBtn.setText("Discard Changes");
        revertBtn.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                onRevert();
            }
        });
    }

    @Override
    public void activatePart() {
        if (!loaded) {
            loadData();
        }
    }

    private void loadData() {
        loaded = true;
        YDBPermissionHolder holder = getDatabaseObject();
        if (holder == null) return;

        new AbstractJob("Load YDB permissions") {
            @Override
            protected org.eclipse.core.runtime.IStatus run(DBRProgressMonitor monitor) {
                try {
                    holder.ensurePermissionsLoaded(monitor);
                } catch (DBException e) {
                    log.error("Failed to load permissions", e);
                }
                Display.getDefault().asyncExec(() -> {
                    if (ownerLabel.isDisposed()) return;
                    populateFromHolder(holder);
                    refreshUI();
                });
                return org.eclipse.core.runtime.Status.OK_STATUS;
            }
        }.schedule();
    }

    private void populateFromHolder(YDBPermissionHolder holder) {
        currentPermissions.clear();
        originalPermissions.clear();
        effectivePermissions.clear();

        for (YDBPermissionHolder.PermissionEntry entry : holder.getExplicitPermissions()) {
            Set<String> perms = new LinkedHashSet<>(entry.getPermissionNames());
            currentPermissions.put(entry.getSubject(), perms);
            originalPermissions.put(entry.getSubject(), new LinkedHashSet<>(perms));
        }

        for (YDBPermissionHolder.PermissionEntry entry : holder.getEffectivePermissions()) {
            effectivePermissions.put(entry.getSubject(), new LinkedHashSet<>(entry.getPermissionNames()));
        }

        currentOwner = holder.getOwner();
        originalOwner = currentOwner;
    }

    private void refreshUI() {
        if (ownerLabel.isDisposed()) return;

        ownerLabel.setText(currentOwner != null ? currentOwner + "  (owner)" : "");
        ownerLabel.getParent().layout();

        permTable.removeAll();

        // Collect all subjects (from both explicit and effective), sorted alphabetically
        Set<String> allSubjects = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        allSubjects.addAll(currentPermissions.keySet());
        allSubjects.addAll(effectivePermissions.keySet());

        for (String subject : allSubjects) {
            TableItem item = new TableItem(permTable, SWT.NONE);
            item.setText(0, subject);

            Set<String> explicit = currentPermissions.get(subject);
            item.setText(1, explicit != null ? formatPermissions(explicit) : "");

            Set<String> effective = effectivePermissions.get(subject);
            item.setText(2, effective != null ? formatPermissions(effective) : "");
        }
    }

    private String formatPermissions(Set<String> perms) {
        if (perms == null || perms.isEmpty()) return "";
        List<String> shortNames = new ArrayList<>();
        for (String perm : perms) {
            int lastDot = perm.lastIndexOf('.');
            shortNames.add(lastDot >= 0 ? perm.substring(lastDot + 1) : perm);
        }
        return String.join("   ", shortNames);
    }

    private void onChangeOwner() {
        InputDialog dialog = new InputDialog(
            getSite().getShell(),
            "Change Owner",
            "Enter new owner SID:",
            currentOwner != null ? currentOwner : "",
            input -> (input == null || input.trim().isEmpty()) ? "Owner cannot be empty" : null
        );
        if (dialog.open() == IDialogConstants.OK_ID) {
            currentOwner = dialog.getValue().trim();
            ownerLabel.setText(currentOwner + "  (owner)");
            ownerLabel.getParent().layout();
        }
    }

    private void onGrantAccess() {
        YDBAddSubjectDialog dialog = new YDBAddSubjectDialog(getSite().getShell());
        if (dialog.open() == IDialogConstants.OK_ID) {
            String subject = dialog.getSubject();
            if (subject != null && !subject.isEmpty()) {
                if (!currentPermissions.containsKey(subject)) {
                    currentPermissions.put(subject, new LinkedHashSet<>());
                }
                onEditSubject(subject);
            }
        }
    }

    private void onEditSubject(String subject) {
        Set<String> perms = currentPermissions.getOrDefault(subject, new LinkedHashSet<>());
        Set<String> effPerms = effectivePermissions.getOrDefault(subject, Collections.emptySet());

        YDBPermissionEditDialog dialog = new YDBPermissionEditDialog(
            getSite().getShell(), subject, perms, effPerms);
        if (dialog.open() == IDialogConstants.OK_ID) {
            currentPermissions.put(subject, dialog.getPermissions());
            refreshUI();
        }
    }

    private void onRevokeAll() {
        int idx = permTable.getSelectionIndex();
        if (idx < 0) {
            MessageDialog.openWarning(getSite().getShell(), "Revoke All", "Select a subject first");
            return;
        }
        String subject = permTable.getItem(idx).getText(0);
        if (MessageDialog.openConfirm(getSite().getShell(), "Revoke All",
                "Revoke all explicit permissions for " + subject + "?")) {
            currentPermissions.remove(subject);
            refreshUI();
        }
    }

    private void onRevert() {
        currentPermissions.clear();
        for (Map.Entry<String, Set<String>> entry : originalPermissions.entrySet()) {
            currentPermissions.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
        currentOwner = originalOwner;
        refreshUI();
    }

    private void onApply() {
        YDBPermissionHolder holder = getDatabaseObject();
        if (holder == null) return;

        List<PermissionAction> actions = buildActions();

        if (actions.isEmpty()) {
            MessageDialog.openInformation(getSite().getShell(), "No Changes", "No permission changes to apply.");
            return;
        }

        new AbstractJob("Modify YDB permissions") {
            @Override
            protected org.eclipse.core.runtime.IStatus run(DBRProgressMonitor monitor) {
                try {
                    holder.modifyPermissions(monitor, actions, false, null);
                    holder.ensurePermissionsLoaded(monitor);

                    Display.getDefault().asyncExec(() -> {
                        loaded = false;
                        loadData();
                    });
                } catch (DBException e) {
                    log.error("Failed to modify permissions", e);
                    Display.getDefault().asyncExec(() ->
                        MessageDialog.openError(getSite().getShell(), "Error",
                            "Failed to modify permissions: " + e.getMessage()));
                }
                return org.eclipse.core.runtime.Status.OK_STATUS;
            }
        }.schedule();
    }

    private List<PermissionAction> buildActions() {
        List<PermissionAction> actions = new ArrayList<>();

        // Change owner
        if (currentOwner != null && !currentOwner.equals(originalOwner)) {
            actions.add(PermissionAction.changeOwner(currentOwner));
        }

        // Compute per-subject diffs
        Set<String> allSubjects = new LinkedHashSet<>();
        allSubjects.addAll(originalPermissions.keySet());
        allSubjects.addAll(currentPermissions.keySet());

        for (String subject : allSubjects) {
            Set<String> origPerms = originalPermissions.getOrDefault(subject, Collections.emptySet());
            Set<String> currPerms = currentPermissions.getOrDefault(subject, Collections.emptySet());

            // Use SET action to atomically replace permissions for this subject
            if (!origPerms.equals(currPerms)) {
                actions.add(PermissionAction.set(subject, currPerms));
            }
        }

        return actions;
    }

    @Override
    public RefreshResult refreshPart(Object source, boolean force) {
        YDBPermissionHolder holder = getDatabaseObject();
        if (holder != null) {
            holder.resetPermissions();
        }
        loaded = false;
        loadData();
        return IRefreshablePart.RefreshResult.REFRESHED;
    }

    @Override
    public void setFocus() {
        if (permTable != null && !permTable.isDisposed()) {
            permTable.setFocus();
        }
    }
}
