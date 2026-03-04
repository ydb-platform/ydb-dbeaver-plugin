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

import org.eclipse.jface.dialogs.TitleAreaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.*;

import java.util.*;

/**
 * Dialog for editing permissions of a single subject.
 * Two tabs: Groups (composite permissions) and Granular (individual permissions).
 */
class YDBPermissionEditDialog extends TitleAreaDialog {

    // Group permissions: name -> { description, sub-permissions }
    private static final GroupPermission[] GROUP_PERMISSIONS = {
        new GroupPermission("full",
            "Unrestricted system access. All permissions including DB management.",
            new String[]{"manage", "use"}),
        new GroupPermission("full_legacy",
            "[Legacy] Unrestricted system access. All permissions including DB management.",
            new String[]{"grant", "manage", "insert", "select"}),
        new GroupPermission("manage",
            "Create/drop database. Infrastructure-level change.",
            new String[]{"create_database", "drop_database"}),
        new GroupPermission("use",
            "Complete data operations. Modern full access excluding DB management.",
            new String[]{"grant", "connect", "insert", "select"}),
        new GroupPermission("insert",
            "Modify data and schemas. Write/delete data + alter structure.",
            new String[]{"update_row", "erase_row", "modify_attributes", "create_directory",
                "create_table", "create_queue", "remove_schema", "alter_schema"}),
        new GroupPermission("select",
            "Read-only access. View data and metadata.",
            new String[]{"select_row", "describe_schema", "select_attributes"}),
        new GroupPermission("connect",
            "Connect to the database.",
            new String[]{}),
        new GroupPermission("grant",
            "Grant access rights to other subjects.",
            new String[]{}),
    };

    // Granular permissions
    private static final GranularPermission[] GRANULAR_PERMISSIONS = {
        new GranularPermission("ydb.granular.select_row",
            "Retrieve data rows from tables using SELECT queries."),
        new GranularPermission("ydb.granular.update_row",
            "Modify existing data rows with UPDATE operations."),
        new GranularPermission("ydb.granular.erase_row",
            "Delete existing data rows with DELETE operations."),
        new GranularPermission("ydb.granular.read_attributes",
            "View object metadata and access control lists."),
        new GranularPermission("ydb.granular.write_attributes",
            "Modify object metadata and manage access permissions."),
        new GranularPermission("ydb.granular.create_directory",
            "Create new subdirectories in the database namespace."),
        new GranularPermission("ydb.granular.create_table",
            "Define new tables with specified schemas."),
        new GranularPermission("ydb.granular.remove_schema",
            "Remove schema objects (tables, directories, etc.)."),
        new GranularPermission("ydb.granular.describe_schema",
            "View schema definitions and structure."),
        new GranularPermission("ydb.granular.alter_schema",
            "Modify existing schema objects."),
    };

    private final String subject;
    private final Set<String> permissions;
    private final Set<String> effectivePermissions;
    private final Map<String, Button> groupToggles = new LinkedHashMap<>();
    private final Map<String, Button> granularToggles = new LinkedHashMap<>();

    YDBPermissionEditDialog(Shell parentShell, String subject,
                            Set<String> currentPermissions, Set<String> effectivePermissions) {
        super(parentShell);
        this.subject = subject;
        this.permissions = new LinkedHashSet<>(currentPermissions);
        this.effectivePermissions = effectivePermissions != null ? effectivePermissions : Collections.emptySet();
        setShellStyle(getShellStyle() | SWT.RESIZE);
    }

    @Override
    protected void configureShell(Shell newShell) {
        super.configureShell(newShell);
        newShell.setSize(550, 600);
    }

    @Override
    protected Control createDialogArea(Composite parent) {
        setTitle(subject);
        setMessage("Please note that granular rights can be combined into groups");

        Composite area = (Composite) super.createDialogArea(parent);

        TabFolder tabFolder = new TabFolder(area, SWT.NONE);
        tabFolder.setLayoutData(new GridData(GridData.FILL_BOTH));

        createGroupsTab(tabFolder);
        createGranularTab(tabFolder);

        return area;
    }

    private void createGroupsTab(TabFolder tabFolder) {
        TabItem tab = new TabItem(tabFolder, SWT.NONE);
        tab.setText("Groups");

        ScrolledComposite scrolled = new ScrolledComposite(tabFolder, SWT.V_SCROLL);
        scrolled.setExpandHorizontal(true);
        scrolled.setExpandVertical(true);

        Composite content = new Composite(scrolled, SWT.NONE);
        GridLayout layout = new GridLayout(1, false);
        layout.marginWidth = 10;
        layout.marginHeight = 10;
        layout.verticalSpacing = 15;
        content.setLayout(layout);

        for (GroupPermission gp : GROUP_PERMISSIONS) {
            createGroupEntry(content, gp);
        }

        scrolled.setContent(content);
        scrolled.addListener(SWT.Resize, e -> {
            int width = scrolled.getClientArea().width;
            scrolled.setMinSize(content.computeSize(width, SWT.DEFAULT));
        });
        tab.setControl(scrolled);
    }

    private void createGroupEntry(Composite parent, GroupPermission gp) {
        Composite row = new Composite(parent, SWT.NONE);
        row.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        GridLayout rowLayout = new GridLayout(2, false);
        rowLayout.marginWidth = 0;
        row.setLayout(rowLayout);

        // Left side: name + description + sub-perms
        Composite info = new Composite(row, SWT.NONE);
        info.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        GridLayout infoLayout = new GridLayout(1, false);
        infoLayout.marginWidth = 0;
        infoLayout.marginHeight = 0;
        infoLayout.verticalSpacing = 2;
        info.setLayout(infoLayout);

        Label nameLabel = new Label(info, SWT.NONE);
        nameLabel.setText(gp.name);
        nameLabel.setFont(getBoldFont(nameLabel));

        Label descLabel = new Label(info, SWT.WRAP);
        descLabel.setText(gp.description);
        GridData descGD = new GridData(GridData.FILL_HORIZONTAL);
        descGD.widthHint = 350;
        descLabel.setLayoutData(descGD);

        if (gp.subPermissions.length > 0) {
            Label subLabel = new Label(info, SWT.WRAP);
            subLabel.setText(String.join("   ", gp.subPermissions));
            subLabel.setForeground(parent.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
        }

        // Right side: toggle
        String permName = "ydb.generic." + gp.name;
        if ("grant".equals(gp.name)) {
            permName = "ydb.access.grant";
        }

        Button toggle = new Button(row, SWT.CHECK);
        toggle.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));

        boolean isExplicit = permissions.contains(permName);
        boolean isEffective = effectivePermissions.contains(permName);

        if (isExplicit) {
            toggle.setSelection(true);
        } else if (isEffective) {
            toggle.setSelection(true);
            toggle.setGrayed(true);
        }

        String finalPermName = permName;
        toggle.addListener(SWT.Selection, e -> {
            if (toggle.getSelection()) {
                permissions.add(finalPermName);
            } else {
                permissions.remove(finalPermName);
            }
            toggle.setGrayed(false);
        });

        groupToggles.put(permName, toggle);
    }

    private void createGranularTab(TabFolder tabFolder) {
        TabItem tab = new TabItem(tabFolder, SWT.NONE);
        tab.setText("Granular");

        ScrolledComposite scrolled = new ScrolledComposite(tabFolder, SWT.V_SCROLL);
        scrolled.setExpandHorizontal(true);
        scrolled.setExpandVertical(true);

        Composite content = new Composite(scrolled, SWT.NONE);
        GridLayout layout = new GridLayout(1, false);
        layout.marginWidth = 10;
        layout.marginHeight = 10;
        layout.verticalSpacing = 15;
        content.setLayout(layout);

        for (GranularPermission gp : GRANULAR_PERMISSIONS) {
            createGranularEntry(content, gp);
        }

        scrolled.setContent(content);
        scrolled.addListener(SWT.Resize, e -> {
            int width = scrolled.getClientArea().width;
            scrolled.setMinSize(content.computeSize(width, SWT.DEFAULT));
        });
        tab.setControl(scrolled);
    }

    private void createGranularEntry(Composite parent, GranularPermission gp) {
        Composite row = new Composite(parent, SWT.NONE);
        row.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        GridLayout rowLayout = new GridLayout(2, false);
        rowLayout.marginWidth = 0;
        row.setLayout(rowLayout);

        Composite info = new Composite(row, SWT.NONE);
        info.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        GridLayout infoLayout = new GridLayout(1, false);
        infoLayout.marginWidth = 0;
        infoLayout.marginHeight = 0;
        infoLayout.verticalSpacing = 2;
        info.setLayout(infoLayout);

        // Display short name (e.g. "select_row" from "ydb.granular.select_row")
        String shortName = gp.permissionName;
        int lastDot = shortName.lastIndexOf('.');
        if (lastDot >= 0) shortName = shortName.substring(lastDot + 1);

        Label nameLabel = new Label(info, SWT.NONE);
        nameLabel.setText(shortName);
        nameLabel.setFont(getBoldFont(nameLabel));

        Label descLabel = new Label(info, SWT.WRAP);
        descLabel.setText(gp.description);
        GridData descGD = new GridData(GridData.FILL_HORIZONTAL);
        descGD.widthHint = 350;
        descLabel.setLayoutData(descGD);

        Button toggle = new Button(row, SWT.CHECK);
        toggle.setLayoutData(new GridData(SWT.END, SWT.CENTER, false, false));

        boolean isExplicit = permissions.contains(gp.permissionName);
        boolean isEffective = effectivePermissions.contains(gp.permissionName);

        if (isExplicit) {
            toggle.setSelection(true);
        } else if (isEffective) {
            toggle.setSelection(true);
            toggle.setGrayed(true);
        }

        toggle.addListener(SWT.Selection, e -> {
            if (toggle.getSelection()) {
                permissions.add(gp.permissionName);
            } else {
                permissions.remove(gp.permissionName);
            }
            toggle.setGrayed(false);
        });

        granularToggles.put(gp.permissionName, toggle);
    }

    private org.eclipse.swt.graphics.Font getBoldFont(Control control) {
        org.eclipse.swt.graphics.FontData[] fd = control.getFont().getFontData();
        for (org.eclipse.swt.graphics.FontData f : fd) {
            f.setStyle(f.getStyle() | SWT.BOLD);
        }
        return new org.eclipse.swt.graphics.Font(control.getDisplay(), fd);
    }

    @Override
    protected void createButtonsForButtonBar(Composite parent) {
        createButton(parent, 2, "Discard Changes", false)
            .addListener(SWT.Selection, e -> {
                // Reset to original
                setReturnCode(CANCEL);
                close();
            });
        super.createButtonsForButtonBar(parent);
    }

    Set<String> getPermissions() {
        return permissions;
    }

    private static class GroupPermission {
        final String name;
        final String description;
        final String[] subPermissions;

        GroupPermission(String name, String description, String[] subPermissions) {
            this.name = name;
            this.description = description;
            this.subPermissions = subPermissions;
        }
    }

    private static class GranularPermission {
        final String permissionName;
        final String description;

        GranularPermission(String permissionName, String description) {
            this.permissionName = permissionName;
            this.description = description;
        }
    }
}
