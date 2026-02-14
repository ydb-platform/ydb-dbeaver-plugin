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
package org.jkiss.dbeaver.ext.ydb.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSFolder;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * YDB Tables container folder for schema.
 * This is a folder that contains all table folders and root tables within a schema.
 */
public class YDBSchemaTablesFolder implements DBSFolder {

    private final YDBSchema schema;
    private Map<String, YDBTableFolder> rootFolders;
    private List<GenericTableBase> rootTables;
    private boolean hierarchyBuilt = false;

    public YDBSchemaTablesFolder(@NotNull YDBSchema schema) {
        this.schema = schema;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return "Tables";
    }

    @Nullable
    @Override
    public String getDescription() {
        return "YDB Tables";
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Nullable
    @Override
    public DBSObject getParentObject() {
        return schema;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return schema.getDataSource();
    }

    /**
     * Build the folder hierarchy from flat table names.
     */
    private synchronized void buildHierarchy(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (hierarchyBuilt) {
            return;
        }

        rootFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        rootTables = new ArrayList<>();

        List<? extends GenericTableBase> allTables = schema.getAllTables(monitor);
        if (allTables == null) {
            hierarchyBuilt = true;
            return;
        }

        for (GenericTableBase table : allTables) {
            if (table instanceof YDBTable) {
                YDBTable ydbTable = (YDBTable) table;
                if (ydbTable.hasHierarchicalName()) {
                    String fullName = ydbTable.getFullTableName();
                    String[] parts = fullName.split("/");
                    YDBTableFolder currentFolder = null;

                    for (int i = 0; i < parts.length - 1; i++) {
                        String folderName = parts[i];
                        if (currentFolder == null) {
                            currentFolder = rootFolders.computeIfAbsent(
                                folderName,
                                n -> new YDBTableFolder(this, null, n)
                            );
                        } else {
                            currentFolder = currentFolder.getOrCreateSubFolder(folderName);
                        }
                    }

                    if (currentFolder != null) {
                        currentFolder.addTable(ydbTable);
                    }
                } else {
                    rootTables.add(ydbTable);
                }
            } else {
                // Non-YDB tables go to root
                rootTables.add(table);
            }
        }

        hierarchyBuilt = true;
    }

    @NotNull
    @Override
    public Collection<DBSObject> getChildrenObjects(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        List<DBSObject> children = new ArrayList<>();
        if (rootFolders != null) {
            children.addAll(rootFolders.values());
        }
        if (rootTables != null) {
            children.addAll(rootTables);
        }
        return children;
    }

    /**
     * Get root-level folders.
     */
    @Association
    @NotNull
    public Collection<YDBTableFolder> getTableFolders(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        return rootFolders != null ? rootFolders.values() : List.of();
    }

    /**
     * Get tables at the root level (without "/" in names).
     */
    @Association
    @NotNull
    public List<GenericTableBase> getRootTables(@NotNull DBRProgressMonitor monitor) throws DBException {
        buildHierarchy(monitor);
        if (rootTables != null) {
            rootTables.sort(Comparator.comparing(GenericTableBase::getName, String.CASE_INSENSITIVE_ORDER));
        }
        return rootTables != null ? rootTables : List.of();
    }

    public synchronized void refresh() {
        hierarchyBuilt = false;
        rootFolders = null;
        rootTables = null;
    }

    @Override
    public String toString() {
        return getName();
    }
}
