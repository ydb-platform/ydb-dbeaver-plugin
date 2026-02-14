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
 * Virtual folder for hierarchical external data source organization.
 */
public class YDBExternalDataSourceFolder implements DBSFolder {

    private final DBSObject owner;
    private final YDBExternalDataSourceFolder parentFolder;
    private final String name;
    private final String fullPath;
    private final Map<String, YDBExternalDataSourceFolder> subFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final List<YDBExternalDataSource> entries = new ArrayList<>();

    public YDBExternalDataSourceFolder(
        @NotNull DBSObject owner,
        @Nullable YDBExternalDataSourceFolder parentFolder,
        @NotNull String name
    ) {
        this.owner = owner;
        this.parentFolder = parentFolder;
        this.name = name;
        this.fullPath = parentFolder != null ? parentFolder.getFullPath() + "/" + name : name;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return name;
    }

    @NotNull
    public String getFullPath() {
        return fullPath;
    }

    @Nullable
    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Nullable
    @Override
    public DBSObject getParentObject() {
        return parentFolder != null ? parentFolder : owner;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return owner.getDataSource();
    }

    @NotNull
    @Override
    public Collection<DBSObject> getChildrenObjects(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<DBSObject> children = new ArrayList<>();
        children.addAll(subFolders.values());
        children.addAll(entries);
        return children;
    }

    @Association
    @NotNull
    public Collection<YDBExternalDataSourceFolder> getSubFolders() {
        return subFolders.values();
    }

    @Association
    @NotNull
    public List<YDBExternalDataSource> getExternalDataSources() {
        entries.sort(Comparator.comparing(YDBExternalDataSource::getName, String.CASE_INSENSITIVE_ORDER));
        return entries;
    }

    public void addEntry(@NotNull YDBExternalDataSource entry) {
        entries.add(entry);
    }

    @NotNull
    public YDBExternalDataSourceFolder getOrCreateSubFolder(@NotNull String folderName) {
        return subFolders.computeIfAbsent(folderName, n -> new YDBExternalDataSourceFolder(owner, this, n));
    }

    @Override
    public String toString() {
        return name;
    }
}
