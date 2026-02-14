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
 * Virtual folder for hierarchical streaming query organization.
 * Streaming queries with "/" in their Path are organized into nested folders.
 */
public class YDBStreamingQueryFolder implements DBSFolder {

    private final DBSObject owner;
    private final YDBStreamingQueryFolder parentFolder;
    private final String name;
    private final String fullPath;
    private final Map<String, YDBStreamingQueryFolder> subFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final List<YDBStreamingQuery> queries = new ArrayList<>();

    public YDBStreamingQueryFolder(
        @NotNull DBSObject owner,
        @Nullable YDBStreamingQueryFolder parentFolder,
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
        children.addAll(queries);
        return children;
    }

    @Association
    @NotNull
    public Collection<YDBStreamingQueryFolder> getSubFolders() {
        return subFolders.values();
    }

    @Association
    @NotNull
    public List<YDBStreamingQuery> getQueries() {
        queries.sort(Comparator.comparing(YDBStreamingQuery::getName, String.CASE_INSENSITIVE_ORDER));
        return queries;
    }

    public void addQuery(@NotNull YDBStreamingQuery query) {
        queries.add(query);
    }

    @NotNull
    public YDBStreamingQueryFolder getOrCreateSubFolder(@NotNull String folderName) {
        return subFolders.computeIfAbsent(folderName, n -> new YDBStreamingQueryFolder(owner, this, n));
    }

    @Override
    public String toString() {
        return name;
    }
}
