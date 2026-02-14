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
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.util.Collection;
import java.util.List;

/**
 * YDB Catalog with hierarchical folder support.
 * Tables with "/" in their names are organized into virtual folders.
 */
public class YDBCatalog extends GenericCatalog {

    private YDBCatalogTablesFolder tablesFolder;

    public YDBCatalog(@NotNull GenericDataSource dataSource, @NotNull String catalogName) {
        super(dataSource, catalogName);
    }

    /**
     * Get the Tables folder container.
     */
    @Association
    @NotNull
    public YDBCatalogTablesFolder getTablesFolder(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (tablesFolder == null) {
            tablesFolder = new YDBCatalogTablesFolder(this);
        }
        return tablesFolder;
    }

    /**
     * Get all tables for internal use by YDBCatalogTablesFolder.
     */
    public List<? extends GenericTableBase> getAllTables(@NotNull DBRProgressMonitor monitor) throws DBException {
        return super.getTables(monitor);
    }

    @Override
    public Collection<? extends DBSObject> getChildren(@NotNull DBRProgressMonitor monitor) throws DBException {
        return List.of(getTablesFolder(monitor));
    }

    @Override
    public DBSObject getChild(@NotNull DBRProgressMonitor monitor, @NotNull String childName) throws DBException {
        if ("Tables".equalsIgnoreCase(childName)) {
            return getTablesFolder(monitor);
        }
        return null;
    }

    @Override
    public synchronized DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (tablesFolder != null) {
            tablesFolder.refresh();
        }
        return super.refreshObject(monitor);
    }
}
