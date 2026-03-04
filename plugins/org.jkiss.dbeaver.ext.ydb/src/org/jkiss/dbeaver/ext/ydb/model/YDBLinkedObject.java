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
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.struct.DBSObject;

/**
 * Wrapper around a real DBSObject that overrides getName() to show a custom display path,
 * while delegating equals()/hashCode() to the real object so that navigator node lookup works.
 */
class YDBLinkedObject implements DBSObject {

    private final DBSObject real;
    private final String displayPath;

    YDBLinkedObject(@NotNull DBSObject real, @NotNull String displayPath) {
        this.real = real;
        this.displayPath = displayPath;
    }

    @NotNull
    @Override
    public String getName() {
        return displayPath;
    }

    @Nullable
    @Override
    public String getDescription() {
        return real.getDescription();
    }

    @Override
    public DBSObject getParentObject() {
        return real.getParentObject();
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return real.getDataSource();
    }

    @Override
    public boolean isPersisted() {
        return real.isPersisted();
    }

    @Override
    public int hashCode() {
        return real.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YDBLinkedObject other) {
            return real.equals(other.real);
        }
        return real.equals(obj);
    }

    @Override
    public String toString() {
        return displayPath;
    }
}
