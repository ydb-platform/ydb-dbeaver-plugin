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
package org.jkiss.dbeaver.ext.ydb.model.autocomplete;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

public class AutocompleteEntity {

    private final String name;
    private final String type;
    private final String parent;

    public AutocompleteEntity(@NotNull String name, @Nullable String type, @Nullable String parent) {
        this.name = name;
        this.type = type;
        this.parent = parent;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @Nullable
    public String getType() {
        return type;
    }

    @Nullable
    public String getParent() {
        return parent;
    }
}
