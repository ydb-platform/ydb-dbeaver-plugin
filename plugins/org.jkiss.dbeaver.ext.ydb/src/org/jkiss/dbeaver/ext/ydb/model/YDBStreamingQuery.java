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
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBPToolTipObject;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.util.Map;

/**
 * YDB Streaming Query.
 * Represents an active streaming query from .sys/streaming_queries system view.
 * The Path field contains a hierarchical path (e.g. "folder/subfolder/query_name").
 */
public class YDBStreamingQuery implements DBSObject, DBPToolTipObject, DBPScriptObject {

    private final DBSObject parent;
    private final String name;
    private final String fullPath;
    private final String status;
    private final String issues;
    private final String plan;
    private final String ast;
    private final String queryText;
    private final String run;
    private final String resourcePool;
    private final String retryCount;
    private final String lastFailAt;
    private final String suspendedUntil;

    public YDBStreamingQuery(
        @NotNull DBSObject parent,
        @NotNull String name,
        @NotNull String fullPath,
        @Nullable String status,
        @Nullable String issues,
        @Nullable String plan,
        @Nullable String ast,
        @Nullable String queryText,
        @Nullable String run,
        @Nullable String resourcePool,
        @Nullable String retryCount,
        @Nullable String lastFailAt,
        @Nullable String suspendedUntil
    ) {
        this.parent = parent;
        this.name = name;
        this.fullPath = fullPath;
        this.status = status;
        this.issues = issues;
        this.plan = plan;
        this.ast = ast;
        this.queryText = queryText;
        this.run = run;
        this.resourcePool = resourcePool;
        this.retryCount = retryCount;
        this.lastFailAt = lastFailAt;
        this.suspendedUntil = suspendedUntil;
    }

    /**
     * Copy constructor with a different parent and short name.
     */
    public YDBStreamingQuery(@NotNull DBSObject parent, @NotNull String name, @NotNull YDBStreamingQuery source) {
        this(
            parent, name, source.fullPath,
            source.status, source.issues, source.plan, source.ast,
            source.queryText, source.run, source.resourcePool,
            source.retryCount, source.lastFailAt, source.suspendedUntil
        );
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
    @Property(viewable = true, order = 3)
    public String getStatus() {
        return status;
    }

    @Nullable
    @Property(order = 4)
    public String getQueryText() {
        return queryText;
    }

    @Nullable
    @Property(order = 5)
    public String getResourcePool() {
        return resourcePool;
    }

    @Nullable
    @Property(order = 6)
    public String getRun() {
        return run;
    }

    @Nullable
    @Property(order = 7)
    public String getRetryCount() {
        return retryCount;
    }

    @Nullable
    @Property(order = 8)
    public String getLastFailAt() {
        return lastFailAt;
    }

    @Nullable
    @Property(order = 9)
    public String getSuspendedUntil() {
        return suspendedUntil;
    }

    @Nullable
    public String getIssues() {
        return issues;
    }

    @Nullable
    public String getPlan() {
        return plan;
    }

    @Nullable
    public String getAst() {
        return ast;
    }

    @NotNull
    @Override
    public String getObjectDefinitionText(@NotNull DBRProgressMonitor monitor, @NotNull Map<String, Object> options) {
        if (queryText == null || queryText.isEmpty()) {
            return "";
        }
        return "CREATE STREAMING QUERY `" + fullPath + "` AS\nDO BEGIN\n" + queryText + "\nEND DO";
    }

    @Nullable
    @Override
    public String getDescription() {
        return null;
    }

    @Nullable
    @Override
    public String getObjectToolTip() {
        if (status != null && !"RUNNING".equalsIgnoreCase(status)) {
            return status;
        }
        return null;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return parent.getDataSource();
    }

    @Nullable
    @Override
    public DBSObject getParentObject() {
        return parent;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    /**
     * Check if the Path contains "/" separators (i.e. has a hierarchical path).
     */
    public boolean hasHierarchicalPath() {
        return fullPath.contains("/");
    }
}
