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
package org.jkiss.dbeaver.ext.ydb.model.plan;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNodeKind;
import org.jkiss.dbeaver.model.impl.PropertyDescriptor;
import org.jkiss.dbeaver.model.impl.plan.AbstractExecutionPlanNode;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.jkiss.dbeaver.model.preferences.DBPPropertySource;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import java.util.*;

/**
 * Represents a node in a YDB execution plan tree.
 * Reusable plan node model that can be used for both streaming queries and regular query plans.
 */
public class YDBPlanNode extends AbstractExecutionPlanNode implements DBPPropertySource {

    private final YDBPlanNode parent;
    private final String nodeType;
    private final String nodeName;
    private final String operators;
    private final Map<String, String> attributes;
    private final List<YDBPlanNode> children = new ArrayList<>();

    public YDBPlanNode(
        @Nullable YDBPlanNode parent,
        @NotNull String nodeType,
        @Nullable String nodeName,
        @Nullable String operators,
        @NotNull Map<String, String> attributes
    ) {
        this.parent = parent;
        this.nodeType = nodeType;
        this.nodeName = nodeName;
        this.operators = operators;
        this.attributes = attributes;
    }

    @NotNull
    @Override
    @Property(order = 0, viewable = true)
    public String getNodeType() {
        return nodeType;
    }

    @Nullable
    @Override
    @Property(order = 1, viewable = true)
    public String getNodeName() {
        return nodeName;
    }

    @Nullable
    @Property(order = 2, viewable = true)
    public String getOperators() {
        return operators;
    }

    @NotNull
    @Override
    public DBCPlanNodeKind getNodeKind() {
        if (nodeType == null) {
            return DBCPlanNodeKind.DEFAULT;
        }
        String type = nodeType.toLowerCase(Locale.ROOT);
        if (type.contains("source") || type.contains("tablescan") || type.contains("table scan")
            || type.contains("readtable") || type.contains("read table")) {
            return DBCPlanNodeKind.TABLE_SCAN;
        } else if (type.contains("index") || type.contains("indexscan") || type.contains("index scan")) {
            return DBCPlanNodeKind.INDEX_SCAN;
        } else if (type.contains("upsert") || type.contains("sink") || type.contains("write")
            || type.contains("modify") || type.contains("delete") || type.contains("insert")
            || type.contains("update")) {
            return DBCPlanNodeKind.MODIFY;
        } else if (type.contains("hash")) {
            return DBCPlanNodeKind.HASH;
        } else if (type.contains("join")) {
            return DBCPlanNodeKind.JOIN;
        } else if (type.contains("sort") || type.contains("order")) {
            return DBCPlanNodeKind.SORT;
        } else if (type.contains("filter") || type.contains("where")) {
            return DBCPlanNodeKind.FILTER;
        } else if (type.contains("aggregate") || type.contains("agg")) {
            return DBCPlanNodeKind.AGGREGATE;
        } else if (type.contains("union")) {
            return DBCPlanNodeKind.UNION;
        } else if (type.contains("merge")) {
            return DBCPlanNodeKind.MERGE;
        } else if (type.contains("group")) {
            return DBCPlanNodeKind.GROUP;
        } else if (type.contains("result")) {
            return DBCPlanNodeKind.RESULT;
        } else if (type.contains("materialize")) {
            return DBCPlanNodeKind.MATERIALIZE;
        }
        return DBCPlanNodeKind.DEFAULT;
    }

    @Nullable
    @Override
    public DBCPlanNode getParent() {
        return parent;
    }

    @NotNull
    @Override
    public Collection<YDBPlanNode> getNested() {
        return children;
    }

    public void addChild(@NotNull YDBPlanNode child) {
        children.add(child);
    }

    // DBPPropertySource implementation

    @NotNull
    @Override
    public Object getEditableValue() {
        return this;
    }

    @NotNull
    @Override
    public DBPPropertyDescriptor[] getProperties() {
        List<DBPPropertyDescriptor> props = new ArrayList<>();
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            props.add(new PropertyDescriptor(
                "Details",
                attr.getKey(),
                attr.getKey(),
                null,
                String.class,
                false,
                null,
                null,
                false
            ));
        }
        return props.toArray(new DBPPropertyDescriptor[0]);
    }

    @Nullable
    @Override
    public Object getPropertyValue(@Nullable DBRProgressMonitor monitor, @NotNull String id) {
        return attributes.get(id);
    }

    @Override
    public boolean isPropertySet(@NotNull String id) {
        return attributes.containsKey(id);
    }

    @Override
    public boolean isPropertyResettable(@NotNull String id) {
        return false;
    }

    @Override
    public void resetPropertyValue(@Nullable DBRProgressMonitor monitor, @NotNull String id) {
        // Read-only
    }

    @Override
    public void resetPropertyValueToDefault(@NotNull String id) {
        // Read-only
    }

    @Override
    public void setPropertyValue(
        @Nullable DBRProgressMonitor monitor,
        @NotNull String id,
        @Nullable Object value
    ) {
        // Read-only
    }
}
