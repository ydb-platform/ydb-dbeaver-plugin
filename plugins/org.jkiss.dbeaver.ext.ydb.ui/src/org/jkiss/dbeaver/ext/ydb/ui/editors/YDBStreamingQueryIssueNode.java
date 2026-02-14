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

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single issue node in a YDB streaming query issues tree.
 */
public class YDBStreamingQueryIssueNode {

    private final YDBStreamingQueryIssueNode parent;
    private final int severity;
    private final String message;
    private final List<YDBStreamingQueryIssueNode> children = new ArrayList<>();

    public YDBStreamingQueryIssueNode(
        @Nullable YDBStreamingQueryIssueNode parent,
        int severity,
        @NotNull String message
    ) {
        this.parent = parent;
        this.severity = severity;
        this.message = message;
    }

    @Nullable
    public YDBStreamingQueryIssueNode getParent() {
        return parent;
    }

    public int getSeverity() {
        return severity;
    }

    @NotNull
    public String getSeverityLabel() {
        switch (severity) {
            case 1:
                return "INFO";
            case 2:
                return "WARNING";
            case 3:
                return "ERROR";
            case 4:
                return "FATAL";
            default:
                return "UNKNOWN(" + severity + ")";
        }
    }

    @NotNull
    public String getMessage() {
        return message;
    }

    @NotNull
    public List<YDBStreamingQueryIssueNode> getChildren() {
        return children;
    }

    public boolean hasChildren() {
        return !children.isEmpty();
    }

    public void addChild(@NotNull YDBStreamingQueryIssueNode child) {
        children.add(child);
    }
}
