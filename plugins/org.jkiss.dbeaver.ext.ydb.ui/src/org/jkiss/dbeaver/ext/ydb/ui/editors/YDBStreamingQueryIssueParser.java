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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses YDB streaming query issues JSON into a tree of {@link YDBStreamingQueryIssueNode}.
 */
public class YDBStreamingQueryIssueParser {

    private static final Log log = Log.getLog(YDBStreamingQueryIssueParser.class);

    private YDBStreamingQueryIssueParser() {
    }

    @NotNull
    public static List<YDBStreamingQueryIssueNode> parse(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            JsonElement root = JsonParser.parseString(json);
            if (root.isJsonArray()) {
                return parseArray(null, root.getAsJsonArray());
            } else if (root.isJsonObject()) {
                YDBStreamingQueryIssueNode node = parseNode(null, root.getAsJsonObject());
                if (node != null) {
                    return Collections.singletonList(node);
                }
            }
        } catch (Exception e) {
            log.debug("Failed to parse issues JSON", e);
        }
        return Collections.emptyList();
    }

    @NotNull
    private static List<YDBStreamingQueryIssueNode> parseArray(
        @Nullable YDBStreamingQueryIssueNode parent,
        @NotNull JsonArray array
    ) {
        List<YDBStreamingQueryIssueNode> result = new ArrayList<>();
        for (JsonElement element : array) {
            if (element.isJsonObject()) {
                YDBStreamingQueryIssueNode node = parseNode(parent, element.getAsJsonObject());
                if (node != null) {
                    result.add(node);
                }
            }
        }
        return result;
    }

    @Nullable
    private static YDBStreamingQueryIssueNode parseNode(
        @Nullable YDBStreamingQueryIssueNode parent,
        @NotNull JsonObject obj
    ) {
        int severity = 0;
        if (obj.has("severity")) {
            severity = obj.get("severity").getAsInt();
        }
        String message = "";
        if (obj.has("message")) {
            message = obj.get("message").getAsString();
        }
        YDBStreamingQueryIssueNode node = new YDBStreamingQueryIssueNode(parent, severity, message);
        if (obj.has("issues") && obj.get("issues").isJsonArray()) {
            List<YDBStreamingQueryIssueNode> children = parseArray(node, obj.getAsJsonArray("issues"));
            for (YDBStreamingQueryIssueNode child : children) {
                node.addChild(child);
            }
        }
        return node;
    }
}
