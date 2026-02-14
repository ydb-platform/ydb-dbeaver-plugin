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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.Log;

import java.util.*;

/**
 * Parses YDB execution plan JSON into a tree of {@link YDBPlanNode}.
 */
public class YDBPlanParser {

    private static final Log log = Log.getLog(YDBPlanParser.class);

    private YDBPlanParser() {
    }

    @NotNull
    public static List<YDBPlanNode> parse(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            JsonElement root = JsonParser.parseString(json);
            if (!root.isJsonObject()) {
                return Collections.emptyList();
            }
            JsonObject rootObj = root.getAsJsonObject();
            // YDB plan JSON may have a top-level "Plan" key
            if (rootObj.has("Plan") && rootObj.get("Plan").isJsonObject()) {
                rootObj = rootObj.getAsJsonObject("Plan");
            }
            YDBPlanNode node = parseNode(null, rootObj);
            if (node != null) {
                return Collections.singletonList(node);
            }
        } catch (Exception e) {
            log.debug("Failed to parse plan JSON", e);
        }
        return Collections.emptyList();
    }

    @Nullable
    private static YDBPlanNode parseNode(@Nullable YDBPlanNode parent, @NotNull JsonObject obj) {
        String nodeType = getStringField(obj, "Node Type");
        if (nodeType == null) {
            nodeType = getStringField(obj, "PlanNodeType");
        }
        if (nodeType == null) {
            nodeType = "Unknown";
        }

        // Extract table names
        String nodeName = null;
        if (obj.has("Tables") && obj.get("Tables").isJsonArray()) {
            JsonArray tables = obj.getAsJsonArray("Tables");
            StringBuilder sb = new StringBuilder();
            for (JsonElement t : tables) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(t.getAsString());
            }
            nodeName = sb.toString();
        }
        if (nodeName == null) {
            nodeName = getStringField(obj, "Table");
        }

        // Extract operators
        String operators = null;
        if (obj.has("Operators") && obj.get("Operators").isJsonArray()) {
            JsonArray ops = obj.getAsJsonArray("Operators");
            StringBuilder sb = new StringBuilder();
            for (JsonElement opElem : ops) {
                if (opElem.isJsonObject()) {
                    JsonObject opObj = opElem.getAsJsonObject();
                    String opName = getStringField(opObj, "Name");
                    if (opName != null) {
                        if (sb.length() > 0) {
                            sb.append(", ");
                        }
                        sb.append(opName);
                    }
                }
            }
            if (sb.length() > 0) {
                operators = sb.toString();
            }
        }

        // Collect all attributes for property panel
        Map<String, String> attributes = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            String key = entry.getKey();
            if ("Plans".equals(key) || "Operators".equals(key)) {
                continue;
            }
            JsonElement value = entry.getValue();
            if (value.isJsonPrimitive()) {
                attributes.put(key, value.getAsString());
            } else {
                attributes.put(key, value.toString());
            }
        }

        // Add stats if present
        if (obj.has("Stats") && obj.get("Stats").isJsonObject()) {
            JsonObject stats = obj.getAsJsonObject("Stats");
            for (Map.Entry<String, JsonElement> entry : stats.entrySet()) {
                String statKey = "Stats." + entry.getKey();
                JsonElement value = entry.getValue();
                if (value.isJsonPrimitive()) {
                    attributes.put(statKey, value.getAsString());
                } else {
                    attributes.put(statKey, value.toString());
                }
            }
        }

        YDBPlanNode node = new YDBPlanNode(parent, nodeType, nodeName, operators, attributes);

        // Parse child plans
        if (obj.has("Plans") && obj.get("Plans").isJsonArray()) {
            JsonArray plans = obj.getAsJsonArray("Plans");
            for (JsonElement childElem : plans) {
                if (childElem.isJsonObject()) {
                    YDBPlanNode child = parseNode(node, childElem.getAsJsonObject());
                    if (child != null) {
                        node.addChild(child);
                    }
                }
            }
        }

        return node;
    }

    @Nullable
    private static String getStringField(@NotNull JsonObject obj, @NotNull String field) {
        if (obj.has(field) && obj.get(field).isJsonPrimitive()) {
            return obj.get(field).getAsString();
        }
        return null;
    }
}
