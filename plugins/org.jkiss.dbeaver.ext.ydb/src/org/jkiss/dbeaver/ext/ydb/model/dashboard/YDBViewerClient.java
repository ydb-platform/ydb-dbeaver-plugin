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
package org.jkiss.dbeaver.ext.ydb.model.dashboard;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.jkiss.utils.CommonUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class YDBViewerClient {

    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 10000;
    private static final Pattern JDBC_URL_PATTERN = Pattern.compile(
        "jdbc:ydb:(grpcs?)://([^:/]+)(?::(\\d+))?(/.*?)(?:\\?.*)?$"
    );

    private final String baseUrl;
    private final String database;
    private final String authToken;

    public YDBViewerClient(String baseUrl, String database, String authToken) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.database = database;
        this.authToken = authToken;
    }

    public static String resolveBaseUrl(String explicitUrl, String jdbcUrl, String hostName, boolean secure) {
        if (!CommonUtils.isEmpty(explicitUrl)) {
            return explicitUrl;
        }
        // Try parsing JDBC URL first
        if (!CommonUtils.isEmpty(jdbcUrl)) {
            Matcher matcher = JDBC_URL_PATTERN.matcher(jdbcUrl);
            if (matcher.find()) {
                String scheme = "grpcs".equals(matcher.group(1)) ? "https" : "http";
                String host = matcher.group(2);
                return scheme + "://" + host + ":8765";
            }
        }
        // Fallback: build from host name and secure flag
        if (!CommonUtils.isEmpty(hostName)) {
            String scheme = secure ? "https" : "http";
            return scheme + "://" + hostName + ":8765";
        }
        return null;
    }

    public YDBDatabaseLoadInfo fetchDatabaseLoad() {
        YDBDatabaseLoadInfo info = new YDBDatabaseLoadInfo();
        try {
            parseClusterInfo(info);
            parseNodesInfo(info);
        } catch (Exception e) {
            info.setErrorMessage(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
        return info;
    }

    private void parseClusterInfo(YDBDatabaseLoadInfo info) throws Exception {
        String json = httpGet(baseUrl + "/viewer/json/cluster");
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        // All metrics are at the top level of the response
        if (root.has("CoresTotal")) {
            info.setCoresTotal(root.get("CoresTotal").getAsDouble());
        }
        if (root.has("CoresUsed")) {
            info.setCoresUsed(root.get("CoresUsed").getAsDouble());
        }
        // MemoryTotal and MemoryUsed are strings in the JSON
        if (root.has("MemoryTotal")) {
            info.setMemoryTotal(Long.parseLong(root.get("MemoryTotal").getAsString()));
        }
        if (root.has("MemoryUsed")) {
            info.setMemoryUsed(Long.parseLong(root.get("MemoryUsed").getAsString()));
        }
        // StorageTotal and StorageUsed are strings in the JSON
        if (root.has("StorageTotal")) {
            info.setStorageTotal(Long.parseLong(root.get("StorageTotal").getAsString()));
        }
        if (root.has("StorageUsed")) {
            info.setStorageUsed(Long.parseLong(root.get("StorageUsed").getAsString()));
        }
        if (root.has("NodesTotal")) {
            info.setNodesTotal(root.get("NodesTotal").getAsInt());
        }
        if (root.has("NodesAlive")) {
            info.setNodesAlive(root.get("NodesAlive").getAsInt());
        }
        if (root.has("Overall")) {
            info.setOverallStatus(root.get("Overall").getAsString());
        }
        // NetworkWriteThroughput is a string (bytes per second)
        if (root.has("NetworkWriteThroughput")) {
            info.setNetworkBytesPerSec(Double.parseDouble(root.get("NetworkWriteThroughput").getAsString()));
        }
    }

    private void parseNodesInfo(YDBDatabaseLoadInfo info) throws Exception {
        String encodedDb = URLEncoder.encode(database, StandardCharsets.UTF_8);
        String json = httpGet(baseUrl + "/viewer/json/nodes?database=" + encodedDb);
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        List<YDBNodeInfo> nodes = new ArrayList<>();
        if (root.has("Nodes")) {
            JsonArray nodesArr = root.getAsJsonArray("Nodes");
            for (JsonElement nodeEl : nodesArr) {
                JsonObject node = nodeEl.getAsJsonObject();
                int nodeId = node.has("NodeId") ? node.get("NodeId").getAsInt() : 0;
                String host = node.has("Host") ? node.get("Host").getAsString() : "";
                String version = "";
                double loadAverage = 0;
                int numberOfCpus = 1;
                long memUsed = 0;
                long memLimit = 0;

                if (node.has("SystemState")) {
                    JsonObject ss = node.getAsJsonObject("SystemState");
                    if (host.isEmpty() && ss.has("Host")) {
                        host = ss.get("Host").getAsString();
                    }
                    if (ss.has("Version")) {
                        version = ss.get("Version").getAsString();
                    }
                    if (ss.has("LoadAverage")) {
                        JsonArray loadArr = ss.getAsJsonArray("LoadAverage");
                        if (loadArr.size() > 0) {
                            loadAverage = loadArr.get(0).getAsDouble();
                        }
                    }
                    if (ss.has("NumberOfCpus")) {
                        numberOfCpus = ss.get("NumberOfCpus").getAsInt();
                    }
                    if (ss.has("MemoryUsed")) {
                        memUsed = ss.get("MemoryUsed").getAsLong();
                    }
                    if (ss.has("MemoryLimit")) {
                        memLimit = ss.get("MemoryLimit").getAsLong();
                    }
                }

                nodes.add(new YDBNodeInfo(nodeId, host, version, loadAverage, numberOfCpus, memUsed, memLimit));
            }
        }

        // Sort by load percent descending
        nodes.sort((a, b) -> Double.compare(b.getLoadPercent(), a.getLoadPercent()));
        info.setNodes(nodes);
    }

    private static final int MAX_REDIRECTS = 5;

    private String httpGet(String urlString) throws Exception {
        String currentUrl = urlString;
        for (int i = 0; i < MAX_REDIRECTS; i++) {
            URI uri = URI.create(currentUrl);
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            try {
                connection.setInstanceFollowRedirects(false);
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
                connection.setReadTimeout(READ_TIMEOUT_MS);
                connection.setRequestProperty("Accept", "application/json");
                if (!CommonUtils.isEmpty(authToken)) {
                    connection.setRequestProperty("Authorization", "OAuth " + authToken);
                }

                int responseCode = connection.getResponseCode();
                if (responseCode == 301 || responseCode == 302
                        || responseCode == 303 || responseCode == 307 || responseCode == 308) {
                    String location = connection.getHeaderField("Location");
                    if (CommonUtils.isEmpty(location)) {
                        throw new Exception("Redirect without Location from " + currentUrl);
                    }
                    if (location.startsWith("/")) {
                        // Relative redirect — resolve against current URL
                        URI base = URI.create(currentUrl);
                        currentUrl = base.getScheme() + "://" + base.getAuthority() + location;
                    } else {
                        currentUrl = location;
                    }
                    continue;
                }

                if (responseCode != 200) {
                    throw new Exception("HTTP " + responseCode + " from " + currentUrl);
                }

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    return sb.toString();
                }
            } finally {
                connection.disconnect();
            }
        }
        throw new Exception("Too many redirects from " + urlString);
    }
}
