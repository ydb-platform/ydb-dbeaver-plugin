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
import java.io.OutputStream;
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
    private static final long SESSION_TTL_MS = 60L * 60L * 1000L; // 1 hour
    private static final String SESSION_COOKIE_NAME = "ydb_session_id";
    private static final Pattern JDBC_URL_PATTERN = Pattern.compile(
        "jdbc:ydb:(grpcs?)://([^:/]+)(?::(\\d+))?(/.*?)(?:\\?.*)?$"
    );
    private static final Pattern SESSION_COOKIE_PATTERN = Pattern.compile(
        "(?:^|[;,\\s])" + Pattern.quote(SESSION_COOKIE_NAME) + "=([^;,\\s]+)"
    );

    private final String baseUrl;
    private final String database;
    private final String authToken;
    private final String user;
    private final String password;

    private final Object sessionLock = new Object();
    private volatile String sessionCookie;
    private volatile long sessionExpiresAt;

    public YDBViewerClient(String baseUrl, String database, String authToken) {
        this(baseUrl, database, authToken, null, null);
    }

    public YDBViewerClient(String baseUrl, String database, String authToken, String user, String password) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.database = database;
        this.authToken = authToken;
        this.user = user;
        this.password = password;
    }

    public static String resolveBaseUrl(String explicitUrl, String jdbcUrl, String hostName, boolean secure) {
        if (!CommonUtils.isEmpty(explicitUrl)) {
            return explicitUrl;
        }
        if (!CommonUtils.isEmpty(jdbcUrl)) {
            Matcher matcher = JDBC_URL_PATTERN.matcher(jdbcUrl);
            if (matcher.find()) {
                String scheme = "grpcs".equals(matcher.group(1)) ? "https" : "http";
                String host = matcher.group(2);
                return scheme + "://" + host + ":8765";
            }
        }
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

        if (root.has("CoresTotal")) {
            info.setCoresTotal(root.get("CoresTotal").getAsDouble());
        }
        if (root.has("CoresUsed")) {
            info.setCoresUsed(root.get("CoresUsed").getAsDouble());
        }
        if (root.has("MemoryTotal")) {
            info.setMemoryTotal(Long.parseLong(root.get("MemoryTotal").getAsString()));
        }
        if (root.has("MemoryUsed")) {
            info.setMemoryUsed(Long.parseLong(root.get("MemoryUsed").getAsString()));
        }
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

        nodes.sort((a, b) -> Double.compare(b.getLoadPercent(), a.getLoadPercent()));
        info.setNodes(nodes);
    }

    private static final int MAX_REDIRECTS = 5;

    private String httpGet(String urlString) throws Exception {
        try {
            return doHttpGet(urlString, false);
        } catch (UnauthorizedException e) {
            // Cookie may have been rotated/invalidated server-side — drop and retry once
            invalidateSession();
            return doHttpGet(urlString, true);
        }
    }

    private String doHttpGet(String urlString, boolean isRetry) throws Exception {
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
                applyAuth(connection);

                int responseCode = connection.getResponseCode();
                if (responseCode == 301 || responseCode == 302
                        || responseCode == 303 || responseCode == 307 || responseCode == 308) {
                    String location = connection.getHeaderField("Location");
                    if (CommonUtils.isEmpty(location)) {
                        throw new Exception("Redirect without Location from " + currentUrl);
                    }
                    if (location.startsWith("/")) {
                        URI base = URI.create(currentUrl);
                        currentUrl = base.getScheme() + "://" + base.getAuthority() + location;
                    } else {
                        currentUrl = location;
                    }
                    continue;
                }

                if (responseCode == 401 && !isRetry) {
                    throw new UnauthorizedException(currentUrl);
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

    private void applyAuth(HttpURLConnection connection) throws Exception {
        if (!CommonUtils.isEmpty(authToken)) {
            connection.setRequestProperty("Authorization", "OAuth " + authToken);
            return;
        }
        if (!CommonUtils.isEmpty(user)) {
            String cookie = ensureSession();
            if (cookie != null) {
                connection.setRequestProperty("Cookie", SESSION_COOKIE_NAME + "=" + cookie);
            }
        }
    }

    private String ensureSession() throws Exception {
        String cookie = sessionCookie;
        if (cookie != null && System.currentTimeMillis() < sessionExpiresAt) {
            return cookie;
        }
        synchronized (sessionLock) {
            if (sessionCookie != null && System.currentTimeMillis() < sessionExpiresAt) {
                return sessionCookie;
            }
            String fresh = login();
            sessionCookie = fresh;
            sessionExpiresAt = System.currentTimeMillis() + SESSION_TTL_MS;
            return fresh;
        }
    }

    private void invalidateSession() {
        synchronized (sessionLock) {
            sessionCookie = null;
            sessionExpiresAt = 0;
        }
    }

    private String login() throws Exception {
        String urlString = baseUrl + "/login";
        URI uri = URI.create(urlString);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        try {
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");

            JsonObject body = new JsonObject();
            body.addProperty("user", user);
            body.addProperty("password", password == null ? "" : password);
            byte[] payload = body.toString().getBytes(StandardCharsets.UTF_8);
            try (OutputStream os = connection.getOutputStream()) {
                os.write(payload);
            }

            int code = connection.getResponseCode();
            if (code != 200) {
                throw new Exception("Login failed: HTTP " + code + " from " + urlString);
            }

            // Set-Cookie header may be split across multiple values; getHeaderFields gives all of them
            for (String header : connection.getHeaderFields().getOrDefault("Set-Cookie", List.of())) {
                String value = extractSessionCookie(header);
                if (value != null) {
                    return value;
                }
            }
            throw new Exception("Login response did not contain " + SESSION_COOKIE_NAME + " cookie");
        } finally {
            connection.disconnect();
        }
    }

    static String extractSessionCookie(String setCookieHeader) {
        if (setCookieHeader == null) {
            return null;
        }
        Matcher m = SESSION_COOKIE_PATTERN.matcher(setCookieHeader);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private static final class UnauthorizedException extends Exception {
        UnauthorizedException(String url) {
            super("HTTP 401 from " + url);
        }
    }
}
