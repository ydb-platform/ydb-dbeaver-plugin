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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.jkiss.dbeaver.Log;
import org.jkiss.utils.CommonUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class YDBAutocompleteClient {

    private static final Log log = Log.getLog(YDBAutocompleteClient.class);

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 3000;
    private static final int MAX_REDIRECTS = 5;
    private static final long CIRCUIT_BREAKER_COOLDOWN_MS = 60_000;

    private final String baseUrl;
    private final String database;
    private final String authToken;

    private volatile boolean available = true;
    private volatile long lastFailureTime = 0;

    public YDBAutocompleteClient(String baseUrl, String database, String authToken) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.database = database;
        this.authToken = authToken;
    }

    public List<AutocompleteEntity> fetchEntities(String prefix, int limit) {
        if (!checkAvailable()) {
            return Collections.emptyList();
        }
        try {
            String encodedDb = URLEncoder.encode(database, StandardCharsets.UTF_8);
            String encodedPrefix = URLEncoder.encode(prefix, StandardCharsets.UTF_8);
            String url = baseUrl + "/viewer/json/autocomplete?database=" + encodedDb
                + "&prefix=" + encodedPrefix + "&limit=" + limit;
            String json = httpGet(url);
            return parseEntities(json);
        } catch (Exception e) {
            markUnavailable();
            log.warn("YDB autocomplete API is not available (" + baseUrl + "): " + e.getMessage()
                + ". Will retry in " + (CIRCUIT_BREAKER_COOLDOWN_MS / 1000) + "s");
            return Collections.emptyList();
        }
    }

    public List<AutocompleteEntity> fetchColumns(List<String> tableNames, int limit) {
        if (!checkAvailable()) {
            return Collections.emptyList();
        }
        try {
            String encodedDb = URLEncoder.encode(database, StandardCharsets.UTF_8);
            StringBuilder url = new StringBuilder(baseUrl + "/viewer/json/autocomplete?database=" + encodedDb
                + "&limit=" + limit);
            for (String table : tableNames) {
                url.append("&table=").append(URLEncoder.encode(table, StandardCharsets.UTF_8));
            }
            String json = httpGet(url.toString());
            return parseEntities(json);
        } catch (Exception e) {
            markUnavailable();
            log.warn("YDB autocomplete API is not available (" + baseUrl + "): " + e.getMessage()
                + ". Will retry in " + (CIRCUIT_BREAKER_COOLDOWN_MS / 1000) + "s");
            return Collections.emptyList();
        }
    }

    public boolean isAvailable() {
        return checkAvailable();
    }

    private boolean checkAvailable() {
        if (available) {
            return true;
        }
        if (System.currentTimeMillis() - lastFailureTime > CIRCUIT_BREAKER_COOLDOWN_MS) {
            available = true;
            log.warn("YDB autocomplete API: retrying after cooldown (" + baseUrl + ")");
            return true;
        }
        return false;
    }

    private void markUnavailable() {
        available = false;
        lastFailureTime = System.currentTimeMillis();
    }

    public static List<AutocompleteEntity> parseEntities(String json) {
        List<AutocompleteEntity> result = new ArrayList<>();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        if (!root.has("Result")) {
            return result;
        }
        JsonObject resultObj = root.getAsJsonObject("Result");
        if (!resultObj.has("Entities")) {
            return result;
        }
        JsonArray entities = resultObj.getAsJsonArray("Entities");
        for (JsonElement el : entities) {
            JsonObject obj = el.getAsJsonObject();
            String name = obj.has("Name") ? obj.get("Name").getAsString() : "";
            String type = obj.has("Type") ? obj.get("Type").getAsString() : null;
            String parent = obj.has("Parent") ? obj.get("Parent").getAsString() : null;
            if (!name.isEmpty()) {
                result.add(new AutocompleteEntity(name, type, parent));
            }
        }
        return result;
    }

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
