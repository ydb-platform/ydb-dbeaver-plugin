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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class YDBSqlglotClient {

    private static final String BASE_URL = "https://functions.yandexcloud.net/d4e4evd4n6rg50cb3mag";
    private static final int TIMEOUT_MS = 30000;
    private static final Gson GSON = new Gson();

    private static volatile List<String> cachedDialects = null;

    @NotNull
    public static List<String> getDialects() throws DBException {
        if (cachedDialects == null) {
            synchronized (YDBSqlglotClient.class) {
                if (cachedDialects == null) {
                    cachedDialects = fetchDialects();
                }
            }
        }
        return cachedDialects;
    }

    @NotNull
    private static List<String> fetchDialects() throws DBException {
        try {
            URL url = new URL(BASE_URL + "?action=dialects");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            if (conn instanceof HttpsURLConnection) {
                configureSsl((HttpsURLConnection) conn);
            }
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(TIMEOUT_MS);
            conn.setReadTimeout(TIMEOUT_MS);

            int responseCode = conn.getResponseCode();
            InputStream is = responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
            String responseBody;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                responseBody = sb.toString().trim();
            }

            if (responseCode != 200) {
                throw new DBException("Failed to fetch dialects: HTTP " + responseCode + ": " + responseBody);
            }

            JsonObject json = GSON.fromJson(responseBody, JsonObject.class);
            JsonArray array = json.getAsJsonArray("dialects");
            List<String> result = new ArrayList<>(array.size());
            for (int i = 0; i < array.size(); i++) {
                result.add(array.get(i).getAsString());
            }
            return Collections.unmodifiableList(result);
        } catch (IOException e) {
            throw new DBException("Failed to connect to sqlglot service: " + e.getMessage(), e);
        }
    }

    @NotNull
    public static String convertSql(@NotNull String sql, @NotNull String dialect) throws DBException {
        try {
            Map<String, String> body = Map.of("sql", sql, "dialect", dialect);
            String jsonBody = GSON.toJson(body);

            URL url = new URL(BASE_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            if (conn instanceof HttpsURLConnection) {
                configureSsl((HttpsURLConnection) conn);
            }
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            conn.setConnectTimeout(TIMEOUT_MS);
            conn.setReadTimeout(TIMEOUT_MS);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            String responseBody;
            InputStream is = responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                responseBody = sb.toString().trim();
            }

            if (responseCode != 200) {
                throw new DBException("Failed to convert SQL: HTTP " + responseCode + ": " + responseBody);
            }

            JsonObject json = GSON.fromJson(responseBody, JsonObject.class);
            if (json.has("error")) {
                throw new DBException(json.get("error").getAsString());
            }
            if (json.has("convertedSql")) {
                return json.get("convertedSql").getAsString();
            }
            return responseBody;
        } catch (IOException e) {
            throw new DBException("Failed to connect to sqlglot service: " + e.getMessage(), e);
        }
    }

    private static void configureSsl(HttpsURLConnection conn) throws DBException {
        try {
            TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                }
            };
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAll, new java.security.SecureRandom());
            conn.setSSLSocketFactory(sc.getSocketFactory());
            conn.setHostnameVerifier((hostname, session) -> true);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new DBException("Failed to configure SSL: " + e.getMessage(), e);
        }
    }
}
