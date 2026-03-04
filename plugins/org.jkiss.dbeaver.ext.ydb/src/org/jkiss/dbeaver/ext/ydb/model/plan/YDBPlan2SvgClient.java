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
import org.jkiss.dbeaver.DBException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * HTTP client that converts plan JSON to SVG via the YDB viewer endpoint.
 * Uses POST /viewer/plan2svg on the YDB monitoring port.
 */
public class YDBPlan2SvgClient {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @NotNull
    public static String convertToSvg(@NotNull String viewerBaseUrl, @NotNull String planJson,
                                       @Nullable String authToken) throws DBException {
        String url = viewerBaseUrl.endsWith("/")
            ? viewerBaseUrl + "viewer/plan2svg"
            : viewerBaseUrl + "/viewer/plan2svg";

        try {
            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(planJson));

            if (authToken != null && !authToken.isEmpty()) {
                requestBuilder.header("Authorization", authToken);
            }

            HttpResponse<String> response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new DBException("plan2svg endpoint returned HTTP " + response.statusCode()
                    + ": " + response.body());
            }

            return response.body();
        } catch (IOException e) {
            throw new DBException("Failed to connect to plan2svg endpoint at " + url
                + ". Make sure the YDB monitoring port is accessible.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DBException("plan2svg request interrupted", e);
        }
    }

    @NotNull
    public static String buildViewerUrl(@NotNull String host, int monitoringPort) {
        return "http://" + host + ":" + monitoringPort;
    }
}
