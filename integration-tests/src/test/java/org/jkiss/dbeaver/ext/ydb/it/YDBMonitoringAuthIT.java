package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.jkiss.dbeaver.ext.ydb.it.YDBMonitoringIT.httpGet;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates the login/password wire protocol for the YDB Viewer HTTP API.
 *
 * Runs only when {@code -Dydb.it.monitoring.auth.url} is set, pointing at a
 * SECOND local-ydb container started by run-tests.sh with
 * {@code security_config.enforce_user_token_requirement: true}. That container
 * cannot share endpoints with the main IT YDB (which the rest of the suite
 * connects to anonymously via JDBC), so it listens on alternate ports.
 *
 * Defaults match {@code run-tests.sh} (auth container on grpc:2138 / viewer:8766).
 */
@EnabledIfSystemProperty(named = "ydb.it.monitoring.auth.url", matches = ".*")
public class YDBMonitoringAuthIT {

    private static final Pattern SESSION_COOKIE = Pattern.compile(
        "(?:^|[;,\\s])ydb_session_id=([^;,\\s]+)");

    private static final String BASE_URL =
        System.getProperty("ydb.it.monitoring.auth.url", "http://localhost:8766");
    private static final String DATABASE =
        System.getProperty("ydb.it.monitoring.auth.database", "/local");
    private static final String USER =
        System.getProperty("ydb.it.monitoring.auth.user", "root");
    private static final String PASSWORD =
        System.getProperty("ydb.it.monitoring.auth.password", "");

    @Test
    public void anonymousRequestIsRejected() throws Exception {
        HttpURLConnection c = openGet(BASE_URL + "/viewer/json/cluster", null);
        try {
            int code = c.getResponseCode();
            assertEquals(401, code,
                "expected 401 from auth-enforced YDB without credentials, got " + code);
        } finally {
            c.disconnect();
        }
    }

    @Test
    public void loginReturnsSessionCookie() throws Exception {
        String cookie = login();
        assertNotNull(cookie, "login must return ydb_session_id cookie");
        assertFalse(cookie.isEmpty());
    }

    @Test
    public void clusterEndpoint_worksWithSessionCookie() throws Exception {
        String cookie = login();
        String body = httpGet(BASE_URL + "/viewer/json/cluster", cookie);
        assertTrue(body.contains("CoresTotal") || body.contains("NodesTotal"),
            "expected cluster metrics in response");
    }

    @Test
    public void nodesEndpoint_worksWithSessionCookie() throws Exception {
        String cookie = login();
        String url = BASE_URL + "/viewer/json/nodes?database="
            + URLEncoder.encode(DATABASE, StandardCharsets.UTF_8);
        String body = httpGet(url, cookie);
        assertTrue(body.contains("\"Nodes\""), "expected Nodes array in response");
    }

    private static String login() throws Exception {
        HttpURLConnection c = (HttpURLConnection) URI.create(BASE_URL + "/login").toURL().openConnection();
        try {
            c.setRequestMethod("POST");
            c.setConnectTimeout(5000);
            c.setReadTimeout(10000);
            c.setDoOutput(true);
            c.setRequestProperty("Content-Type", "application/json");
            c.setRequestProperty("Accept", "application/json");
            String body = "{\"user\":\"" + USER + "\",\"password\":\"" + PASSWORD + "\"}";
            try (OutputStream os = c.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
            int code = c.getResponseCode();
            assertEquals(200, code, "login failed: HTTP " + code);
            for (String header : c.getHeaderFields().getOrDefault("Set-Cookie", List.of())) {
                Matcher m = SESSION_COOKIE.matcher(header);
                if (m.find()) {
                    return m.group(1);
                }
            }
            fail("no ydb_session_id cookie in login response");
            return null;
        } finally {
            c.disconnect();
        }
    }

    private static HttpURLConnection openGet(String urlString, String cookie) throws Exception {
        HttpURLConnection c = (HttpURLConnection) URI.create(urlString).toURL().openConnection();
        c.setRequestMethod("GET");
        c.setConnectTimeout(5000);
        c.setReadTimeout(10000);
        c.setRequestProperty("Accept", "application/json");
        if (cookie != null) {
            c.setRequestProperty("Cookie", "ydb_session_id=" + cookie);
        }
        return c;
    }
}
