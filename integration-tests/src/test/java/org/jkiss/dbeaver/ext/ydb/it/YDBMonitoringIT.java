package org.jkiss.dbeaver.ext.ydb.it;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates the YDB Viewer HTTP API wire protocol that {@code YDBViewerClient}
 * relies on, against a local-ydb container with authentication disabled
 * (the default mode used by the rest of the IT suite).
 *
 * Anonymous access:
 *   GET http://localhost:8765/viewer/json/cluster   → 200 + JSON metrics
 *   GET http://localhost:8765/viewer/json/nodes     → 200 + JSON node array
 */
public class YDBMonitoringIT {

    private static final String BASE_URL =
        System.getProperty("ydb.it.monitoring.url", "http://localhost:8765");
    private static final String DATABASE =
        System.getProperty("ydb.it.monitoring.database", "/local");

    @Test
    public void clusterEndpoint_returnsJsonMetrics() throws Exception {
        String body = httpGet(BASE_URL + "/viewer/json/cluster", null);
        // Sanity check: the cluster endpoint always returns at least these fields.
        assertTrue(body.contains("CoresTotal") || body.contains("NodesTotal"),
            "expected cluster metrics in response, got: " + truncate(body));
    }

    @Test
    public void nodesEndpoint_returnsJsonNodes() throws Exception {
        String url = BASE_URL + "/viewer/json/nodes?database="
            + URLEncoder.encode(DATABASE, StandardCharsets.UTF_8);
        String body = httpGet(url, null);
        assertTrue(body.contains("\"Nodes\""),
            "expected Nodes array in response, got: " + truncate(body));
    }

    /**
     * Guards the database-scoped dashboard: {@code YDBViewerClient.fetchDatabaseLoad()}
     * must hit /viewer/json/tenantinfo (per-database), not /viewer/json/cluster
     * (cluster-wide). This test pins the wire shape we rely on:
     * the response carries a {@code TenantInfo} array with the metric fields we map.
     */
    @Test
    public void tenantInfoEndpoint_returnsDatabaseScopedMetrics() throws Exception {
        String url = BASE_URL + "/viewer/json/tenantinfo?path="
            + URLEncoder.encode(DATABASE, StandardCharsets.UTF_8)
            + "&tablets=false&system_tablets=false&storage=true&memory=true";
        String body = httpGet(url, null);

        assertTrue(body.contains("\"TenantInfo\""),
            "expected TenantInfo array in response, got: " + truncate(body));
        // Tenant-scoped fields the dashboard maps onto YDBDatabaseLoadInfo.
        // (Not asserting values — just that the wire contract still includes these keys.)
        assertTrue(body.contains("\"CoresUsed\"") || body.contains("\"PoolStats\""),
            "expected CPU metrics in tenantinfo, got: " + truncate(body));
        assertTrue(body.contains("\"MemoryLimit\"") || body.contains("\"MemoryUsed\""),
            "expected memory metrics in tenantinfo, got: " + truncate(body));
        assertTrue(body.contains("\"StorageAllocatedSize\"")
                || body.contains("\"StorageAllocatedLimit\""),
            "expected storage metrics in tenantinfo, got: " + truncate(body));
        assertTrue(body.contains("\"AliveNodes\"") || body.contains("\"NodeIds\""),
            "expected node info in tenantinfo, got: " + truncate(body));
    }

    static String httpGet(String urlString, String cookie) throws Exception {
        HttpURLConnection c = (HttpURLConnection) URI.create(urlString).toURL().openConnection();
        try {
            c.setRequestMethod("GET");
            c.setConnectTimeout(5000);
            c.setReadTimeout(10000);
            c.setRequestProperty("Accept", "application/json");
            if (cookie != null) {
                c.setRequestProperty("Cookie", "ydb_session_id=" + cookie);
            }
            int code = c.getResponseCode();
            assertEquals(200, code, "HTTP " + code + " from " + urlString);
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = r.readLine()) != null) sb.append(line);
                return sb.toString();
            }
        } finally {
            c.disconnect();
        }
    }

    static String truncate(String s) {
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }
}
