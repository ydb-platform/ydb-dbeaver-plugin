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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the /viewer/json/tenantinfo → {@link YDBDatabaseLoadInfo}
 * field mapping. Guards against regressing to the cluster-wide
 * /viewer/json/cluster shape (CoresUsed/MemoryTotal/StorageTotal — wrong scope).
 */
public class YDBViewerClientTenantInfoTest {

    @Test
    public void appliesTenantMetrics_databaseScoped() {
        // Tenant payload as returned by /viewer/json/tenantinfo?path=/Root/db
        // — uint64 values come as JSON strings, cores as doubles.
        String tenantJson = "{"
            + "\"Name\":\"/Root/db\","
            + "\"Overall\":\"Green\","
            + "\"CoresUsed\":2.5,"
            + "\"CoresTotal\":16,"
            + "\"MemoryUsed\":\"8589934592\","
            + "\"MemoryLimit\":\"34359738368\","
            + "\"StorageAllocatedSize\":\"107374182400\","
            + "\"StorageAllocatedLimit\":\"536870912000\","
            + "\"AliveNodes\":3,"
            + "\"NodeIds\":[1,2,3,4]"
            + "}";
        JsonObject t = JsonParser.parseString(tenantJson).getAsJsonObject();

        YDBDatabaseLoadInfo info = new YDBDatabaseLoadInfo();
        YDBViewerClient.applyTenantMetrics(t, info);

        assertEquals(2.5, info.getCoresUsed(), 0.0001);
        assertEquals(16.0, info.getCoresTotal(), 0.0001);
        assertEquals(8L * 1024 * 1024 * 1024, info.getMemoryUsed());
        assertEquals(32L * 1024 * 1024 * 1024, info.getMemoryTotal());
        assertEquals(100L * 1024 * 1024 * 1024, info.getStorageUsed());
        assertEquals(500L * 1024 * 1024 * 1024, info.getStorageTotal());
        assertEquals(3, info.getNodesAlive());
        // NodeIds.length is the authoritative total (AliveNodes only counts alive)
        assertEquals(4, info.getNodesTotal());
        assertEquals("Green", info.getOverallStatus());
        // tenantinfo doesn't expose network throughput → 0, not leaked from cluster
        assertEquals(0.0, info.getNetworkBytesPerSec(), 0.0001);
    }

    @Test
    public void appliesTenantMetrics_handlesNumericMemoryFields() {
        // Defensive: some viewer versions / future builds may emit numbers, not strings.
        String tenantJson = "{"
            + "\"CoresUsed\":1,"
            + "\"CoresTotal\":4,"
            + "\"MemoryUsed\":1024,"
            + "\"MemoryLimit\":2048,"
            + "\"StorageAllocatedSize\":4096,"
            + "\"StorageAllocatedLimit\":8192,"
            + "\"AliveNodes\":1,"
            + "\"NodeIds\":[42]"
            + "}";
        JsonObject t = JsonParser.parseString(tenantJson).getAsJsonObject();

        YDBDatabaseLoadInfo info = new YDBDatabaseLoadInfo();
        YDBViewerClient.applyTenantMetrics(t, info);

        assertEquals(1024L, info.getMemoryUsed());
        assertEquals(2048L, info.getMemoryTotal());
        assertEquals(4096L, info.getStorageUsed());
        assertEquals(8192L, info.getStorageTotal());
        assertEquals(1, info.getNodesAlive());
        assertEquals(1, info.getNodesTotal());
    }

    @Test
    public void appliesTenantMetrics_missingFieldsLeaveDefaults() {
        // Minimal tenant entry — only one field present. Others stay at default 0.
        JsonObject t = JsonParser.parseString("{\"Overall\":\"Yellow\"}").getAsJsonObject();

        YDBDatabaseLoadInfo info = new YDBDatabaseLoadInfo();
        YDBViewerClient.applyTenantMetrics(t, info);

        assertEquals("Yellow", info.getOverallStatus());
        assertEquals(0.0, info.getCoresUsed(), 0.0001);
        assertEquals(0L, info.getMemoryUsed());
        assertEquals(0L, info.getStorageTotal());
        assertEquals(0, info.getNodesAlive());
        assertEquals(0, info.getNodesTotal());
    }

    @Test
    public void appliesTenantMetrics_fallsBackToAliveNodesWhenNodeIdsMissing() {
        String tenantJson = "{\"AliveNodes\":7}";
        JsonObject t = JsonParser.parseString(tenantJson).getAsJsonObject();

        YDBDatabaseLoadInfo info = new YDBDatabaseLoadInfo();
        YDBViewerClient.applyTenantMetrics(t, info);

        assertEquals(7, info.getNodesAlive());
        assertEquals(7, info.getNodesTotal());
    }

    @Test
    public void getCpuPercent_reflectsTenantCores() {
        JsonObject t = JsonParser.parseString(
            "{\"CoresUsed\":3,\"CoresTotal\":12}").getAsJsonObject();
        YDBDatabaseLoadInfo info = new YDBDatabaseLoadInfo();
        YDBViewerClient.applyTenantMetrics(t, info);

        // 3 of 12 cores = 25%
        assertTrue(Math.abs(info.getCpuPercent() - 25.0) < 0.0001);
    }
}
