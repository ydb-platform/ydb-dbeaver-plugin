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
package org.jkiss.dbeaver.ext.ydb.ui.dashboard;

import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBDatabaseLoadInfo;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBViewerClient;
import org.jkiss.dbeaver.ext.ydb.ui.YDBConnectionPage;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.runtime.VoidProgressMonitor;
import org.jkiss.utils.CommonUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared cache that fetches YDB monitoring data once per cycle
 * and distributes it to all dashboard renderer instances for the same data source.
 */
public class YDBDashboardDataCache {

    private static final Log log = Log.getLog(YDBDashboardDataCache.class);
    private static final long FETCH_INTERVAL_MS = 5000;
    private static final Map<String, CacheEntry> entries = new ConcurrentHashMap<>();

    public interface Listener {
        void onDataUpdate(YDBDatabaseLoadInfo info);
    }

    private static final String RUNNING_QUERIES_SQL = "SELECT COUNT(*) FROM `.sys/query_sessions`";

    private static class CacheEntry {
        final YDBViewerClient client;
        final DBPDataSourceContainer dsContainer;
        final Set<Listener> listeners = Collections.synchronizedSet(new HashSet<>());
        volatile YDBDatabaseLoadInfo lastData;
        Timer timer;

        CacheEntry(YDBViewerClient client, DBPDataSourceContainer dsContainer) {
            this.client = client;
            this.dsContainer = dsContainer;
        }
    }

    public static void register(DBPDataSourceContainer dsContainer, Listener listener) {
        String key = dsContainer.getId();
        CacheEntry entry = entries.computeIfAbsent(key, k -> createEntry(dsContainer));
        entry.listeners.add(listener);
        if (entry.lastData != null) {
            listener.onDataUpdate(entry.lastData);
        }
    }

    public static void unregister(DBPDataSourceContainer dsContainer, Listener listener) {
        String key = dsContainer.getId();
        CacheEntry entry = entries.get(key);
        if (entry == null) {
            return;
        }
        entry.listeners.remove(listener);
        if (entry.listeners.isEmpty()) {
            if (entry.timer != null) {
                entry.timer.cancel();
            }
            entries.remove(key);
        }
    }

    private static CacheEntry createEntry(DBPDataSourceContainer dsContainer) {
        DBPConnectionConfiguration config = dsContainer.getConnectionConfiguration();
        String jdbcUrl = config.getUrl();
        String explicitUrl = config.getProviderProperty(YDBConnectionPage.PROP_MONITORING_URL);
        String hostName = config.getHostName();
        String useSecureProp = config.getProviderProperty(YDBConnectionPage.PROP_USE_SECURE);
        boolean secure = CommonUtils.isEmpty(useSecureProp) || CommonUtils.toBoolean(useSecureProp);
        String baseUrl = YDBViewerClient.resolveBaseUrl(explicitUrl, jdbcUrl, hostName, secure);
        String database = config.getDatabaseName();

        String authType = config.getProviderProperty(YDBConnectionPage.PROP_AUTH_TYPE);
        String token = null;
        String userName = null;
        String password = null;
        if ("token".equals(authType)) {
            token = config.getProviderProperty(YDBConnectionPage.PROP_TOKEN);
        } else {
            userName = config.getUserName();
            password = config.getUserPassword();
        }

        YDBViewerClient client = new YDBViewerClient(
            baseUrl != null ? baseUrl : "http://localhost:8765",
            database,
            token,
            userName,
            password
        );

        CacheEntry entry = new CacheEntry(client, dsContainer);
        Timer timer = new Timer("YDB-Dashboard-Fetch-" + dsContainer.getName(), true);
        entry.timer = timer;
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    YDBDatabaseLoadInfo info = entry.client.fetchDatabaseLoad();
                    fetchRunningQueries(entry.dsContainer, info);
                    entry.lastData = info;
                    Set<Listener> snapshot;
                    synchronized (entry.listeners) {
                        snapshot = new HashSet<>(entry.listeners);
                    }
                    for (Listener l : snapshot) {
                        try {
                            l.onDataUpdate(info);
                        } catch (Exception e) {
                            log.debug("Error notifying dashboard listener", e);
                        }
                    }
                } catch (Exception e) {
                    log.debug("Error fetching YDB dashboard data", e);
                }
            }
        }, 0, FETCH_INTERVAL_MS);

        return entry;
    }

    private static void fetchRunningQueries(DBPDataSourceContainer dsContainer, YDBDatabaseLoadInfo info) {
        if (dsContainer.getDataSource() == null) {
            return;
        }
        try (JDBCSession session = DBUtils.openMetaSession(new VoidProgressMonitor(), dsContainer, "Fetch running queries count")) {
            try (JDBCPreparedStatement stmt = session.prepareStatement(RUNNING_QUERIES_SQL)) {
                try (JDBCResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        info.setRunningQueries(rs.getInt(1));
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to fetch running queries count: " + e.getMessage());
        }
    }
}
