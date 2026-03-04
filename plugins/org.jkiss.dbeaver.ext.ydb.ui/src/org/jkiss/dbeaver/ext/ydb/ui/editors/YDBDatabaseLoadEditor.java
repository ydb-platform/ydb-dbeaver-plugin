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
package org.jkiss.dbeaver.ext.ydb.ui.editors;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.IEditorInput;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBDatabaseLoadInfo;
import org.jkiss.dbeaver.ext.ydb.model.dashboard.YDBViewerClient;
import org.jkiss.dbeaver.ext.ydb.ui.YDBConnectionPage;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.ui.editors.SinglePageDatabaseEditor;
import org.jkiss.utils.CommonUtils;

public class YDBDatabaseLoadEditor extends SinglePageDatabaseEditor<IEditorInput> {

    private static final Log log = Log.getLog(YDBDatabaseLoadEditor.class);
    private static final long REFRESH_INTERVAL_MS = 5000;

    private YDBDatabaseLoadComposite dashboardComposite;
    private YDBViewerClient viewerClient;
    private volatile boolean disposed;
    private Thread refreshThread;

    @Override
    public void createEditorControl(Composite parent) {
        DBCExecutionContext executionContext = getExecutionContext();
        if (executionContext == null) {
            return;
        }

        var container = executionContext.getDataSource().getContainer();
        var connectionConfig = container.getConnectionConfiguration();

        String jdbcUrl = connectionConfig.getUrl();
        String explicitUrl = connectionConfig.getProviderProperty(YDBConnectionPage.PROP_MONITORING_URL);
        String hostName = connectionConfig.getHostName();
        String useSecureProp = connectionConfig.getProviderProperty(YDBConnectionPage.PROP_USE_SECURE);
        boolean secure = CommonUtils.isEmpty(useSecureProp) || CommonUtils.toBoolean(useSecureProp);
        String baseUrl = YDBViewerClient.resolveBaseUrl(explicitUrl, jdbcUrl, hostName, secure);
        String database = connectionConfig.getDatabaseName();

        dashboardComposite = new YDBDatabaseLoadComposite(parent, SWT.NONE);

        if (baseUrl == null) {
            YDBDatabaseLoadInfo errorInfo = new YDBDatabaseLoadInfo();
            errorInfo.setErrorMessage("Cannot determine monitoring URL. Set ydb.monitoringUrl in driver properties.");
            dashboardComposite.updateData(errorInfo);
            return;
        }

        String authType = connectionConfig.getProviderProperty(YDBConnectionPage.PROP_AUTH_TYPE);
        String token = null;
        if ("token".equals(authType)) {
            token = connectionConfig.getProviderProperty(YDBConnectionPage.PROP_TOKEN);
        }

        viewerClient = new YDBViewerClient(baseUrl, database, token);
        startRefreshThread();
    }

    private void startRefreshThread() {
        refreshThread = new Thread(() -> {
            while (!disposed) {
                try {
                    YDBDatabaseLoadInfo info = viewerClient.fetchDatabaseLoad();
                    Display display = dashboardComposite.getDisplay();
                    if (!disposed && !display.isDisposed()) {
                        display.asyncExec(() -> {
                            if (!disposed && !dashboardComposite.isDisposed()) {
                                dashboardComposite.updateData(info);
                            }
                        });
                    }
                } catch (Exception e) {
                    log.debug("Error fetching database load", e);
                }
                try {
                    Thread.sleep(REFRESH_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "YDB-DatabaseLoad-Refresh");
        refreshThread.setDaemon(true);
        refreshThread.start();
    }

    @Override
    public void dispose() {
        disposed = true;
        if (refreshThread != null) {
            refreshThread.interrupt();
        }
        super.dispose();
    }

    @Override
    public void setFocus() {
        if (dashboardComposite != null && !dashboardComposite.isDisposed()) {
            dashboardComposite.setFocus();
        }
    }

    @Override
    public RefreshResult refreshPart(Object source, boolean force) {
        // Auto-refresh thread handles periodic updates
        return RefreshResult.REFRESHED;
    }
}
