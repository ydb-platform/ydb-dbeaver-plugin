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

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.ui.handlers.HandlerUtil;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.ydb.model.YDBDataSource;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBPlan2SvgClient;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBStatisticsExecutor;
import org.jkiss.dbeaver.ext.ydb.model.plan.YDBStatisticsResult;
import org.jkiss.dbeaver.ext.ydb.ui.YDBConnectionPage;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.exec.DBCExecutionPurpose;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.runtime.DefaultProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLScriptElement;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.editors.sql.SQLEditor;
import org.jkiss.dbeaver.utils.RuntimeUtils;
import org.jkiss.utils.CommonUtils;

/**
 * Handler for "Statistics" command in SQL editor toolbar.
 * Executes the current query with StatsMode.PROFILE, converts the plan JSON to SVG
 * via the YDB viewer endpoint, and displays the result.
 */
public class YDBStatisticsHandler extends AbstractHandler {

    private static final Log log = Log.getLog(YDBStatisticsHandler.class);

    @Override
    public void setEnabled(Object evaluationContext) {
        setBaseEnabled(YDBHandlerUtils.isYdbEditor(evaluationContext));
    }

    @Override
    public Object execute(ExecutionEvent event) throws ExecutionException {
        SQLEditor editor = RuntimeUtils.getObjectAdapter(HandlerUtil.getActiveEditor(event), SQLEditor.class);
        if (editor == null) {
            return null;
        }

        DBPDataSource dataSource = editor.getDataSource();
        if (!(dataSource instanceof YDBDataSource)) {
            DBWorkbench.getPlatformUI().showError("Statistics", "Statistics profiling is only available for YDB datasources.");
            return null;
        }

        SQLScriptElement scriptElement = editor.extractActiveQuery();
        if (scriptElement == null) {
            DBWorkbench.getPlatformUI().showError("Statistics", "No active query found. Place cursor on a SQL statement.");
            return null;
        }

        String queryText = scriptElement.getText();
        if (queryText == null || queryText.trim().isEmpty()) {
            DBWorkbench.getPlatformUI().showError("Statistics", "Query text is empty.");
            return null;
        }

        YDBDataSource ydbDataSource = (YDBDataSource) dataSource;
        DBPConnectionConfiguration connConfig = ydbDataSource.getContainer().getConnectionConfiguration();

        String monitoringUrl = connConfig.getProviderProperty(YDBConnectionPage.PROP_MONITORING_URL);
        if (CommonUtils.isEmpty(monitoringUrl)) {
            String host = connConfig.getHostName();
            monitoringUrl = YDBPlan2SvgClient.buildViewerUrl(host, 8765);
        }

        String viewerUrl = monitoringUrl;

        // Get auth token for the viewer endpoint
        String finalAuthToken;
        String authType = connConfig.getProviderProperty(YDBConnectionPage.PROP_AUTH_TYPE);
        if ("token".equals(authType)) {
            finalAuthToken = connConfig.getProviderProperty(YDBConnectionPage.PROP_TOKEN);
        } else {
            finalAuthToken = null;
        }

        Job job = new Job("YDB Statistics Profile") {
            @Override
            protected IStatus run(IProgressMonitor monitor) {
                monitor.beginTask("Executing query with statistics profiling...", 3);
                try {
                    DBCExecutionContext executionContext = editor.getExecutionContext();
                    if (executionContext == null) {
                        showError("No execution context available.");
                        return Status.CANCEL_STATUS;
                    }

                    // Step 1: Execute query with PROFILE stats
                    monitor.subTask("Executing query...");
                    YDBStatisticsResult result;
                    try (JDBCSession session = (JDBCSession) executionContext.openSession(
                            new DefaultProgressMonitor(monitor), DBCExecutionPurpose.UTIL, "Statistics profiling")) {
                        result = YDBStatisticsExecutor.execute(session, queryText);
                    }
                    monitor.worked(1);

                    // Step 2: Convert plan JSON to SVG
                    monitor.subTask("Converting plan to SVG...");
                    String svg = YDBPlan2SvgClient.convertToSvg(viewerUrl, result.getPlanJson(), finalAuthToken);
                    result.setSvgContent(svg);
                    monitor.worked(1);

                    // Step 3: Display SVG in UI
                    monitor.subTask("Rendering...");
                    UIUtils.asyncExec(() -> {
                        YDBStatisticsSvgDialog dialog = new YDBStatisticsSvgDialog(
                            UIUtils.getActiveWorkbenchShell(), result);
                        dialog.open();
                    });
                    monitor.worked(1);

                    return Status.OK_STATUS;
                } catch (DBException e) {
                    log.error("Statistics profiling failed", e);
                    showError(e.getMessage());
                    return Status.CANCEL_STATUS;
                } catch (Exception e) {
                    log.error("Statistics profiling failed", e);
                    showError("Unexpected error: " + e.getMessage());
                    return Status.CANCEL_STATUS;
                } finally {
                    monitor.done();
                }
            }
        };
        job.setUser(true);
        job.schedule();

        return null;
    }

    private static void showError(String message) {
        UIUtils.asyncExec(() ->
            DBWorkbench.getPlatformUI().showError("Statistics Error", message)
        );
    }
}
