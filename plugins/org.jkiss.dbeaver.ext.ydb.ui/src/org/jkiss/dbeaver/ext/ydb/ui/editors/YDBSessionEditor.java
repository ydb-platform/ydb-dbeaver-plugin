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

import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IContributionManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.dialogs.IDialogSettings;
import org.eclipse.swt.widgets.Composite;
import org.jkiss.dbeaver.ext.ydb.model.YDBDataSource;
import org.jkiss.dbeaver.ext.ydb.model.session.YDBSessionManager;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSession;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionManager;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.ui.ActionUtils;
import org.jkiss.dbeaver.ui.DBeaverIcons;
import org.jkiss.dbeaver.ui.UIIcon;
import org.jkiss.dbeaver.ui.views.session.AbstractSessionEditor;
import org.jkiss.dbeaver.ui.views.session.SessionManagerViewer;
import org.jkiss.utils.CommonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * YDB Session Editor.
 *
 * Displays query sessions from .sys/query_sessions system view.
 *
 * How to access:
 * 1. Double-click on YDB connection in Database Navigator
 * 2. In the opened editor, find "Session Manager" tab
 *
 * Note: YDB currently does not support killing sessions.
 */
public class YDBSessionEditor extends AbstractSessionEditor {

    @Override
    protected SessionManagerViewer createSessionViewer(DBCExecutionContext executionContext, Composite parent) {
        final YDBDataSource dataSource = (YDBDataSource) executionContext.getDataSource();
        return new SessionManagerViewer<>(this, parent, new YDBSessionManager(dataSource)) {
            private boolean hideIdle;

            @Override
            protected void contributeToToolbar(DBAServerSessionManager sessionManager, IContributionManager contributionManager) {
                // Add "Hide Idle Sessions" toggle button
                contributionManager.add(ActionUtils.makeActionContribution(
                    new Action("Hide Idle Sessions", Action.AS_CHECK_BOX) {
                        {
                            setToolTipText("Hide sessions in IDLE state");
                            setImageDescriptor(DBeaverIcons.getImageDescriptor(UIIcon.HIDE_ALL_DETAILS));
                            setChecked(hideIdle);
                        }

                        @Override
                        public void run() {
                            hideIdle = isChecked();
                            refreshPart(YDBSessionEditor.this, true);
                        }
                    }, true));

                contributionManager.add(new Separator());
            }

            @Override
            protected void onSessionSelect(DBAServerSession session) {
                super.onSessionSelect(session);
                // No kill actions available for YDB
            }

            @Override
            public Map<String, Object> getSessionOptions() {
                Map<String, Object> options = new HashMap<>();
                if (hideIdle) {
                    options.put(YDBSessionManager.OPTION_HIDE_IDLE, true);
                }
                return options;
            }

            @Override
            protected void loadSettings(IDialogSettings settings) {
                hideIdle = CommonUtils.toBoolean(settings.get("hideIdle"));
                super.loadSettings(settings);
            }

            @Override
            protected void saveSettings(IDialogSettings settings) {
                super.saveSettings(settings);
                settings.put("hideIdle", hideIdle);
            }
        };
    }
}
