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
package org.jkiss.dbeaver.ext.ydb.ui;

import org.eclipse.osgi.util.NLS;

public class YDBUIMessages extends NLS {
    private static final String BUNDLE_NAME = "org.jkiss.dbeaver.ext.ydb.ui.YDBUIMessages"; //$NON-NLS-1$

    public static String dialog_connection_auth_type;
    public static String dialog_connection_auth_anonymous;
    public static String dialog_connection_auth_static;
    public static String dialog_connection_auth_token;
    public static String dialog_connection_auth_service_account;
    public static String dialog_connection_auth_metadata;
    public static String dialog_connection_token;
    public static String dialog_connection_token_tip;
    public static String dialog_connection_sa_file;
    public static String dialog_connection_sa_file_tip;
    public static String dialog_connection_use_secure;
    public static String dialog_connection_monitoring_port;
    public static String dialog_connection_monitoring_port_tip;
    public static String dialog_connection_autocomplete_api;
    public static String dialog_connection_autocomplete_api_tip;
    public static String dialog_connection_ssl_certificate;
    public static String dialog_connection_ssl_certificate_tip;
    public static String dialog_connection_data_engineering_mode;
    public static String dialog_connection_data_engineering_mode_tip;

    static {
        NLS.initializeMessages(BUNDLE_NAME, YDBUIMessages.class);
    }

    private YDBUIMessages() {
    }
}
