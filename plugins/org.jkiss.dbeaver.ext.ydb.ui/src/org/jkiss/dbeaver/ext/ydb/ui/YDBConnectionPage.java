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

import org.eclipse.jface.dialogs.IDialogPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.*;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.ui.IDialogPageProvider;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.dialogs.connection.ConnectionPageAbstract;
import org.jkiss.dbeaver.ui.dialogs.connection.DriverPropertiesDialogPage;
import org.jkiss.utils.CommonUtils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * YDB connection page with authentication options
 */
public class YDBConnectionPage extends ConnectionPageAbstract implements IDialogPageProvider {

    // Auth types
    private static final String AUTH_ANONYMOUS = "anonymous";
    private static final String AUTH_STATIC = "static";
    private static final String AUTH_TOKEN = "token";
    private static final String AUTH_SERVICE_ACCOUNT = "saFile";
    private static final String AUTH_METADATA = "metadata";

    // Provider properties
    public static final String PROP_AUTH_TYPE = "ydb.authType";
    public static final String PROP_TOKEN = "ydb.token";
    public static final String PROP_SA_FILE = "ydb.saFile";
    public static final String PROP_USE_SECURE = "ydb.useSecure";
    public static final String PROP_MONITORING_URL = "ydb.monitoringUrl";
    public static final String PROP_AUTOCOMPLETE_API_ENABLED = "ydb.autocompleteApiEnabled";
    public static final String PROP_SSL_CERTIFICATE = "ydb.sslCertificate";
    public static final String PROP_DATA_ENGINEERING_MODE = "ydb.dataEngineeringMode";

    private Text hostText;
    private Text portText;
    private Text databaseText;
    private Text monitoringUrlText;
    private Combo authTypeCombo;
    private Text userText;
    private Text passwordText;
    private Text tokenText;
    private Text saFileText;
    private Button saFileBrowseButton;
    private Button useSecureCheckbox;
    private Button autocompleteApiCheckbox;
    private Button dataEngineeringModeCheckbox;
    private Text sslCertificateText;
    private Button sslCertificateBrowseButton;
    private Label sslCertificateLabel;
    private Composite sslCertComposite;

    private Composite credentialsGroup;
    private Composite tokenGroup;
    private Composite saFileGroup;

    private final String[] authTypes = {
        AUTH_ANONYMOUS,
        AUTH_STATIC,
        AUTH_TOKEN,
        AUTH_SERVICE_ACCOUNT,
        AUTH_METADATA
    };

    private final String[] authTypeLabels = {
        YDBUIMessages.dialog_connection_auth_anonymous,
        YDBUIMessages.dialog_connection_auth_static,
        YDBUIMessages.dialog_connection_auth_token,
        YDBUIMessages.dialog_connection_auth_service_account,
        YDBUIMessages.dialog_connection_auth_metadata
    };

    @Override
    public void createControl(Composite composite) {
        Composite control = new Composite(composite, SWT.NONE);
        control.setLayout(new GridLayout(1, false));
        control.setLayoutData(new GridData(GridData.FILL_BOTH));

        ModifyListener textListener = e -> {
            updateUrl();
            site.updateButtons();
        };

        // Connection settings group
        Group connectionGroup = UIUtils.createControlGroup(control, "Connection", 4, GridData.FILL_HORIZONTAL, 0);

        // Host
        UIUtils.createControlLabel(connectionGroup, "Host");
        hostText = new Text(connectionGroup, SWT.BORDER);
        hostText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        hostText.addModifyListener(textListener);

        // Port
        UIUtils.createControlLabel(connectionGroup, "Port");
        portText = new Text(connectionGroup, SWT.BORDER);
        GridData gd = new GridData(GridData.HORIZONTAL_ALIGN_BEGINNING);
        gd.widthHint = UIUtils.getFontHeight(portText) * 7;
        portText.setLayoutData(gd);
        portText.addModifyListener(textListener);

        // Database
        UIUtils.createControlLabel(connectionGroup, "Database");
        databaseText = new Text(connectionGroup, SWT.BORDER);
        gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = 3;
        databaseText.setLayoutData(gd);
        databaseText.addModifyListener(textListener);

        // Monitoring URL
        UIUtils.createControlLabel(connectionGroup, YDBUIMessages.dialog_connection_monitoring_port);
        monitoringUrlText = new Text(connectionGroup, SWT.BORDER);
        gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = 3;
        monitoringUrlText.setLayoutData(gd);
        monitoringUrlText.setToolTipText(YDBUIMessages.dialog_connection_monitoring_port_tip);
        monitoringUrlText.addModifyListener(textListener);

        // Use secure connection
        useSecureCheckbox = UIUtils.createCheckbox(connectionGroup,
            YDBUIMessages.dialog_connection_use_secure,
            "Use grpcs:// instead of grpc://",
            true,
            4);
        useSecureCheckbox.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                updateSslCertControls();
                updateUrl();
                site.updateButtons();
            }
        });

        // Custom SSL certificate
        sslCertificateLabel = UIUtils.createControlLabel(connectionGroup, YDBUIMessages.dialog_connection_ssl_certificate);
        sslCertComposite = new Composite(connectionGroup, SWT.NONE);
        gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = 3;
        sslCertComposite.setLayoutData(gd);
        sslCertComposite.setLayout(new GridLayout(2, false));

        sslCertificateText = new Text(sslCertComposite, SWT.BORDER);
        sslCertificateText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        sslCertificateText.setToolTipText(YDBUIMessages.dialog_connection_ssl_certificate_tip);
        sslCertificateText.addModifyListener(textListener);

        sslCertificateBrowseButton = new Button(sslCertComposite, SWT.PUSH);
        sslCertificateBrowseButton.setText("...");
        sslCertificateBrowseButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                FileDialog dialog = new FileDialog(getShell(), SWT.OPEN);
                dialog.setFilterExtensions(new String[]{"*.pem", "*.crt", "*.cer", "*.der", "*.*"});
                dialog.setFilterNames(new String[]{"PEM Files", "CRT Files", "CER Files", "DER Files", "All Files"});
                String file = dialog.open();
                if (file != null) {
                    sslCertificateText.setText(file);
                }
            }
        });

        // Enable autocomplete API
        autocompleteApiCheckbox = UIUtils.createCheckbox(connectionGroup,
            YDBUIMessages.dialog_connection_autocomplete_api,
            YDBUIMessages.dialog_connection_autocomplete_api_tip,
            true,
            4);

        // Data Engineering mode
        dataEngineeringModeCheckbox = UIUtils.createCheckbox(connectionGroup,
            YDBUIMessages.dialog_connection_data_engineering_mode,
            YDBUIMessages.dialog_connection_data_engineering_mode_tip,
            true,
            4);

        // Authentication group
        Group authGroup = UIUtils.createControlGroup(control, "Authentication", 2, GridData.FILL_HORIZONTAL, 0);

        // Auth type selector
        UIUtils.createControlLabel(authGroup, YDBUIMessages.dialog_connection_auth_type);
        authTypeCombo = new Combo(authGroup, SWT.DROP_DOWN | SWT.READ_ONLY);
        authTypeCombo.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        for (String label : authTypeLabels) {
            authTypeCombo.add(label);
        }
        authTypeCombo.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                updateAuthControls();
                updateUrl();
                site.updateButtons();
            }
        });

        // Credentials group (user/password)
        credentialsGroup = new Composite(authGroup, SWT.NONE);
        gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = 2;
        credentialsGroup.setLayoutData(gd);
        credentialsGroup.setLayout(new GridLayout(2, false));

        UIUtils.createControlLabel(credentialsGroup, "User");
        userText = new Text(credentialsGroup, SWT.BORDER);
        userText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        userText.addModifyListener(textListener);

        UIUtils.createControlLabel(credentialsGroup, "Password");
        passwordText = new Text(credentialsGroup, SWT.BORDER | SWT.PASSWORD);
        passwordText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        passwordText.addModifyListener(textListener);

        // Token group
        tokenGroup = new Composite(authGroup, SWT.NONE);
        gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = 2;
        tokenGroup.setLayoutData(gd);
        tokenGroup.setLayout(new GridLayout(2, false));

        UIUtils.createControlLabel(tokenGroup, YDBUIMessages.dialog_connection_token);
        tokenText = new Text(tokenGroup, SWT.BORDER | SWT.PASSWORD);
        tokenText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        tokenText.setToolTipText(YDBUIMessages.dialog_connection_token_tip);
        tokenText.addModifyListener(textListener);

        // Service Account File group
        saFileGroup = new Composite(authGroup, SWT.NONE);
        gd = new GridData(GridData.FILL_HORIZONTAL);
        gd.horizontalSpan = 2;
        saFileGroup.setLayoutData(gd);
        saFileGroup.setLayout(new GridLayout(3, false));

        UIUtils.createControlLabel(saFileGroup, YDBUIMessages.dialog_connection_sa_file);
        saFileText = new Text(saFileGroup, SWT.BORDER);
        saFileText.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        saFileText.setToolTipText(YDBUIMessages.dialog_connection_sa_file_tip);
        saFileText.addModifyListener(textListener);

        saFileBrowseButton = new Button(saFileGroup, SWT.PUSH);
        saFileBrowseButton.setText("...");
        saFileBrowseButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                FileDialog dialog = new FileDialog(getShell(), SWT.OPEN);
                dialog.setFilterExtensions(new String[]{"*.json", "*.*"});
                dialog.setFilterNames(new String[]{"JSON Files", "All Files"});
                String file = dialog.open();
                if (file != null) {
                    saFileText.setText(file);
                }
            }
        });

        // Set default auth type and update visibility
        authTypeCombo.select(0);
        updateAuthControls();
        updateSslCertControls();

        setControl(control);
    }

    private void updateSslCertControls() {
        boolean secure = useSecureCheckbox.getSelection();
        setWidgetVisible(sslCertificateLabel, secure);
        setWidgetVisible(sslCertComposite, secure);
        sslCertificateLabel.getParent().layout(true, true);
    }

    private void setWidgetVisible(Control widget, boolean visible) {
        widget.setVisible(visible);
        ((GridData) widget.getLayoutData()).exclude = !visible;
    }

    private void updateAuthControls() {
        int selection = authTypeCombo.getSelectionIndex();
        String authType = selection >= 0 ? authTypes[selection] : AUTH_ANONYMOUS;

        boolean showCredentials = AUTH_STATIC.equals(authType);
        boolean showToken = AUTH_TOKEN.equals(authType);
        boolean showSaFile = AUTH_SERVICE_ACCOUNT.equals(authType);

        setGroupVisible(credentialsGroup, showCredentials);
        setGroupVisible(tokenGroup, showToken);
        setGroupVisible(saFileGroup, showSaFile);

        // Force full layout refresh
        Composite parent = credentialsGroup.getParent();
        parent.layout(true, true);
        parent.getParent().layout(true, true);
    }

    private void setGroupVisible(Composite group, boolean visible) {
        group.setVisible(visible);
        ((GridData) group.getLayoutData()).exclude = !visible;
    }

    private void updateUrl() {
        if (hostText == null || hostText.isDisposed()) {
            return;
        }

        String host = hostText.getText().trim();
        String port = portText.getText().trim();
        String database = databaseText.getText().trim();
        boolean secure = useSecureCheckbox.getSelection();

        StringBuilder url = new StringBuilder();
        url.append("jdbc:ydb:");
        url.append(secure ? "grpcs://" : "grpc://");
        url.append(host);
        if (!CommonUtils.isEmpty(port)) {
            url.append(":").append(port);
        }
        if (!CommonUtils.isEmpty(database)) {
            // Normalize: ensure single leading slash
            database = "/" + database.replaceAll("^/+", "");
            url.append(database);
        }

        // Add auth params
        int selection = authTypeCombo.getSelectionIndex();
        String authType = selection >= 0 ? authTypes[selection] : AUTH_ANONYMOUS;

        StringBuilder params = new StringBuilder();

        switch (authType) {
            case AUTH_STATIC:
                String user = userText.getText().trim();
                String password = passwordText.getText();
                if (!CommonUtils.isEmpty(user)) {
                    params.append("user=").append(user);
                    if (!CommonUtils.isEmpty(password)) {
                        params.append("&password=").append(password);
                    }
                }
                break;
            case AUTH_TOKEN:
                String token = tokenText.getText().trim();
                if (!CommonUtils.isEmpty(token)) {
                    params.append("token=").append(token);
                }
                break;
            case AUTH_SERVICE_ACCOUNT:
                String saFile = saFileText.getText().trim();
                if (!CommonUtils.isEmpty(saFile)) {
                    params.append("saKeyFile=").append(URLEncoder.encode(saFile, StandardCharsets.UTF_8));
                }
                break;
            case AUTH_METADATA:
                params.append("useMetadata=true");
                break;
        }

        if (params.length() > 0) {
            url.append("?").append(params);
        }

        site.getActiveDataSource().getConnectionConfiguration().setUrl(url.toString());
    }

    @Override
    public boolean isComplete() {
        if (hostText == null || hostText.isDisposed()
            || databaseText == null || databaseText.isDisposed()) {
            return false;
        }
        return !CommonUtils.isEmpty(hostText.getText()) &&
               !CommonUtils.isEmpty(databaseText.getText());
    }

    @Override
    public void loadSettings() {
        super.loadSettings();

        DBPConnectionConfiguration connectionInfo = site.getActiveDataSource().getConnectionConfiguration();

        // Load host/port/database
        String hostName = connectionInfo.getHostName();
        String hostPort = connectionInfo.getHostPort();
        String databaseName = connectionInfo.getDatabaseName();

        if (CommonUtils.isEmpty(hostName)) {
            hostName = "localhost";
        }
        if (CommonUtils.isEmpty(hostPort)) {
            hostPort = "2135";
        }
        if (CommonUtils.isEmpty(databaseName)) {
            databaseName = "/local";
        }

        hostText.setText(hostName);
        portText.setText(hostPort);
        databaseText.setText(databaseName);

        // Load auth settings
        String authType = connectionInfo.getProviderProperty(PROP_AUTH_TYPE);
        if (CommonUtils.isEmpty(authType)) {
            authType = AUTH_ANONYMOUS;
        }

        int authIndex = 0;
        for (int i = 0; i < authTypes.length; i++) {
            if (authTypes[i].equals(authType)) {
                authIndex = i;
                break;
            }
        }
        authTypeCombo.select(authIndex);

        // Load credentials
        String userName = connectionInfo.getUserName();
        String userPassword = connectionInfo.getUserPassword();
        if (!CommonUtils.isEmpty(userName)) {
            userText.setText(userName);
        }
        if (!CommonUtils.isEmpty(userPassword)) {
            passwordText.setText(userPassword);
        }

        // Load token
        String token = connectionInfo.getProviderProperty(PROP_TOKEN);
        if (!CommonUtils.isEmpty(token)) {
            tokenText.setText(token);
        }

        // Load SA file
        String saFile = connectionInfo.getProviderProperty(PROP_SA_FILE);
        if (!CommonUtils.isEmpty(saFile)) {
            saFileText.setText(saFile);
        }

        // Load monitoring URL
        String monitoringUrl = connectionInfo.getProviderProperty(PROP_MONITORING_URL);
        if (!CommonUtils.isEmpty(monitoringUrl)) {
            monitoringUrlText.setText(monitoringUrl);
        }

        // Load secure setting
        String useSecure = connectionInfo.getProviderProperty(PROP_USE_SECURE);
        useSecureCheckbox.setSelection(CommonUtils.isEmpty(useSecure) || CommonUtils.toBoolean(useSecure));

        // Load SSL certificate
        String sslCertificate = connectionInfo.getProviderProperty(PROP_SSL_CERTIFICATE);
        if (!CommonUtils.isEmpty(sslCertificate)) {
            sslCertificateText.setText(sslCertificate);
        }

        // Load autocomplete API setting
        String autocompleteEnabled = connectionInfo.getProviderProperty(PROP_AUTOCOMPLETE_API_ENABLED);
        autocompleteApiCheckbox.setSelection(CommonUtils.isEmpty(autocompleteEnabled) || CommonUtils.toBoolean(autocompleteEnabled));

        // Load data engineering mode setting (enabled by default)
        String dataEngineeringMode = connectionInfo.getProviderProperty(PROP_DATA_ENGINEERING_MODE);
        dataEngineeringModeCheckbox.setSelection(CommonUtils.isEmpty(dataEngineeringMode) || CommonUtils.toBoolean(dataEngineeringMode));

        updateAuthControls();
        updateSslCertControls();
        updateUrl();
    }

    @Override
    public void saveSettings(DBPDataSourceContainer dataSource) {
        DBPConnectionConfiguration connectionInfo = dataSource.getConnectionConfiguration();

        connectionInfo.setHostName(hostText.getText().trim());
        connectionInfo.setHostPort(portText.getText().trim());
        connectionInfo.setDatabaseName(databaseText.getText().trim());

        int selection = authTypeCombo.getSelectionIndex();
        String authType = selection >= 0 ? authTypes[selection] : AUTH_ANONYMOUS;
        connectionInfo.setProviderProperty(PROP_AUTH_TYPE, authType);

        // Clear all auth-related JDBC properties first
        connectionInfo.getProperties().remove("token");
        connectionInfo.getProperties().remove("saFile");
        connectionInfo.getProperties().remove("saKeyFile");
        connectionInfo.getProperties().remove("useMetadata");
        connectionInfo.getProperties().remove("user");
        connectionInfo.getProperties().remove("password");

        // Save credentials and set JDBC properties based on auth type
        switch (authType) {
            case AUTH_STATIC:
                String user = userText.getText().trim();
                String password = passwordText.getText();
                connectionInfo.setUserName(user);
                connectionInfo.setUserPassword(password);
                connectionInfo.setProperty("user", user);
                connectionInfo.setProperty("password", password);
                break;
            case AUTH_TOKEN:
                String token = tokenText.getText().trim();
                connectionInfo.setProviderProperty(PROP_TOKEN, token);
                connectionInfo.setProperty("token", token);
                break;
            case AUTH_SERVICE_ACCOUNT:
                String saFile = saFileText.getText().trim();
                connectionInfo.setProviderProperty(PROP_SA_FILE, saFile);
                connectionInfo.setProperty("saKeyFile", saFile);
                break;
            case AUTH_METADATA:
                connectionInfo.setProperty("useMetadata", "true");
                break;
            default:
                // Anonymous or metadata - no additional user/password
                connectionInfo.setUserName(null);
                connectionInfo.setUserPassword(null);
                break;
        }

        // Store auth type for UI
        if (!AUTH_TOKEN.equals(authType)) {
            connectionInfo.setProviderProperty(PROP_TOKEN, null);
        }
        if (!AUTH_SERVICE_ACCOUNT.equals(authType)) {
            connectionInfo.setProviderProperty(PROP_SA_FILE, null);
        }

        // Save monitoring URL
        String monitoringUrl = monitoringUrlText.getText().trim();
        if (!CommonUtils.isEmpty(monitoringUrl)) {
            connectionInfo.setProviderProperty(PROP_MONITORING_URL, monitoringUrl);
        } else {
            connectionInfo.setProviderProperty(PROP_MONITORING_URL, null);
        }

        // Save secure setting
        connectionInfo.setProviderProperty(PROP_USE_SECURE, String.valueOf(useSecureCheckbox.getSelection()));

        // Save SSL certificate
        String sslCertificate = sslCertificateText.getText().trim();
        if (!CommonUtils.isEmpty(sslCertificate)) {
            connectionInfo.setProviderProperty(PROP_SSL_CERTIFICATE, sslCertificate);
            connectionInfo.setProperty("secureConnectionCertificate", sslCertificate);
        } else {
            connectionInfo.setProviderProperty(PROP_SSL_CERTIFICATE, null);
            connectionInfo.getProperties().remove("secureConnectionCertificate");
        }

        // Save autocomplete API setting
        connectionInfo.setProviderProperty(PROP_AUTOCOMPLETE_API_ENABLED, String.valueOf(autocompleteApiCheckbox.getSelection()));

        // Save data engineering mode setting
        connectionInfo.setProviderProperty(PROP_DATA_ENGINEERING_MODE, String.valueOf(dataEngineeringModeCheckbox.getSelection()));

        // Update URL
        updateUrl();

        super.saveSettings(dataSource);
    }

    @Override
    public IDialogPage[] getDialogPages(boolean ext498, boolean forceCreate) {
        return new IDialogPage[]{
            new DriverPropertiesDialogPage(this)
        };
    }
}
