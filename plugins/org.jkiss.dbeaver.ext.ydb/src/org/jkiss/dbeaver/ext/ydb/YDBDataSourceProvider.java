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
package org.jkiss.dbeaver.ext.ydb;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.GenericMetaModelRegistry;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.ydb.model.YDBDataSource;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPDataSourceProvider;
import org.jkiss.dbeaver.model.app.DBPPlatform;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.CommonUtils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * YDB data source provider.
 *
 * Deliberately implements {@link DBPDataSourceProvider} via the raw interface
 * type instead of extending {@code GenericDataSourceProvider}. Upstream
 * DBeaver changed the generic provider's constructor signature (no-arg → one
 * accepting {@code Class<?>}) in a non-backward-compatible way; since this
 * plugin ships externally and has no control over the host DBeaver build,
 * extending that class makes the plugin fail at instantiation against either
 * older or newer runtimes depending on which variant we compile against.
 *
 * The raw-interface binding erases identically across DBeaver versions and
 * keeps the plugin loadable on any runtime that provides the
 * {@code DBPDataSourceProvider} contract.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class YDBDataSourceProvider implements DBPDataSourceProvider {

    private static final String PROP_AUTH_TYPE = "ydb.authType";
    private static final String PROP_TOKEN = "ydb.token";
    private static final String PROP_SA_FILE = "ydb.saFile";
    private static final String PROP_USE_SECURE = "ydb.useSecure";
    private static final String PROP_SSL_CERTIFICATE = "ydb.sslCertificate";

    public YDBDataSourceProvider() {
    }

    @Override
    public void init(@NotNull DBPPlatform platform) {
        // nothing to initialize
    }

    @Override
    public long getFeatures() {
        return FEATURE_CATALOGS;
    }

    @NotNull
    @Override
    public DBPPropertyDescriptor[] getConnectionProperties(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBPDriver driver,
        @NotNull DBPConnectionConfiguration connectionInfo
    ) throws DBException {
        return new DBPPropertyDescriptor[0];
    }

    @NotNull
    @Override
    public Class<? extends DBPDataSource> getDataSourceClass() {
        return YDBDataSource.class;
    }

    @NotNull
    @Override
    public String getConnectionURL(@NotNull DBPDriver driver, @NotNull DBPConnectionConfiguration connectionInfo) {
        String host = connectionInfo.getHostName();
        if (CommonUtils.isEmpty(host)) {
            return "";
        }

        String port = connectionInfo.getHostPort();
        String database = connectionInfo.getDatabaseName();
        String secureStr = connectionInfo.getProviderProperty(PROP_USE_SECURE);
        boolean secure = CommonUtils.isEmpty(secureStr) || CommonUtils.toBoolean(secureStr);

        StringBuilder url = new StringBuilder();
        url.append("jdbc:ydb:");
        url.append(secure ? "grpcs://" : "grpc://");
        url.append(host);
        if (!CommonUtils.isEmpty(port)) {
            url.append(":").append(port);
        }
        if (!CommonUtils.isEmpty(database)) {
            database = "/" + database.replaceAll("^/+", "");
            url.append(database);
        }

        StringBuilder params = new StringBuilder();
        String authType = connectionInfo.getProviderProperty(PROP_AUTH_TYPE);
        if (!CommonUtils.isEmpty(authType)) {
            switch (authType) {
                case "static":
                    String user = connectionInfo.getUserName();
                    String password = connectionInfo.getUserPassword();
                    if (!CommonUtils.isEmpty(user)) {
                        params.append("user=").append(user);
                        if (!CommonUtils.isEmpty(password)) {
                            params.append("&password=").append(password);
                        }
                    }
                    break;
                case "token":
                    String token = connectionInfo.getProviderProperty(PROP_TOKEN);
                    if (!CommonUtils.isEmpty(token)) {
                        params.append("token=").append(token);
                    }
                    break;
                case "saFile":
                    String saFile = connectionInfo.getProviderProperty(PROP_SA_FILE);
                    if (!CommonUtils.isEmpty(saFile)) {
                        params.append("saKeyFile=").append(URLEncoder.encode(saFile, StandardCharsets.UTF_8));
                    }
                    break;
                case "metadata":
                    params.append("useMetadata=true");
                    break;
            }
        }

        String sslCert = connectionInfo.getProviderProperty(PROP_SSL_CERTIFICATE);
        if (!CommonUtils.isEmpty(sslCert)) {
            if (params.length() > 0) {
                params.append("&");
            }
            params.append("secureConnectionCertificate=").append(URLEncoder.encode(sslCert, StandardCharsets.UTF_8));
        }

        if (params.length() > 0) {
            url.append("?").append(params);
        }

        return url.toString();
    }

    @NotNull
    @Override
    public DBPDataSource openDataSource(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBPDataSourceContainer container
    ) throws DBException {
        GenericMetaModel metaModel = GenericMetaModelRegistry.getInstance().getMetaModel(container);
        return new YDBDataSource(monitor, container, metaModel);
    }
}
