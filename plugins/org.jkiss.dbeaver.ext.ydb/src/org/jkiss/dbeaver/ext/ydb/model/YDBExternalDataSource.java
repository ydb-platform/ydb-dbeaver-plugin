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
package org.jkiss.dbeaver.ext.ydb.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.SessionRetryContext;

import java.util.Map;

/**
 * YDB External Data Source.
 */
public class YDBExternalDataSource implements DBSObject, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBExternalDataSource.class);

    private final DBSObject parent;
    private final String name;
    private final String fullPath;

    private boolean propertiesLoaded = false;
    private String sourceType;
    private String location;
    private String databaseName;
    private String authMethod;
    private String tokenSecretPath;
    private String installation;

    public YDBExternalDataSource(@NotNull DBSObject parent, @NotNull String fullPath) {
        this.parent = parent;
        this.fullPath = fullPath;
        if (fullPath.contains("/")) {
            this.name = fullPath.substring(fullPath.lastIndexOf('/') + 1);
        } else {
            this.name = fullPath;
        }
    }

    public YDBExternalDataSource(@NotNull DBSObject parent, @NotNull String name, @NotNull String fullPath) {
        this.parent = parent;
        this.name = name;
        this.fullPath = fullPath;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return name;
    }

    @NotNull
    @Property(viewable = true, order = 2)
    public String getFullPath() {
        return fullPath;
    }

    @Nullable
    @Property(viewable = true, order = 3)
    public String getSourceType() {
        return sourceType;
    }

    @Nullable
    @Property(viewable = true, order = 4)
    public String getLocation() {
        return location;
    }

    @Nullable
    @Property(order = 5)
    public String getDatabaseName() {
        return databaseName;
    }

    @Nullable
    @Property(order = 6)
    public String getAuthMethod() {
        return authMethod;
    }

    @Nullable
    @Property(order = 7)
    public String getTokenSecretPath() {
        return tokenSecretPath;
    }

    @Nullable
    @Property(order = 8)
    public String getInstallation() {
        return installation;
    }

    synchronized void loadProperties(
        @NotNull GrpcTransport transport,
        @NotNull SessionRetryContext retryCtx,
        @NotNull String prefixPath
    ) {
        if (propertiesLoaded) {
            return;
        }
        String absolutePath = prefixPath + "/" + fullPath;
        YdbTable.DescribeTableResult result = YDBDescribeHelper.describeTable(
            transport, retryCtx, absolutePath);
        if (result != null) {
            parseAttributes(result.getAttributesMap());
        }
        propertiesLoaded = true;
    }

    private void parseAttributes(@NotNull Map<String, String> attributes) {
        sourceType = attributes.get("SOURCE_TYPE");
        location = attributes.get("LOCATION");
        databaseName = attributes.get("DATABASE_NAME");
        authMethod = attributes.get("AUTH_METHOD");
        tokenSecretPath = attributes.get("TOKEN_SECRET_NAME");
        installation = attributes.get("INSTALLATION");

        if (sourceType == null) {
            sourceType = attributes.get("source_type");
        }
        if (location == null) {
            location = attributes.get("location");
        }
        if (databaseName == null) {
            databaseName = attributes.get("database_name");
        }
        if (authMethod == null) {
            authMethod = attributes.get("auth_method");
        }
        if (tokenSecretPath == null) {
            tokenSecretPath = attributes.get("token_secret_name");
        }
        if (installation == null) {
            installation = attributes.get("installation");
        }

        log.debug("External data source attributes: " + attributes);
    }

    @Nullable
    @Override
    public String getDescription() {
        return null;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return parent.getDataSource();
    }

    @Override
    public DBSObject getParentObject() {
        return parent;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        propertiesLoaded = false;
        sourceType = null;
        location = null;
        databaseName = null;
        authMethod = null;
        tokenSecretPath = null;
        installation = null;
        return this;
    }

    @Override
    public String toString() {
        return name;
    }
}
