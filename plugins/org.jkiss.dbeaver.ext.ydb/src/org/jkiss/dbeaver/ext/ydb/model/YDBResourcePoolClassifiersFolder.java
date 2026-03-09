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
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSFolder;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Container for YDB resource pool classifiers.
 * Resource pool classifiers are used for workload management in YDB.
 * Data is loaded from .sys/resource_pool_classifiers system view.
 */
public class YDBResourcePoolClassifiersFolder implements DBSFolder, DBPRefreshableObject {

    private static final Log log = Log.getLog(YDBResourcePoolClassifiersFolder.class);

    private static final String RESOURCE_POOL_CLASSIFIERS_QUERY =
        org.jkiss.dbeaver.ext.ydb.core.YDBSysQueries.RESOURCE_POOL_CLASSIFIERS_QUERY;

    private final YDBDataSource dataSource;
    private List<YDBResourcePoolClassifier> classifiers;
    private boolean classifiersLoaded = false;

    public YDBResourcePoolClassifiersFolder(@NotNull YDBDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @NotNull
    @Override
    public String getName() {
        return "Resource Pool Classifiers";
    }

    @Override
    public String getDescription() {
        return "Resource pool classifiers for workload management";
    }

    @NotNull
    @Override
    public YDBDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public DBSObject getParentObject() {
        return dataSource;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @NotNull
    @Override
    public Collection<DBSObject> getChildrenObjects(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<DBSObject> children = new ArrayList<>();
        children.addAll(getResourcePoolClassifiers(monitor));
        return children;
    }

    /**
     * Load resource pool classifiers from .sys/resource_pool_classifiers system view.
     */
    @Association
    @NotNull
    public synchronized List<YDBResourcePoolClassifier> getResourcePoolClassifiers(@NotNull DBRProgressMonitor monitor) throws DBException {
        log.debug("getResourcePoolClassifiers() called, classifiersLoaded=" + classifiersLoaded);
        if (!classifiersLoaded) {
            loadResourcePoolClassifiers(monitor);
        }
        log.debug("Returning " + (classifiers != null ? classifiers.size() : 0) + " resource pool classifiers");
        return classifiers != null ? classifiers : List.of();
    }

    private void loadResourcePoolClassifiers(@NotNull DBRProgressMonitor monitor) throws DBException {
        monitor.beginTask("Loading resource pool classifiers", 1);
        try {
            classifiers = new ArrayList<>();

            // Query resource pool classifiers from .sys/resource_pool_classifiers system view
            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load resource pool classifiers")) {
                try (JDBCPreparedStatement stmt = session.prepareStatement(RESOURCE_POOL_CLASSIFIERS_QUERY)) {
                    try (JDBCResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            // The first column should be the classifier name
                            String classifierName = rs.getString(1);
                            if (classifierName != null && !classifierName.isEmpty()) {
                                YDBResourcePoolClassifier classifier = new YDBResourcePoolClassifier(dataSource, classifierName);
                                classifiers.add(classifier);
                                log.debug("Added resource pool classifier: " + classifierName);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                log.debug("Failed to load resource pool classifiers: " + e.getMessage());
                // Resource pool classifiers may not be available in all YDB configurations
            } catch (DBCException e) {
                log.debug("Failed to open session for loading resource pool classifiers: " + e.getMessage());
            }

            // Sort classifiers by name
            classifiers.sort(Comparator.comparing(YDBResourcePoolClassifier::getName, String.CASE_INSENSITIVE_ORDER));
            classifiersLoaded = true;

        } finally {
            monitor.done();
        }
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        classifiersLoaded = false;
        classifiers = null;
        return this;
    }
}
