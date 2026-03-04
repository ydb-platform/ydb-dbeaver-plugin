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
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSFolder;
import org.jkiss.dbeaver.model.struct.DBSObject;

import tech.ydb.jdbc.YdbConnection;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Virtual folder for hierarchical view organization.
 */
public class YDBViewFolder implements DBSFolder, YDBPermissionHolder {

    private static final Log log = Log.getLog(YDBViewFolder.class);

    private final DBSObject owner;
    private final YDBViewFolder parentFolder;
    private final String name;
    private final String fullPath;
    private final Map<String, YDBViewFolder> subFolders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final List<YDBView> entries = new ArrayList<>();

    private String permOwner;
    private List<PermissionEntry> explicitPermissions = new ArrayList<>();
    private List<PermissionEntry> effectivePermissions = new ArrayList<>();
    private boolean permissionsLoaded;

    public YDBViewFolder(
        @NotNull DBSObject owner,
        @Nullable YDBViewFolder parentFolder,
        @NotNull String name
    ) {
        this.owner = owner;
        this.parentFolder = parentFolder;
        this.name = name;
        this.fullPath = parentFolder != null ? parentFolder.getFullPath() + "/" + name : name;
    }

    @NotNull @Override @Property(viewable = true, order = 1)
    public String getName() { return name; }

    @NotNull public String getFullPath() { return fullPath; }

    @Nullable @Override public String getDescription() { return null; }
    @Override public boolean isPersisted() { return true; }

    @Nullable @Override
    public DBSObject getParentObject() { return parentFolder != null ? parentFolder : owner; }

    @NotNull @Override
    public DBPDataSource getDataSource() { return owner.getDataSource(); }

    @NotNull @Override
    public Collection<DBSObject> getChildrenObjects(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<DBSObject> children = new ArrayList<>();
        children.addAll(subFolders.values());
        children.addAll(entries);
        return children;
    }

    @Association @NotNull
    public Collection<YDBViewFolder> getSubFolders() { return subFolders.values(); }

    @Association @NotNull
    public List<YDBView> getViews() {
        entries.sort(Comparator.comparing(YDBView::getName, String.CASE_INSENSITIVE_ORDER));
        return entries;
    }

    public void addEntry(@NotNull YDBView entry) { entries.add(entry); }

    @NotNull
    public YDBViewFolder getOrCreateSubFolder(@NotNull String folderName) {
        return subFolders.computeIfAbsent(folderName, n -> new YDBViewFolder(owner, this, n));
    }

    // --- YDBPermissionHolder ---

    @Nullable @Override public String getOwner() { return permOwner; }
    @NotNull @Override public List<PermissionEntry> getExplicitPermissions() { return explicitPermissions; }
    @NotNull @Override public List<PermissionEntry> getEffectivePermissions() { return effectivePermissions; }

    @Override
    public void ensurePermissionsLoaded(@NotNull DBRProgressMonitor monitor) throws DBException {
        if (permissionsLoaded) return;
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Load permissions")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                loadPermissions(ctx.getSchemeClient(), ctx.getPrefixPath());
            }
        } catch (Exception e) {
            log.debug("Failed to load permissions: " + e.getMessage());
        }
    }

    synchronized void loadPermissions(@NotNull SchemeClient schemeClient, @NotNull String prefixPath) {
        if (permissionsLoaded) return;
        String absolutePath = prefixPath + "/" + fullPath;
        SchemeOperationProtos.Entry entry = YDBDescribeHelper.describePath(schemeClient, absolutePath);
        if (entry != null) {
            permOwner = entry.getOwner();
            explicitPermissions = YDBDescribeHelper.toPermissionEntries(entry.getPermissionsList());
            effectivePermissions = YDBDescribeHelper.toPermissionEntries(entry.getEffectivePermissionsList());
        }
        permissionsLoaded = true;
    }

    @Override
    public void resetPermissions() {
        permissionsLoaded = false;
        permOwner = null;
        explicitPermissions = new ArrayList<>();
        effectivePermissions = new ArrayList<>();
    }

    @Override
    public void modifyPermissions(@NotNull DBRProgressMonitor monitor, @NotNull List<PermissionAction> actions,
                                  boolean clearPermissions, @Nullable Boolean interruptInheritance) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Modify permissions")) {
            Connection conn = session.getOriginal();
            if (conn.isWrapperFor(YdbConnection.class)) {
                YdbConnection ydbConn = conn.unwrap(YdbConnection.class);
                YdbContext ctx = ydbConn.getCtx();
                String absolutePath = ctx.getPrefixPath() + "/" + fullPath;
                boolean ok = YDBDescribeHelper.modifyPermissions(
                    ctx.getGrpcTransport(), absolutePath, actions, clearPermissions, interruptInheritance);
                if (!ok) throw new DBException("Failed to modify permissions on " + absolutePath);
                resetPermissions();
            }
        } catch (SQLException e) {
            throw new DBException("Failed to modify permissions", e);
        }
    }

    @Override public String toString() { return name; }
}
