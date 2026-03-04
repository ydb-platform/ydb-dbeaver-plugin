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
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.util.Collection;
import java.util.List;

/**
 * Interface for YDB objects that have ACL permissions.
 * Provides access to explicit and effective permissions loaded from SchemeClient.describePath().
 */
public interface YDBPermissionHolder extends DBSObject {

    /**
     * A single permission entry: subject (SID) and its permission names.
     */
    class PermissionEntry {
        private final String subject;
        private final List<String> permissionNames;

        public PermissionEntry(@NotNull String subject, @NotNull List<String> permissionNames) {
            this.subject = subject;
            this.permissionNames = permissionNames;
        }

        @NotNull
        public String getSubject() {
            return subject;
        }

        @NotNull
        public List<String> getPermissionNames() {
            return permissionNames;
        }
    }

    enum ActionType {
        GRANT,
        REVOKE,
        SET,
        CHANGE_OWNER
    }

    /**
     * A permission modification action (grant/revoke/set for a subject, or change owner).
     */
    class PermissionAction {
        private final ActionType type;
        @Nullable
        private final String subject;
        @Nullable
        private final Collection<String> permissionNames;
        @Nullable
        private final String newOwner;

        private PermissionAction(ActionType type, @Nullable String subject,
                                 @Nullable Collection<String> permissionNames, @Nullable String newOwner) {
            this.type = type;
            this.subject = subject;
            this.permissionNames = permissionNames;
            this.newOwner = newOwner;
        }

        public static PermissionAction grant(@NotNull String subject, @NotNull Collection<String> permissions) {
            return new PermissionAction(ActionType.GRANT, subject, permissions, null);
        }

        public static PermissionAction revoke(@NotNull String subject, @NotNull Collection<String> permissions) {
            return new PermissionAction(ActionType.REVOKE, subject, permissions, null);
        }

        public static PermissionAction set(@NotNull String subject, @NotNull Collection<String> permissions) {
            return new PermissionAction(ActionType.SET, subject, permissions, null);
        }

        public static PermissionAction changeOwner(@NotNull String newOwner) {
            return new PermissionAction(ActionType.CHANGE_OWNER, null, null, newOwner);
        }

        public ActionType getType() { return type; }
        @Nullable public String getSubject() { return subject; }
        @Nullable public Collection<String> getPermissionNames() { return permissionNames; }
        @Nullable public String getNewOwner() { return newOwner; }
    }

    @NotNull
    List<PermissionEntry> getExplicitPermissions();

    @NotNull
    List<PermissionEntry> getEffectivePermissions();

    @Nullable
    String getOwner();

    void ensurePermissionsLoaded(@NotNull DBRProgressMonitor monitor) throws DBException;

    void resetPermissions();

    /**
     * Modify permissions on this object.
     *
     * @param monitor progress monitor
     * @param actions list of grant/revoke/set/change_owner actions
     * @param clearPermissions if true, clear all permissions first
     * @param interruptInheritance if non-null, set interrupt_inheritance flag
     * @throws DBException on failure
     */
    void modifyPermissions(
        @NotNull DBRProgressMonitor monitor,
        @NotNull List<PermissionAction> actions,
        boolean clearPermissions,
        @Nullable Boolean interruptInheritance
    ) throws DBException;
}
