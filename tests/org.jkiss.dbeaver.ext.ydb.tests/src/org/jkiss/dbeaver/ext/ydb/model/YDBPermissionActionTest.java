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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for YDBPermissionHolder.PermissionAction static factories
 * and YDBPermissionHolder.PermissionEntry.
 */
public class YDBPermissionActionTest {

    // PermissionEntry tests

    @Test
    public void testPermissionEntrySubject() {
        YDBPermissionHolder.PermissionEntry entry =
            new YDBPermissionHolder.PermissionEntry("user@domain", List.of("ydb.tables.read"));
        assertEquals("user@domain", entry.getSubject());
    }

    @Test
    public void testPermissionEntryPermissions() {
        List<String> perms = List.of("ydb.tables.read", "ydb.tables.write");
        YDBPermissionHolder.PermissionEntry entry =
            new YDBPermissionHolder.PermissionEntry("user", perms);
        assertEquals(perms, entry.getPermissionNames());
    }

    // PermissionAction.grant()

    @Test
    public void testGrantType() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.grant("user", List.of("ydb.tables.read"));
        assertEquals(YDBPermissionHolder.ActionType.GRANT, action.getType());
    }

    @Test
    public void testGrantSubject() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.grant("user@domain", List.of("perm"));
        assertEquals("user@domain", action.getSubject());
    }

    @Test
    public void testGrantPermissions() {
        Collection<String> perms = Arrays.asList("ydb.tables.read", "ydb.tables.write");
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.grant("user", perms);
        assertEquals(perms, action.getPermissionNames());
        assertNull(action.getNewOwner());
    }

    // PermissionAction.revoke()

    @Test
    public void testRevokeType() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.revoke("user", List.of("perm"));
        assertEquals(YDBPermissionHolder.ActionType.REVOKE, action.getType());
    }

    @Test
    public void testRevokeFields() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.revoke("user", List.of("perm1"));
        assertEquals("user", action.getSubject());
        assertNotNull(action.getPermissionNames());
        assertNull(action.getNewOwner());
    }

    // PermissionAction.set()

    @Test
    public void testSetType() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.set("user", List.of("perm"));
        assertEquals(YDBPermissionHolder.ActionType.SET, action.getType());
    }

    @Test
    public void testSetFields() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.set("user", List.of("perm1", "perm2"));
        assertEquals("user", action.getSubject());
        assertEquals(2, action.getPermissionNames().size());
        assertNull(action.getNewOwner());
    }

    // PermissionAction.changeOwner()

    @Test
    public void testChangeOwnerType() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.changeOwner("newOwner@domain");
        assertEquals(YDBPermissionHolder.ActionType.CHANGE_OWNER, action.getType());
    }

    @Test
    public void testChangeOwnerNewOwner() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.changeOwner("newOwner@domain");
        assertEquals("newOwner@domain", action.getNewOwner());
    }

    @Test
    public void testChangeOwnerSubjectNull() {
        YDBPermissionHolder.PermissionAction action =
            YDBPermissionHolder.PermissionAction.changeOwner("owner");
        assertNull(action.getSubject());
        assertNull(action.getPermissionNames());
    }
}
