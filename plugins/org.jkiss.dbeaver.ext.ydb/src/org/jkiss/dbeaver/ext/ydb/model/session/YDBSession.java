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
package org.jkiss.dbeaver.ext.ydb.model.session;

import org.jkiss.dbeaver.model.admin.sessions.AbstractServerSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * YDB Query Session from .sys/query_sessions system view.
 *
 * The query_sessions view contains information about active query sessions in YDB.
 *
 * Available columns:
 * - SessionId, NodeId, State, Query, QueryCount
 * - QueryStartAt, SessionStartAt, StateChangeAt
 * - ApplicationName, ClientAddress, ClientPID
 * - ClientSdkBuildInfo, ClientUserAgent, UserSID
 */
public class YDBSession extends AbstractServerSession {

    private final String sessionId;
    private final String nodeId;
    private final String state;
    private final String query;
    private final Long queryCount;
    private final Timestamp queryStartAt;
    private final Timestamp sessionStartAt;
    private final Timestamp stateChangeAt;
    private final String applicationName;
    private final String clientAddress;
    private final String clientPid;
    private final String clientSdkBuildInfo;
    private final String clientUserAgent;
    private final String userSid;

    public YDBSession(ResultSet dbResult) {
        // YDB uses PascalCase column names
        this.sessionId = JDBCUtils.safeGetString(dbResult, "SessionId");
        this.nodeId = JDBCUtils.safeGetString(dbResult, "NodeId");
        this.state = JDBCUtils.safeGetString(dbResult, "State");
        this.query = JDBCUtils.safeGetString(dbResult, "Query");
        this.queryCount = JDBCUtils.safeGetLong(dbResult, "QueryCount");
        this.queryStartAt = JDBCUtils.safeGetTimestamp(dbResult, "QueryStartAt");
        this.sessionStartAt = JDBCUtils.safeGetTimestamp(dbResult, "SessionStartAt");
        this.stateChangeAt = JDBCUtils.safeGetTimestamp(dbResult, "StateChangeAt");
        this.applicationName = JDBCUtils.safeGetString(dbResult, "ApplicationName");
        this.clientAddress = JDBCUtils.safeGetString(dbResult, "ClientAddress");
        this.clientPid = JDBCUtils.safeGetString(dbResult, "ClientPID");
        this.clientSdkBuildInfo = JDBCUtils.safeGetString(dbResult, "ClientSdkBuildInfo");
        this.clientUserAgent = JDBCUtils.safeGetString(dbResult, "ClientUserAgent");
        this.userSid = JDBCUtils.safeGetString(dbResult, "UserSID");
    }

    @Override
    @Property(name = "Session ID", viewable = true, order = 1)
    public String getSessionId() {
        return sessionId;
    }

    @Property(name = "Node ID", viewable = true, order = 2)
    public String getNodeId() {
        return nodeId;
    }

    @Property(name = "State", viewable = true, order = 3)
    public String getState() {
        return state;
    }

    @Override
    @Property(name = "Active Query", viewable = true, order = 4)
    public String getActiveQuery() {
        return query;
    }

    @Property(name = "Query Count", viewable = true, order = 5)
    public Long getQueryCount() {
        return queryCount;
    }

    @Override
    public Object getActiveQueryId() {
        return sessionId; // YDB doesn't have separate query ID, use session ID
    }

    @Property(name = "Query Start", viewable = true, order = 6)
    public Timestamp getQueryStartAt() {
        return queryStartAt;
    }

    @Property(name = "Session Start", viewable = true, order = 7)
    public Timestamp getSessionStartAt() {
        return sessionStartAt;
    }

    @Property(name = "State Change", viewable = false, order = 8)
    public Timestamp getStateChangeAt() {
        return stateChangeAt;
    }

    @Property(name = "Application", viewable = true, order = 9)
    public String getApplicationName() {
        return applicationName;
    }

    @Property(name = "Client Address", viewable = true, order = 10)
    public String getClientAddress() {
        return clientAddress;
    }

    @Property(name = "Client PID", viewable = false, order = 11)
    public String getClientPid() {
        return clientPid;
    }

    @Property(name = "SDK Build Info", viewable = false, order = 12)
    public String getClientSdkBuildInfo() {
        return clientSdkBuildInfo;
    }

    @Property(name = "User Agent", viewable = false, order = 13)
    public String getClientUserAgent() {
        return clientUserAgent;
    }

    @Property(name = "User SID", viewable = true, order = 14)
    public String getUserSid() {
        return userSid;
    }

    @Override
    public String toString() {
        return sessionId + "@" + nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YDBSession that = (YDBSession) o;
        return Objects.equals(sessionId, that.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId);
    }
}
