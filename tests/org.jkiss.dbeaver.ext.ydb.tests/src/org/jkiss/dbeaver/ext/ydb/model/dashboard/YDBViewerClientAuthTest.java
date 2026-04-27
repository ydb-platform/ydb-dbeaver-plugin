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
package org.jkiss.dbeaver.ext.ydb.model.dashboard;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link YDBViewerClient} authentication logic:
 * Set-Cookie parsing, 1-hour session TTL, and request-header selection.
 * No network calls — exercised through reflection.
 */
public class YDBViewerClientAuthTest {

    // ── Set-Cookie parsing ───────────────────────────────────────────────────

    @Test
    public void extractSessionCookie_simple() {
        assertEquals("abc.def.ghi",
            YDBViewerClient.extractSessionCookie("ydb_session_id=abc.def.ghi"));
    }

    @Test
    public void extractSessionCookie_withAttributes() {
        String header = "ydb_session_id=eyJhbGciOiJQUzI1NiJ9.payload.sig; Max-Age=43199; Path=/; HttpOnly";
        assertEquals("eyJhbGciOiJQUzI1NiJ9.payload.sig",
            YDBViewerClient.extractSessionCookie(header));
    }

    @Test
    public void extractSessionCookie_amongOtherCookies() {
        String header = "other=foo; ydb_session_id=value123; Path=/";
        assertEquals("value123", YDBViewerClient.extractSessionCookie(header));
    }

    @Test
    public void extractSessionCookie_missing() {
        assertNull(YDBViewerClient.extractSessionCookie("session=foo; other=bar"));
        assertNull(YDBViewerClient.extractSessionCookie(null));
        assertNull(YDBViewerClient.extractSessionCookie(""));
    }

    @Test
    public void extractSessionCookie_doesNotMatchSubstring() {
        // "not_ydb_session_id" should not match
        assertNull(YDBViewerClient.extractSessionCookie("not_ydb_session_id=foo"));
    }

    // ── Header selection: OAuth vs Cookie vs none ────────────────────────────

    @Test
    public void applyAuth_tokenWins() throws Exception {
        YDBViewerClient client = new YDBViewerClient(
            "http://localhost:8765", "/local", "my-oauth-token", "user", "pwd");
        HttpURLConnection conn = openDummyConnection();
        invokeApplyAuth(client, conn);
        assertEquals("OAuth my-oauth-token", conn.getRequestProperty("Authorization"));
        assertNull(conn.getRequestProperty("Cookie"));
    }

    @Test
    public void applyAuth_cookieWhenSessionCached() throws Exception {
        YDBViewerClient client = new YDBViewerClient(
            "http://localhost:8765", "/local", null, "root1", "");
        // Pre-seed a valid session so we don't hit the network
        seedSession(client, "cookie-value-xyz", System.currentTimeMillis() + 60_000);

        HttpURLConnection conn = openDummyConnection();
        invokeApplyAuth(client, conn);
        assertEquals("ydb_session_id=cookie-value-xyz", conn.getRequestProperty("Cookie"));
        assertNull(conn.getRequestProperty("Authorization"));
    }

    @Test
    public void applyAuth_noCredentialsSetsNoHeader() throws Exception {
        YDBViewerClient client = new YDBViewerClient(
            "http://localhost:8765", "/local", null, null, null);
        HttpURLConnection conn = openDummyConnection();
        invokeApplyAuth(client, conn);
        assertNull(conn.getRequestProperty("Authorization"));
        assertNull(conn.getRequestProperty("Cookie"));
    }

    // ── Session TTL ──────────────────────────────────────────────────────────

    @Test
    public void ensureSession_returnsCachedWhenFresh() throws Exception {
        YDBViewerClient client = new YDBViewerClient(
            "http://localhost:8765", "/local", null, "root1", "");
        seedSession(client, "fresh-cookie", System.currentTimeMillis() + 60_000);

        Method m = YDBViewerClient.class.getDeclaredMethod("ensureSession");
        m.setAccessible(true);
        assertEquals("fresh-cookie", m.invoke(client));
    }

    @Test
    public void ensureSession_expiredCookieIsNotReused() throws Exception {
        YDBViewerClient client = new YDBViewerClient(
            "http://localhost:8765", "/local", null, "root1", "");
        // Already-expired cookie. ensureSession would try to login() — assert it does NOT
        // return the stale value and instead attempts login (which fails — no server).
        seedSession(client, "stale-cookie", System.currentTimeMillis() - 1);

        Method m = YDBViewerClient.class.getDeclaredMethod("ensureSession");
        m.setAccessible(true);
        try {
            Object result = m.invoke(client);
            fail("Expected login attempt to fail (no server), got: " + result);
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Expected — login() throws because no server is reachable
            assertNotNull(e.getCause());
        }
    }

    @Test
    public void invalidateSession_clearsCachedCookie() throws Exception {
        YDBViewerClient client = new YDBViewerClient(
            "http://localhost:8765", "/local", null, "root1", "");
        seedSession(client, "to-be-cleared", System.currentTimeMillis() + 60_000);

        Method m = YDBViewerClient.class.getDeclaredMethod("invalidateSession");
        m.setAccessible(true);
        m.invoke(client);

        Field cookieField = YDBViewerClient.class.getDeclaredField("sessionCookie");
        cookieField.setAccessible(true);
        assertNull(cookieField.get(client));
    }

    @Test
    public void sessionTtlIsOneHour() throws Exception {
        Field ttlField = YDBViewerClient.class.getDeclaredField("SESSION_TTL_MS");
        ttlField.setAccessible(true);
        long ttl = ttlField.getLong(null);
        assertEquals(60L * 60L * 1000L, ttl);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static HttpURLConnection openDummyConnection() throws Exception {
        // Opening a connection does NOT perform any network I/O until connect()/getResponseCode().
        return (HttpURLConnection) URI.create("http://localhost:1/").toURL().openConnection();
    }

    private static void invokeApplyAuth(YDBViewerClient client, HttpURLConnection conn) throws Exception {
        Method m = YDBViewerClient.class.getDeclaredMethod("applyAuth", HttpURLConnection.class);
        m.setAccessible(true);
        m.invoke(client, conn);
    }

    private static void seedSession(YDBViewerClient client, String cookie, long expiresAt) throws Exception {
        Field cookieField = YDBViewerClient.class.getDeclaredField("sessionCookie");
        cookieField.setAccessible(true);
        cookieField.set(client, cookie);
        Field expField = YDBViewerClient.class.getDeclaredField("sessionExpiresAt");
        expField.setAccessible(true);
        expField.setLong(client, expiresAt);
    }
}
