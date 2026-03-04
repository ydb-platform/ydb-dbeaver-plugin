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

import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.Assert.*;

/**
 * Tests for {@link YDBDataSourceProvider#getConnectionURL}.
 * Verifies that the URL always includes grpcs:// or grpc:// scheme
 * and auth parameters from provider properties.
 */
public class YDBDataSourceProviderTest {

    private YDBDataSourceProvider provider;
    private DBPDriver driver;

    @Before
    public void setUp() {
        provider = new YDBDataSourceProvider();
        driver = createDriverProxy("jdbc:ydb:{host}[:{port}]{database}");
    }

    private static DBPDriver createDriverProxy(String sampleURL) {
        return (DBPDriver) Proxy.newProxyInstance(
                DBPDriver.class.getClassLoader(),
                new Class<?>[]{DBPDriver.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) {
                        if ("getSampleURL".equals(method.getName())) {
                            return sampleURL;
                        }
                        if ("getName".equals(method.getName())) {
                            return "YDB";
                        }
                        return null;
                    }
                });
    }

    // --- Scheme tests ---

    @Test
    public void testSecureConnectionIncludesGrpcs() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("lb.example.yandexcloud.net");
        config.setHostPort("2135");
        config.setDatabaseName("/ru-central1/b1g/etn");
        config.setProviderProperty("ydb.useSecure", "true");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should start with jdbc:ydb:grpcs://", result.startsWith("jdbc:ydb:grpcs://"));
    }

    @Test
    public void testSecureByDefault() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/local");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should use grpcs by default", result.startsWith("jdbc:ydb:grpcs://"));
    }

    @Test
    public void testInsecureConnectionIncludesGrpc() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("localhost");
        config.setHostPort("2136");
        config.setDatabaseName("/local");
        config.setProviderProperty("ydb.useSecure", "false");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should start with jdbc:ydb:grpc://", result.startsWith("jdbc:ydb:grpc://"));
        assertFalse("URL should not use grpcs", result.startsWith("jdbc:ydb:grpcs://"));
    }

    // --- Basic URL structure ---

    @Test
    public void testFullUrl() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("lb.example.yandexcloud.net");
        config.setHostPort("2135");
        config.setDatabaseName("/ru-central1/b1g/etn");
        config.setProviderProperty("ydb.useSecure", "true");

        String result = provider.getConnectionURL(driver, config);
        assertEquals(
                "jdbc:ydb:grpcs://lb.example.yandexcloud.net:2135/ru-central1/b1g/etn",
                result);
    }

    @Test
    public void testNoDoubleSlash() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/ru-central1-cs/b1g/etn");

        String result = provider.getConnectionURL(driver, config);
        assertFalse("URL should not contain double slash after port",
                result.matches(".*:\\d+//.*"));
    }

    @Test
    public void testDatabaseWithoutLeadingSlash() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("ru-central1/b1g/etn");

        String result = provider.getConnectionURL(driver, config);
        assertEquals(
                "jdbc:ydb:grpcs://host:2135/ru-central1/b1g/etn",
                result);
    }

    @Test
    public void testDatabaseWithMultipleLeadingSlashes() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("///ru-central1/b1g/etn");

        String result = provider.getConnectionURL(driver, config);
        assertEquals(
                "jdbc:ydb:grpcs://host:2135/ru-central1/b1g/etn",
                result);
    }

    @Test
    public void testWithoutPort() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setDatabaseName("/local");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host/local", result);
    }

    @Test
    public void testWithoutDatabase() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135", result);
    }

    // --- Auth parameter tests ---

    @Test
    public void testServiceAccountAuth() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "saFile");
        config.setProviderProperty("ydb.saFile", "/path/to/key.json");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should contain saKeyFile param",
                result.contains("?saKeyFile="));
    }

    @Test
    public void testTokenAuth() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "token");
        config.setProviderProperty("ydb.token", "mytoken");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135/db?token=mytoken", result);
    }

    @Test
    public void testStaticAuth() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "static");
        config.setUserName("admin");
        config.setUserPassword("secret");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135/db?user=admin&password=secret", result);
    }

    @Test
    public void testStaticAuthUserOnly() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "static");
        config.setUserName("admin");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135/db?user=admin", result);
    }

    @Test
    public void testMetadataAuth() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "metadata");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135/db?useMetadata=true", result);
    }

    @Test
    public void testAnonymousAuth() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "anonymous");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135/db", result);
    }

    @Test
    public void testNoAuthType() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");

        String result = provider.getConnectionURL(driver, config);
        assertEquals("jdbc:ydb:grpcs://host:2135/db", result);
    }

    @Test
    public void testSaFilePathEncoded() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "saFile");
        config.setProviderProperty("ydb.saFile", "/path with spaces/key.json");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("SA file path should be URL-encoded",
                result.contains("saKeyFile=%2Fpath+with+spaces%2Fkey.json"));
    }

    // --- Realistic YDB Managed Service scenario ---

    @Test
    public void testManagedYdbWithSaKey() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("lb.etn4c6lrpm12vsqp69vh.ydb.mdb.yandexcloud.net");
        config.setHostPort("2135");
        config.setDatabaseName("/ru-central1-cs/b1ggceeul2pkher8vhb6/etn4c6lrpm12vsqp69vh");
        config.setProviderProperty("ydb.useSecure", "true");
        config.setProviderProperty("ydb.authType", "saFile");
        config.setProviderProperty("ydb.saFile", "~/Downloads/ydb_qa_key.json");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("Should start with grpcs", result.startsWith("jdbc:ydb:grpcs://"));
        assertTrue("Should contain host", result.contains("lb.etn4c6lrpm12vsqp69vh.ydb.mdb.yandexcloud.net:2135"));
        assertTrue("Should contain database with single slash", result.contains(":2135/ru-central1-cs/"));
        assertTrue("Should contain saKeyFile", result.contains("saKeyFile="));
        assertFalse("Should not have double slash", result.contains("://lb") && result.matches(".*:\\d+//.*"));
    }

    // --- SSL certificate tests ---

    @Test
    public void testSslCertificateInUrl() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.sslCertificate", "/path/to/cert.pem");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should contain secureConnectionCertificate param",
                result.contains("secureConnectionCertificate="));
        assertTrue("Certificate path should be URL-encoded",
                result.contains("secureConnectionCertificate=%2Fpath%2Fto%2Fcert.pem"));
    }

    @Test
    public void testSslCertificateWithAuthParams() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "token");
        config.setProviderProperty("ydb.token", "mytoken");
        config.setProviderProperty("ydb.sslCertificate", "/certs/ca.pem");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should contain token param", result.contains("token=mytoken"));
        assertTrue("URL should contain certificate param",
                result.contains("&secureConnectionCertificate="));
    }

    @Test
    public void testNoSslCertificateWhenEmpty() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");

        String result = provider.getConnectionURL(driver, config);
        assertFalse("URL should not contain secureConnectionCertificate when not set",
                result.contains("secureConnectionCertificate"));
    }

    @Test
    public void testSaFileWithTildePath() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("host");
        config.setHostPort("2135");
        config.setDatabaseName("/db");
        config.setProviderProperty("ydb.authType", "saFile");
        config.setProviderProperty("ydb.saFile", "~/Downloads/ydb_qa_key.json");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("URL should contain saKeyFile param", result.contains("saKeyFile="));
        assertTrue("URL should contain encoded tilde path",
                result.contains("saKeyFile=%7E%2FDownloads%2Fydb_qa_key.json"));
    }

    @Test
    public void testManagedYdbFullScenario() {
        DBPConnectionConfiguration config = new DBPConnectionConfiguration();
        config.setHostName("lb.etn4c6lrpm12vsqp69vh.ydb.mdb.yandexcloud.net");
        config.setHostPort("2135");
        config.setDatabaseName("/ru-central1-cs/b1ggceeul2pkher8vhb6/etn4c6lrpm12vsqp69vh");
        config.setProviderProperty("ydb.useSecure", "true");
        config.setProviderProperty("ydb.authType", "saFile");
        config.setProviderProperty("ydb.saFile", "~/Downloads/ydb_qa_key.json");
        config.setProviderProperty("ydb.sslCertificate", "/certs/yandex-ca.pem");

        String result = provider.getConnectionURL(driver, config);
        assertTrue("Should start with grpcs", result.startsWith("jdbc:ydb:grpcs://"));
        assertTrue("Should contain saKeyFile", result.contains("saKeyFile="));
        assertTrue("Should contain secureConnectionCertificate", result.contains("secureConnectionCertificate="));
        assertFalse("Should not have double slash after port", result.matches(".*:\\d+//.*"));
    }
}
