/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.internal.config.SensitiveDataMasker.isSensitiveKey;
import static com.hazelcast.internal.config.SensitiveDataMasker.mask;
import static com.hazelcast.internal.config.SensitiveDataMasker.maskSystemPropertyArg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SensitiveDataMaskerTest {

    @Test
    public void shouldTreatSslAndCredentialKeysAsSensitive() {
        assertTrue(isSensitiveKey("hazelcast.network.ssl.properties.keystorepassword"));
        assertTrue(isSensitiveKey("hazelcast.network.ssl.properties.truststorepassword"));
        assertTrue(isSensitiveKey("hazelcast.security.realms.realm1.identity.credentials-factory.properties.secretkey"));
        assertTrue(isSensitiveKey("hazelcast.security.realms.realm1.identity.credentials-factory.properties.accesskey"));
        assertTrue(isSensitiveKey("hazelcast.security.member-authentication.token"));
        assertTrue(isSensitiveKey("hazelcast.security.realms.realm1.authentication.simple.salt"));
        assertTrue(isSensitiveKey("hazelcast.rest.ssl.certificateprivatekey"));
        assertTrue(isSensitiveKey("hazelcast-client.network.azure.client-id"));
        assertTrue(isSensitiveKey("hazelcast-client.network.azure.client-secret"));
        assertTrue(isSensitiveKey("hazelcast-client.network.azure.tenant-id"));
        assertTrue(isSensitiveKey("hazelcast.network.join.azure.subscription-id"));
        assertTrue(isSensitiveKey("javax.net.ssl.keyStorePassword"));
        assertTrue(isSensitiveKey("sasl.jaas.config"));
        assertTrue(isSensitiveKey("basic.auth.user.info"));
    }

    @Test
    public void shouldTreatAmbiguousKeywordsAsSensitiveOnlyAsLastSegment() {
        assertTrue(isSensitiveKey("credential"));
        assertTrue(isSensitiveKey("hazelcast.network.join.discoverystrategies.properties.credential"));
        assertTrue(isSensitiveKey("identity"));
        assertFalse(isSensitiveKey("credentials-factory.class-name"));
        assertFalse(isSensitiveKey("hazelcast.security.realms.realm1.identity.username"));
    }

    @Test
    public void shouldTreatNonSensitiveKeysAsNotSensitive() {
        assertFalse(isSensitiveKey("hazelcast.cluster-name"));
        assertFalse(isSensitiveKey("hazelcast.network.ssl.properties.keystore"));
        assertFalse(isSensitiveKey("hazelcast.network.ssl.enabled"));
        assertFalse(isSensitiveKey("hazelcast.properties.foo"));
        assertFalse(isSensitiveKey("hazelcast-client.network.azure.resource-group"));
        assertFalse(isSensitiveKey(null));
    }

    @Test
    public void shouldMaskSensitiveValue() {
        assertEquals(MASK_FOR_SENSITIVE_DATA, mask("hazelcast.network.ssl.properties.keystorepassword", "s3cr3t"));
        assertEquals(MASK_FOR_SENSITIVE_DATA, mask("hazelcast.network.ssl.properties.truststorepassword", "s3cr3t"));
        assertEquals(MASK_FOR_SENSITIVE_DATA, mask("access-key", "s3cr3t"));
    }

    @Test
    public void shouldLeaveNonSensitiveValueUnmasked() {
        assertEquals("test", mask("hazelcast.cluster-name", "test"));
        assertNull(mask("password", null));
    }

    @Test
    public void shouldPartiallyMaskLicenseKeyInEverySpelling() {
        String expected = "ABCDE*********VWXYZ";
        assertEquals(expected, mask("hazelcast.licensekey", "ABCDE12345FGHIJ67890VWXYZ"));
        assertEquals(expected, mask("hazelcast.license-key", "ABCDE12345FGHIJ67890VWXYZ"));
        assertEquals(expected, mask("hazelcast.enterprise.license.key", "ABCDE12345FGHIJ67890VWXYZ"));
    }

    @Test
    public void shouldMaskUrlsAfterTheirScheme() {
        assertEquals("jdbc:postgresql://****",
                mask("jdbcUrl", "jdbc:postgresql://db.example:5432/app?user=app&password=UrlSecret1"));
        assertEquals("jdbc:sqlserver://****",
                mask("jdbcUrl", "jdbc:sqlserver://db.example:1433;databaseName=app;user=app;password=UrlSecret1"));
        assertEquals("mongodb://****", mask("connectionString", "mongodb://app:UrlSecret1@mongo1:27017,mongo2:27017/app"));
        assertEquals("mongodb+srv://****", mask("connectionString", "mongodb+srv://app:UrlSecret1@cluster.example/app"));
        assertEquals("ldap://****", mask("url", "ldap://cn=admin:UrlSecret1@ldap.example:389"));
        assertEquals("vault+https://****", mask("url", "vault+https://u:UrlSecret1@vault.example"));
        // URLs without credentials too: whether a URL holds one depends on the driver
        assertEquals("jdbc:postgresql://****", mask("jdbcUrl", "jdbc:postgresql://db.example:5432/app?ssl=true"));
    }

    @Test
    public void shouldMaskJdbcUrlsWithoutSlashesAfterTheirSubprotocol() {
        // Oracle EZConnect, SID, TNS descriptor, LDAP naming and quoted password containing '@'
        assertEquals("jdbc:oracle:****", mask("jdbcUrl", "jdbc:oracle:thin:scott/tiger@//db.example:1521/svc"));
        assertEquals("jdbc:oracle:****", mask("jdbcUrl", "jdbc:oracle:thin:scott/tiger@db.example:1521:orcl"));
        assertEquals("jdbc:oracle:****",
                mask("jdbcUrl", "jdbc:oracle:oci:scott/tiger@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example)(PORT=1521)))"));
        assertEquals("jdbc:oracle:****",
                mask("jdbcUrl", "jdbc:oracle:thin:scott/tiger@ldap://ldap.example:389/svc,cn=OracleContext"));
        assertEquals("jdbc:oracle:****", mask("jdbcUrl", "jdbc:oracle:thin:scott/\"p@ss\"@db.example:1521:orcl"));
        assertEquals("JDBC:ORACLE:****", mask("jdbcUrl", "JDBC:ORACLE:THIN:scott/tiger@//db.example:1521/svc"));
        assertEquals("jdbc:h2:****", mask("jdbcUrl", "jdbc:h2:mem:test;USER=sa;PASSWORD=UrlSecret1"));
        assertEquals("jdbc:derby:****", mask("jdbcUrl", "jdbc:derby:app;user=app;password=UrlSecret1"));
    }

    @Test
    public void shouldMaskOnlyCredentialsInHttpUrls() {
        assertEquals("https://vault.example:8200", mask("address", "https://vault.example:8200"));
        assertEquals("http://k8s.example/api/v1", mask("kubernetes-master", "http://k8s.example/api/v1"));
        assertEquals("https://****@api.example/v1", mask("url", "https://app:UrlSecret1@api.example/v1"));
        assertEquals("HTTPS://****@api.example", mask("url", "HTTPS://app:UrlSecret1@api.example"));
        assertEquals("https://api.example/v1?api_key=****&page=2",
                mask("url", "https://api.example/v1?api_key=UrlSecret1&page=2"));
    }

    @Test
    public void shouldMaskUrlsInsideLongerValues() {
        assertEquals("Endpoint=sb://****",
                mask("connectionString", "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=Secret1"));
        assertEquals("primary https://****@a.example backup https://****@b.example",
                mask("endpoints", "primary https://u:Secret1@a.example backup https://u:Secret1@b.example"));
    }

    @Test
    public void shouldMaskSensitiveParametersInConnectionStringsWithoutUrl() {
        assertEquals("DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=****;EndpointSuffix=core.windows.net",
                mask("connectionString",
                        "DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=UrlSecret1;EndpointSuffix=core.windows.net"));
        assertEquals("Server=db.example;User Id=app;Password=****",
                mask("connectionString", "Server=db.example;User Id=app;Password=UrlSecret1"));
        assertEquals("user=app;trustStorePassword=****", mask("options", "user=app;trustStorePassword=KeySecret1"));
        assertEquals("api_key=****&page=2", mask("query", "api_key=KeySecret1&page=2"));
    }

    @Test
    public void shouldLeaveValuesWithoutUrlsUnchanged() {
        assertEquals("kafka1:9092,kafka2:9092", mask("bootstrap.servers", "kafka1:9092,kafka2:9092"));
        assertEquals("127.0.0.1:5701", mask("member", "127.0.0.1:5701"));
        assertEquals("C:\\data\\hz", mask("base-dir", "C:\\data\\hz"));
        assertEquals("cluster.name=dev", mask("options", "cluster.name=dev"));
    }

    @Test
    public void shouldIgnoreSurroundingWhitespaceWhenMaskingLicenseKey() {
        assertEquals("ABCDE*********VWXYZ", mask("hazelcast.licensekey", "ABCDE12345FGHIJ67890VWXYZ\n"));
        assertEquals("ABCDE*********VWXYZ", mask("license-key", "\n    ABCDE12345FGHIJ67890VWXYZ\n    "));
    }

    @Test
    public void shouldMaskSensitiveSystemPropertyArg() {
        assertEquals("-Djavax.net.ssl.keyStorePassword=****",
                maskSystemPropertyArg("-Djavax.net.ssl.keyStorePassword=s3cr3t"));
        assertEquals("-Dhz.license-key=ABCDE*********VWXYZ",
                maskSystemPropertyArg("-Dhz.license-key=ABCDE12345FGHIJ67890VWXYZ"));
        assertEquals("-Dhz.cluster-name=dev", maskSystemPropertyArg("-Dhz.cluster-name=dev"));
        assertNull(maskSystemPropertyArg(null));
    }

    @Test
    public void shouldMaskWholeSystemPropertyArgValueContainingSpaces() {
        // getInputArguments() returns -Dkey="a b" as one element without quotes
        assertEquals("-Djavax.net.ssl.keyStorePassword=****",
                maskSystemPropertyArg("-Djavax.net.ssl.keyStorePassword=my secret pass"));
    }

    @Test
    public void shouldLeaveNonSystemPropertyArgsUnchanged() {
        assertEquals("-Xmx1g", maskSystemPropertyArg("-Xmx1g"));
        assertEquals("-XX:+UseG1GC", maskSystemPropertyArg("-XX:+UseG1GC"));
        assertEquals("-Dflag", maskSystemPropertyArg("-Dflag"));
    }

    @Test
    public void shouldMaskUrlInSystemPropertyArg() {
        assertEquals("-Ddb.url=jdbc:postgresql://****",
                maskSystemPropertyArg("-Ddb.url=jdbc:postgresql://db.example:5432/app?user=app&password=UrlSecret1"));
    }
}
