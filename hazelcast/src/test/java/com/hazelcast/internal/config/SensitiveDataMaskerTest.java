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
    public void shouldMaskCredentialsInUrlsAndKeepTheHost() {
        assertEquals("jdbc:postgresql://db.example:5432/app?user=app&password=****",
                mask("jdbcUrl", "jdbc:postgresql://db.example:5432/app?user=app&password=UrlSecret1"));
        assertEquals("jdbc:sqlserver://db.example:1433;databaseName=app;user=app;password=****",
                mask("jdbcUrl", "jdbc:sqlserver://db.example:1433;databaseName=app;user=app;password=UrlSecret1"));
        assertEquals("mongodb://****@mongo1:27017,mongo2:27017/app",
                mask("connectionString", "mongodb://app:UrlSecret1@mongo1:27017,mongo2:27017/app"));
        assertEquals("DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=****;EndpointSuffix=core.windows.net",
                mask("connectionString",
                        "DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=UrlSecret1;EndpointSuffix=core.windows.net"));
    }

    @Test
    public void shouldLeaveUrlsWithoutCredentialsUnchanged() {
        assertEquals("jdbc:postgresql://db.example:5432/app?ssl=true",
                mask("jdbcUrl", "jdbc:postgresql://db.example:5432/app?ssl=true"));
        assertEquals("kafka1:9092,kafka2:9092", mask("bootstrap.servers", "kafka1:9092,kafka2:9092"));
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
        // -Dkey="a b" as one element without quotes
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
    public void shouldMaskCredentialsInSystemPropertyArgUrl() {
        assertEquals("-Ddb.url=jdbc:postgresql://db.example:5432/app?user=app&password=****",
                maskSystemPropertyArg("-Ddb.url=jdbc:postgresql://db.example:5432/app?user=app&password=UrlSecret1"));
    }

    @Test
    public void shouldMaskCredentialsInOracleUrlsAndKeepTheHost() {
        // EZConnect
        assertEquals("jdbc:oracle:thin:****@//db.example:1521/svc",
                mask("jdbcUrl", "jdbc:oracle:thin:scott/tiger@//db.example:1521/svc"));
        // SID
        assertEquals("jdbc:oracle:thin:****@db.example:1521:orcl",
                mask("jdbcUrl", "jdbc:oracle:thin:scott/tiger@db.example:1521:orcl"));
        // TNS descriptor, OCI driver
        assertEquals("jdbc:oracle:oci:****@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example)(PORT=1521)))",
                mask("jdbcUrl", "jdbc:oracle:oci:scott/tiger@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example)(PORT=1521)))"));
        // LDAP naming
        assertEquals("jdbc:oracle:thin:****@ldap://ldap.example:389/svc,cn=OracleContext",
                mask("jdbcUrl", "jdbc:oracle:thin:scott/tiger@ldap://ldap.example:389/svc,cn=OracleContext"));
        // quoted password containing '@'
        assertEquals("jdbc:oracle:thin:****@db.example:1521:orcl",
                mask("jdbcUrl", "jdbc:oracle:thin:scott/\"p@ss\"@db.example:1521:orcl"));
        assertEquals("JDBC:ORACLE:THIN:****@//db.example:1521/svc",
                mask("jdbcUrl", "JDBC:ORACLE:THIN:scott/tiger@//db.example:1521/svc"));
    }

    @Test
    public void shouldLeaveOracleUrlsWithoutCredentialsUnchanged() {
        assertEquals("jdbc:oracle:thin:@//db.example:1521/svc",
                mask("jdbcUrl", "jdbc:oracle:thin:@//db.example:1521/svc"));
        assertEquals("jdbc:oracle:thin:@db.example:1521:orcl",
                mask("jdbcUrl", "jdbc:oracle:thin:@db.example:1521:orcl"));
    }

    @Test
    public void shouldMaskUrlParametersWhoseNameContainsASensitiveKeyword() {
        assertEquals("jdbc:postgresql://db.example:5432/app?user=app&sslpassword=****",
                mask("jdbcUrl", "jdbc:postgresql://db.example:5432/app?user=app&sslpassword=KeySecret1"));
        assertEquals("jdbc:sqlserver://db.example:1433;user=app;trustStorePassword=****",
                mask("jdbcUrl", "jdbc:sqlserver://db.example:1433;user=app;trustStorePassword=KeySecret1"));
        assertEquals("jdbc:mysql://db.example:3306/app?user=app&clientCertificateKeyStorePassword=****",
                mask("jdbcUrl", "jdbc:mysql://db.example:3306/app?user=app&clientCertificateKeyStorePassword=KeySecret1"));
        assertEquals("https://api.example/v1?api_key=****&page=2",
                mask("url", "https://api.example/v1?api_key=KeySecret1&page=2"));
    }
}
