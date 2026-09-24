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

package com.hazelcast.internal.config.override;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.internal.config.override.ExternalConfigurationOverride.formatValueForLog;
import static com.hazelcast.internal.config.override.ExternalConfigurationOverride.isSensitiveKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalConfigurationOverrideSensitiveDataTest {

    @Test
    public void shouldTreatSslAndCredentialKeysAsSensitive() {
        assertTrue(isSensitiveKey("hazelcast.network.ssl.properties.keystorepassword"));
        assertTrue(isSensitiveKey("hazelcast.network.ssl.properties.truststorepassword"));
        assertTrue(isSensitiveKey("hazelcast.security.realms.realm1.identity.credentials-factory.properties.secretkey"));
        assertTrue(isSensitiveKey("hazelcast.security.realms.realm1.identity.credentials-factory.properties.accesskey"));
        assertTrue(isSensitiveKey("hazelcast.security.member-authentication.token"));
        assertTrue(isSensitiveKey("hazelcast.security.realms.realm1.authentication.simple.salt"));
        assertTrue(isSensitiveKey("hazelcast.rest.ssl.certificateprivatekey"));
        assertTrue(isSensitiveKey("hazelcast.rest.ssl.certificateprivatekey"));
        assertTrue(isSensitiveKey("hazelcast-client.network.azure.client-id"));
        assertTrue(isSensitiveKey("hazelcast-client.network.azure.client-secret"));
        assertTrue(isSensitiveKey("hazelcast-client.network.azure.tenant-id"));
    }

    @Test
    public void shouldTreatNonSensitiveKeysAsNotSensitive() {
        assertFalse(isSensitiveKey("hazelcast.cluster-name"));
        assertFalse(isSensitiveKey("hazelcast.network.ssl.properties.keystore"));
        assertFalse(isSensitiveKey("hazelcast.network.ssl.enabled"));
        assertFalse(isSensitiveKey("hazelcast.properties.foo"));
        assertFalse(isSensitiveKey("hazelcast-client.network.azure.resource-group"));
    }

    @Test
    public void shouldMaskSensitiveValueInLogOutput() {
        assertEquals(MASK_FOR_SENSITIVE_DATA,
                formatValueForLog("hazelcast.network.ssl.properties.keystorepassword", "s3cr3t"));
        assertEquals(MASK_FOR_SENSITIVE_DATA,
                formatValueForLog("hazelcast.network.ssl.properties.truststorepassword", "s3cr3t"));
    }

    @Test
    public void shouldLeaveNonSensitiveValueUnmaskedInLogOutput() {
        assertEquals("test", formatValueForLog("hazelcast.cluster-name", "test"));
    }

    @Test
    public void shouldPartiallyMaskLicenseKeyInLogOutput() {
        String masked = formatValueForLog("hazelcast.licensekey", "ABCDE12345FGHIJ67890VWXYZ");
        assertEquals(masked, "ABCDE*********VWXYZ");
    }
}
