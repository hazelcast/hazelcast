/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.spring.CustomSpringExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;

import static com.hazelcast.config.PersistenceClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"persistence-encryption-vault-applicationContext-hazelcast.xml"})
class TestPersistenceEncryptionVaultApplicationContext {

    @Autowired
    private Config config;

    @Autowired
    private SSLContextFactory sslContextFactory;

    @BeforeAll
    @AfterAll
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    void testPersistence() {
        File dir = new File("/mnt/persistence/");
        File hotBackupDir = new File("/mnt/persistence-backup/");
        PersistenceConfig persistenceConfig = config.getPersistenceConfig();

        assertFalse(persistenceConfig.isEnabled());
        assertEquals(dir.getAbsolutePath(), persistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(hotBackupDir.getAbsolutePath(), persistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(1111, persistenceConfig.getValidationTimeoutSeconds());
        assertEquals(2222, persistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(PARTIAL_RECOVERY_MOST_COMPLETE, persistenceConfig.getClusterDataRecoveryPolicy());
        assertFalse(persistenceConfig.isAutoRemoveStaleData());
        EncryptionAtRestConfig encryptionAtRestConfig = persistenceConfig.getEncryptionAtRestConfig();
        assertNotNull(encryptionAtRestConfig);
        assertTrue(encryptionAtRestConfig.isEnabled());
        assertEquals("AES/CBC/PKCS5Padding", encryptionAtRestConfig.getAlgorithm());
        assertEquals("sugar", encryptionAtRestConfig.getSalt());
        assertEquals(16, encryptionAtRestConfig.getKeySize());
        assertInstanceOf(VaultSecureStoreConfig.class, encryptionAtRestConfig.getSecureStoreConfig());
        VaultSecureStoreConfig vaultConfig = (VaultSecureStoreConfig) encryptionAtRestConfig.getSecureStoreConfig();
        assertEquals("http://localhost:1234", vaultConfig.getAddress());
        assertEquals("secret/path", vaultConfig.getSecretPath());
        assertEquals("token", vaultConfig.getToken());
        SSLConfig sslConfig = vaultConfig.getSSLConfig();
        assertNotNull(sslConfig);
        assertTrue(sslConfig.isEnabled());
        assertEquals(sslContextFactory, sslConfig.getFactoryImplementation());
        assertEquals(60, vaultConfig.getPollingInterval());
    }
}
