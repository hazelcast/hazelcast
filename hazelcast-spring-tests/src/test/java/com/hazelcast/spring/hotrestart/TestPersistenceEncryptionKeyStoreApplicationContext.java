/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.io.File;

import static com.hazelcast.config.PersistenceClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"persistence-encryption-keystore-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestPersistenceEncryptionKeyStoreApplicationContext {

    @Resource(name = "theConfig")
    private Config config;

    @Test
    public void testPersistence() {
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
        assertTrue(encryptionAtRestConfig.getSecureStoreConfig() instanceof JavaKeyStoreSecureStoreConfig);
        JavaKeyStoreSecureStoreConfig keyStoreConfig = (JavaKeyStoreSecureStoreConfig) encryptionAtRestConfig
                .getSecureStoreConfig();
        assertEquals(new File("/mnt/hot-restart/keystore.p12").getAbsolutePath(), keyStoreConfig.getPath().getAbsolutePath());
        assertEquals("PKCS12", keyStoreConfig.getType());
        assertEquals("password", keyStoreConfig.getPassword());
        assertEquals(60, keyStoreConfig.getPollingInterval());
    }
}
