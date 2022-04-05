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

package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SecurityWithoutEnterpriseTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void test() {
        SecurityConfig securityConfig = new SecurityConfig()
                .setEnabled(true);

        Config config = new Config()
                .setSecurityConfig(securityConfig);

        expected.expect(IllegalStateException.class);
        createHazelcastInstance(config);
    }

    @Test
    public void testSymmetricEncryption() {
        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true);
        Config config = new Config();
        config.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        expected.expect(IllegalStateException.class);
        createHazelcastInstance(config);
    }

    @Test
    public void testAuditlog() {
        Config config = new Config();
        config.getAuditlogConfig().setEnabled(true);
        expected.expect(IllegalStateException.class);
        createHazelcastInstance(config);
    }

    @Test
    public void testCredentialsSerialization() {
        HazelcastInstance hz = createHazelcastInstance(smallInstanceConfig());
        SerializationService serializationService = getSerializationService(hz);

        UsernamePasswordCredentials upc = new UsernamePasswordCredentials("admin", "secret");
        UsernamePasswordCredentials upc2 = serializationService.toObject(serializationService.toData(upc));
        assertEquals(upc.getName(), upc2.getName());
        assertEquals(upc.getPassword(), upc2.getPassword());

        SimpleTokenCredentials stc = new SimpleTokenCredentials(new byte[] { 1, 2, 3 });
        SimpleTokenCredentials stc2 = serializationService.toObject(serializationService.toData(stc));
        assertEquals(stc.getName(), stc2.getName());
        assertArrayEquals(stc.getToken(), stc2.getToken());
    }

}
