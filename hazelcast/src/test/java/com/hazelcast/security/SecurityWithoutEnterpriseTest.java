/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SecurityWithoutEnterpriseTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void test() {
        SecurityConfig securityConfig = new SecurityConfig()
                .setEnabled(true);

        Config config = new Config()
                .setSecurityConfig(securityConfig);

        createHazelcastInstance(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testSymmetricEncryption() {
        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true);
        Config config = new Config();
        config.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        createHazelcastInstance(config);
    }
}
