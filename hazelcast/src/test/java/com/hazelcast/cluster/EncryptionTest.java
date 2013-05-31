/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)

public class EncryptionTest {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    /**
     * Simple symmetric encryption test.
     */
    @Test(timeout = 1000 * 30)
    public void testSymmetricEncryption() throws Exception {
        Config config = new Config();
        SymmetricEncryptionConfig encryptionConfig = new SymmetricEncryptionConfig();
        encryptionConfig.setEnabled(true);
        config.getNetworkConfig().setSymmetricEncryptionConfig(encryptionConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        Assert.assertEquals(2, h1.getCluster().getMembers().size());
        Assert.assertEquals(2, h2.getCluster().getMembers().size());
        Assert.assertEquals(h1.getCluster().getLocalMember(), h2.getCluster().getMembers().iterator().next());
    }
}
