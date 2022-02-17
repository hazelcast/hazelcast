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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.internal.cluster.impl.ConfigCheck;
import com.hazelcast.internal.cluster.impl.ConfigMismatchException;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigCheckTest {

    @Test
    public void whenClusterNameDifferent_thenFalse() {
        Config config1 = new Config();
        config1.setClusterName("foo");

        Config config2 = new Config();
        config2.setClusterName("bar");

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleFalse(configCheck1, configCheck2);
    }

    @Test
    public void whenGroupPasswordDifferent_thenJoin() {
        Config config1 = new Config();
        config1.setClusterName("c1");
        config1.getSecurityConfig().setMemberRealmConfig("m1",
                new RealmConfig().setUsernamePasswordIdentityConfig("foo", "Here"));

        Config config2 = new Config();
        config2.setClusterName("c1");
        config2.getSecurityConfig().setMemberRealmConfig("m2",
                new RealmConfig().setUsernamePasswordIdentityConfig("foo", "There"));

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleTrue(configCheck1, configCheck2);
    }

    @Test
    public void whenJoinerTypeDifferent_thenConfigMismatchException() {
        Config config1 = new Config();
        Config config2 = new Config();

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner1");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner2");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenDifferentPartitionCount_thenConfigurationMismatchException() {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "100");

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "200");

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenGroupPartitionEnabledMismatch_thenConfigurationMismatchException() {
        Config config1 = new Config();
        config1.getPartitionGroupConfig().setEnabled(false);

        Config config2 = new Config();
        config2.getPartitionGroupConfig().setEnabled(true);

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenPartitionGroupGroupTypeMismatch_thenConfigurationMismatchException() {
        Config config1 = new Config();
        config1.getPartitionGroupConfig().setEnabled(true);
        config1.getPartitionGroupConfig().setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);

        Config config2 = new Config();
        config2.getPartitionGroupConfig().setEnabled(true);
        config2.getPartitionGroupConfig().setGroupType(PartitionGroupConfig.MemberGroupType.PER_MEMBER);

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }


    public void assertIsCompatibleFalse(ConfigCheck c1, ConfigCheck c2) {
        Assert.assertFalse(c1.isCompatible(c2));
        Assert.assertFalse(c2.isCompatible(c1));
        assertIsCompatibleSelf(c1);
        assertIsCompatibleSelf(c2);
    }

    public void assertIsCompatibleTrue(ConfigCheck c1, ConfigCheck c2) {
        Assert.assertTrue(c1.isCompatible(c2));
        Assert.assertTrue(c2.isCompatible(c1));
        assertIsCompatibleSelf(c1);
        assertIsCompatibleSelf(c2);
    }

    public void assertIsCompatibleThrowsConfigMismatchException(ConfigCheck c1, ConfigCheck c2) {
        try {
            c1.isCompatible(c2);
            fail();
        } catch (ConfigMismatchException e) {

        }

        try {
            c2.isCompatible(c1);
            fail();
        } catch (ConfigMismatchException e) {

        }

        assertIsCompatibleSelf(c1);
        assertIsCompatibleSelf(c2);
    }

    private void assertIsCompatibleSelf(ConfigCheck c1) {
        Assert.assertTrue(c1.isCompatible(c1));
    }
}
