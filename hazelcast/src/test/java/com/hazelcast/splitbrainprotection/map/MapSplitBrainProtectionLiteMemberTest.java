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

package com.hazelcast.splitbrainprotection.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapSplitBrainProtectionLiteMemberTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(3);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void test_readSplitBrainProtectionNotSatisfied_withLiteMembers() {
        Config config = createConfig("r", SplitBrainProtectionOn.READ, 3, false);
        Config liteConfig = createConfig("r", SplitBrainProtectionOn.READ, 3, true);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("r").keySet();
    }

    @Test
    public void test_readSplitBrainProtectionSatisfied_withLiteMembers() {
        Config config = createConfig("r", SplitBrainProtectionOn.READ, 2, false);
        Config liteConfig = createConfig("r", SplitBrainProtectionOn.READ, 2, true);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("r").keySet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void test_readReadWriteSplitBrainProtectionNotSatisfied_withLiteMembers() {
        Config config = createConfig("rw", SplitBrainProtectionOn.READ_WRITE, 3, false);
        Config liteConfig = createConfig("rw", SplitBrainProtectionOn.READ_WRITE, 3, true);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("rw").keySet();
    }

    @Test
    public void test_readReadWriteSplitBrainProtectionSatisfied_withLiteMembers() {
        Config config = createConfig("rw", SplitBrainProtectionOn.READ_WRITE, 2, false);
        Config liteConfig = createConfig("rw", SplitBrainProtectionOn.READ_WRITE, 2, true);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("rw").keySet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void test_readWriteSplitBrainProtectionNotSatisfied_withLiteMembers() {
        Config config = createConfig("w", SplitBrainProtectionOn.WRITE, 3, false);
        Config liteConfig = createConfig("w", SplitBrainProtectionOn.WRITE, 3, true);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("w").put(0, 0);
    }

    @Test
    public void test_readWriteSplitBrainProtectionSatisfied_withLiteMembers() {
        Config config = createConfig("w", SplitBrainProtectionOn.WRITE, 2, false);
        Config liteConfig = createConfig("w", SplitBrainProtectionOn.WRITE, 2, true);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("w").put(0, 0);
    }

    private Config createConfig(String name, SplitBrainProtectionOn type, int size, boolean liteMember) {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig().setName(name).setProtectOn(type).setEnabled(true).setMinimumClusterSize(size);
        MapConfig mapConfig = new MapConfig(name);
        mapConfig.setSplitBrainProtectionName(name);
        Config config = new Config();
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        config.addMapConfig(mapConfig);
        config.setLiteMember(liteMember);
        return config;
    }
}
