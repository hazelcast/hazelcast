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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestConfigUtils;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigSmokeTest extends HazelcastTestSupport {

    private static final int DEFAULT_INITIAL_CLUSTER_SIZE = 3;

    @Test
    public void multimap_initialSubmitTest() {
        String mapName = randomMapName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance i1 = instances[0];

        MultiMapConfig multiMapConfig = new MultiMapConfig(mapName);
        multiMapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMultiMapConfig(multiMapConfig);

        for (HazelcastInstance instance : instances) {
            multiMapConfig = instance.getConfig().findMultiMapConfig(mapName);
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, multiMapConfig.getBackupCount());
        }
    }

    @Test(expected = HazelcastException.class)
    public void map_testConflictingDynamicConfig() {
        String mapName = randomMapName();
        int initialClusterSize = 2;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(initialClusterSize);

        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MapConfig mapConfig1 = new MapConfig(mapName);
        mapConfig1.setBackupCount(0);

        MapConfig mapConfig2 = new MapConfig(mapName);
        mapConfig2.setBackupCount(1);

        i1.getConfig().addMapConfig(mapConfig1);
        i1.getConfig().addMapConfig(mapConfig2);
    }

    @Test
    public void map_testNonConflictingDynamicConfigWithTheSameName() {
        String mapName = randomMapName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MapConfig mapConfig1 = new MapConfig(mapName);
        mapConfig1.setBackupCount(0);

        MapConfig mapConfig2 = new MapConfig(mapName);
        mapConfig2.setBackupCount(0);

        i1.getConfig().addMapConfig(mapConfig1);
        i1.getConfig().addMapConfig(mapConfig2);
    }

    @Test
    public void multimap_withNewMemberJoiningLater() {
        String mapName = randomMapName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MultiMapConfig multiMapConfig = new MultiMapConfig(mapName);
        multiMapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMultiMapConfig(multiMapConfig);

        // start an instance AFTER the config was already submitted
        HazelcastInstance i3 = factory.newHazelcastInstance();

        multiMapConfig = i3.getConfig().getMultiMapConfig(mapName);
        assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, multiMapConfig.getBackupCount());
    }

    @Test
    public void map_initialSubmitTest() {
        String mapName = randomMapName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance i1 = instances[0];

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMapConfig(mapConfig);

        for (HazelcastInstance instance : instances) {
            mapConfig = instance.getConfig().findMapConfig(mapName);
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfig.getBackupCount());
        }
    }

    @Test
    public void map_initialSubmitTest_withWildcards() {
        String prefixWithWildcard = randomMapName() + "*";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance i1 = instances[0];

        MapConfig mapConfig = new MapConfig(prefixWithWildcard);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMapConfig(mapConfig);

        for (HazelcastInstance instance : instances) {
            String randomSuffix = randomMapName();
            mapConfig = instance.getConfig().findMapConfig(prefixWithWildcard + randomSuffix);
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfig.getBackupCount());
        }
    }

    @Test
    public void topic_initialSubmitTest() {
        String topicName = randomName();
        String listenerClassName = randomName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HazelcastInstance[] instances = factory.newInstances();
        HazelcastInstance i1 = instances[0];

        TopicConfig topicConfig = new TopicConfig(topicName);
        topicConfig.addMessageListenerConfig(new ListenerConfig(listenerClassName));
        i1.getConfig().addTopicConfig(topicConfig);

        for (HazelcastInstance instance : instances) {
            topicConfig = instance.getConfig().getTopicConfig(topicName);
            assertEquals(listenerClassName, topicConfig.getMessageListenerConfigs().get(0).getClassName());
        }
    }

    @Test
    public void mapConfig_withLiteMemberJoiningLater_isImmediatelyAvailable() {
        String mapName = randomMapName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMapConfig(mapConfig);

        // start a lite member after the dynamic config was submitted
        Config liteConfig = new Config();
        liteConfig.setLiteMember(true);
        HazelcastInstance i3 = factory.newHazelcastInstance(liteConfig);

        MapConfig mapConfigOnLiteMember = i3.getConfig().getMapConfig(mapName);
        assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfigOnLiteMember.getBackupCount());
    }

    @Test
    public void map_testNonConflictingStaticConfig() {
        String mapName = "test_map";
        Config config = new Config();
        config.addMapConfig(getMapConfigWithTTL(mapName, 20));

        HazelcastInstance hz = createHazelcastInstance(config);
        hz.getConfig().addMapConfig(getMapConfigWithTTL(mapName, 20));
    }

    @Test(expected = HazelcastException.class)
    public void map_testConflictingStaticConfig() {
        String mapName = "test_map";
        Config config = new Config();
        config.addMapConfig(getMapConfigWithTTL(mapName, 20));

        HazelcastInstance hz = createHazelcastInstance(config);
        hz.getConfig().addMapConfig(getMapConfigWithTTL(mapName, 50));
    }

    private MapConfig getMapConfigWithTTL(String mapName, int ttl) {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setTimeToLiveSeconds(ttl);
        return mapConfig;
    }
}
