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

package com.hazelcast.splitbrainprotection.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionEvent;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapSplitBrainProtectionListenerTest extends HazelcastTestSupport {

    @Test
    public void testSplitBrainProtectionFailureEventFiredWhenNodeCountBelowThreshold() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(mapName);
        try {
            map.put(generateKeyOwnedBy(instance), 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testSplitBrainProtectionFailureEventFiredWhenNodeCountDropsBelowThreshold() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance();
        HazelcastInstance hz = factory.newHazelcastInstance();
        hz.shutdown();
        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testSplitBrainProtectionEventsFiredWhenNodeCountBelowThenAboveThreshold() {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        final CountDownLatch aboveLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (splitBrainProtectionEvent.isPresent()) {
                    aboveLatch.countDown();
                } else {
                    belowLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        assertOpenEventually(belowLatch, 15);
        factory.newHazelcastInstance(config);
        assertOpenEventually(aboveLatch);
    }

    @Test
    public void testDifferentSplitBrainProtectionsGetCorrectEvents() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final CountDownLatch splitBrainProtectionFailureLatch = new CountDownLatch(2);
        String fourNodeSplitBrainProtectionName = "fourNode";
        SplitBrainProtectionConfig fourNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(fourNodeSplitBrainProtectionName, true, 4);
        fourNodeSplitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    splitBrainProtectionFailureLatch.countDown();
                }
            }
        }));
        String threeNodeSplitBrainProtectionName = "threeNode";
        SplitBrainProtectionConfig threeNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(threeNodeSplitBrainProtectionName, true, 3);
        threeNodeSplitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    splitBrainProtectionFailureLatch.countDown();
                }
            }
        }));
        MapConfig fourNodeMapConfig = new MapConfig("fourNode");
        fourNodeMapConfig.setSplitBrainProtectionName(fourNodeSplitBrainProtectionName);

        MapConfig threeNodeMapConfig = new MapConfig("threeNode");
        threeNodeMapConfig.setSplitBrainProtectionName(threeNodeSplitBrainProtectionName);

        Config config = new Config();
        config.addMapConfig(fourNodeMapConfig);
        config.addSplitBrainProtectionConfig(fourNodeSplitBrainProtectionConfig);
        config.addMapConfig(threeNodeMapConfig);
        config.addSplitBrainProtectionConfig(threeNodeSplitBrainProtectionConfig);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        assertOpenEventually(splitBrainProtectionFailureLatch);
    }

    @Test
    public void testCustomResolverFiresSplitBrainProtectionFailureEvent() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            @Override
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig();
        splitBrainProtectionConfig.setName(splitBrainProtectionName);
        splitBrainProtectionConfig.setEnabled(true);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        splitBrainProtectionConfig.setFunctionImplementation(new SplitBrainProtectionFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.getMapConfig(mapName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance();
        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testSplitBrainProtectionEventProvidesCorrectMemberListSize() {
        final CountDownLatch belowLatch = new CountDownLatch(2);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    Collection<Member> currentMembers = splitBrainProtectionEvent.getCurrentMembers();
                    assertEquals(3, splitBrainProtectionEvent.getThreshold());
                    assertTrue(currentMembers.size() < splitBrainProtectionEvent.getThreshold());
                    belowLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        assertOpenEventually(belowLatch);
    }
}
