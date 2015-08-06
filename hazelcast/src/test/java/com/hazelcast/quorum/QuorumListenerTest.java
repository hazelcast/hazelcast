/*
* Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.QuorumListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QuorumListenerTest extends HazelcastTestSupport {

    @Test
    public void testQuorumFailureEventFiredWhenNodeCountBelowThreshold() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig(quorumName, true, 3);
        quorumConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);
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
    public void testQuorumEventsFiredWhenNodeCountBelowThenAboveThreshold() throws Exception {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        final CountDownLatch aboveLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (quorumEvent.isPresent()) {
                    aboveLatch.countDown();
                } else {
                    belowLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig(quorumName, true, 3);
        quorumConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(mapName);
        try {
            map.put(generateKeyOwnedBy(instance1), 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertOpenEventually(belowLatch, 15);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        map.put(generateKeyOwnedBy(instance1), 1);
        assertOpenEventually(aboveLatch);
    }

    @Test
    public void testDifferentQuorumsGetCorrectEvents() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final CountDownLatch quorumFailureLatch = new CountDownLatch(2);
        String fourNodeQuorumName = "fourNode";
        QuorumConfig fourNodeQuorumConfig = new QuorumConfig(fourNodeQuorumName, true, 4);
        fourNodeQuorumConfig.addListenerConfig(new QuorumListenerConfig(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    quorumFailureLatch.countDown();
                }
            }
        }));
        String threeNodeQuorumName = "threeNode";
        QuorumConfig threeNodeQuorumConfig = new QuorumConfig(threeNodeQuorumName, true, 3);
        threeNodeQuorumConfig.addListenerConfig(new QuorumListenerConfig(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    quorumFailureLatch.countDown();
                }
            }
        }));
        MapConfig fourNodeMapConfig = new MapConfig("fourNode");
        fourNodeMapConfig.setQuorumName(fourNodeQuorumName);

        MapConfig threeNodeMapConfig = new MapConfig("threeNode");
        threeNodeMapConfig.setQuorumName(threeNodeQuorumName);

        Config config = new Config();
        config.addMapConfig(fourNodeMapConfig);
        config.addQuorumConfig(fourNodeQuorumConfig);
        config.addMapConfig(threeNodeMapConfig);
        config.addQuorumConfig(threeNodeQuorumConfig);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> fourNode = h1.getMap("fourNode");
        IMap<Object, Object> threeNode = h1.getMap("threeNode");
        try {
            threeNode.put(generateKeyOwnedBy(h1), "bar");
            fail();
        } catch (Exception e) {
        }
        try {
            fourNode.put(generateKeyOwnedBy(h1), "bar");
            fail();
        } catch (Exception e) {
        }
        assertOpenEventually(quorumFailureLatch);

    }

    @Test
    public void testCustomResolverFiresQuorumFailureEvent() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            @Override
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(quorumName);
        quorumConfig.setEnabled(true);
        quorumConfig.addListenerConfig(listenerConfig);
        quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.getMapConfig(mapName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        QuorumService quorumService = instance.getQuorumService();
        Quorum quorum = quorumService.getQuorum(quorumName);
        IMap<Object, Object> map = instance.getMap(mapName);
        try {
            map.put(generateKeyOwnedBy(instance), 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertOpenEventually(countDownLatch, 15);

    }

    @Test
    public void testQuorumEventProvidesCorrectMemberListSize() throws Exception {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    Collection<Member> currentMembers = quorumEvent.getCurrentMembers();
                    assertEquals(2, currentMembers.size());
                    assertEquals(3, quorumEvent.getThreshold());
                    belowLatch.countDown();
                }
            }
        });
        String mapName = randomMapName();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig(quorumName, true, 3);
        quorumConfig.addListenerConfig(listenerConfig);
        config.getMapConfig(mapName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(mapName);
        try {
            map.put(generateKeyOwnedBy(instance1), 1);
        } catch (Exception e) {

        }
        assertOpenEventually(belowLatch);
    }
}
