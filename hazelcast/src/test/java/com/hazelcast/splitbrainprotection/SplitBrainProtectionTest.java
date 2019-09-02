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

package com.hazelcast.splitbrainprotection;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.impl.SplitBrainProtectionServiceImpl;
import com.hazelcast.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;
import com.hazelcast.internal.services.MemberAttributeServiceEvent;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests split brain protection related configurations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SplitBrainProtectionTest extends HazelcastTestSupport {

    @Test
    public void testSplitBrainProtectionIsSetCorrectlyOnNodeInitialization() {
        String splitBrainProtectionName1 = randomString();
        String splitBrainProtectionName2 = randomString();

        SplitBrainProtectionConfig splitBrainProtectionConfig1 = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName1)
                .setEnabled(true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return true;
                    }
                });

        SplitBrainProtectionConfig splitBrainProtectionConfig2 = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName2)
                .setEnabled(true)
                .setMinimumClusterSize(2);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig1)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig2);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        final SplitBrainProtection splitBrainProtection1 = hazelcastInstance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName1);
        final SplitBrainProtection splitBrainProtection2 = hazelcastInstance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(splitBrainProtection1.isMinimumClusterSizeSatisfied());
                assertFalse(splitBrainProtection2.isMinimumClusterSizeSatisfied());
            }
        });
    }

    @Test
    public void testProbabilisticSplitBrainProtectionConsidersLocalMember() {
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionFunction splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(1, 100, 1250, 20, 100, 20);
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(splitBrainProtectionFunction);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance instance = createHazelcastInstance(config);

        final SplitBrainProtection splitBrainProtection = instance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(splitBrainProtection.isMinimumClusterSizeSatisfied());
            }
        });
    }

    @Test
    public void testRecentlyActiveSplitBrainProtectionConsidersLocalMember() {
        final String splitBrainProtectionName = randomString();
        SplitBrainProtectionFunction splitBrainProtectionFunction = new RecentlyActiveSplitBrainProtectionFunction(1, 10000);
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(splitBrainProtectionFunction);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance instance = createHazelcastInstance(config);

        final SplitBrainProtection splitBrainProtection = instance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(splitBrainProtection.isMinimumClusterSizeSatisfied());
            }
        });
    }

    @Test
    public void testSplitBrainProtectionIgnoresMemberAttributeEvents() {
        final RecordingSplitBrainProtectionFunction function = new RecordingSplitBrainProtectionFunction();

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(randomString())
                .setEnabled(true)
                .setFunctionImplementation(function);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hazelcastInstance);
        MembershipAwareService service = nodeEngine.getService(SplitBrainProtectionServiceImpl.SERVICE_NAME);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(function.wasCalled);
            }
        });
        function.wasCalled = false;

        MemberAttributeServiceEvent event = mock(MemberAttributeServiceEvent.class);
        service.memberAttributeChanged(event);

        assertFalse(function.wasCalled);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void testCustomSplitBrainProtectionFunctionFails() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return false;
                    }
                });

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .addMapConfig(mapConfig);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        map.put("1", "1");
    }

    @Test
    public void testCustomSplitBrainProtectionFunctionIsPresent() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return false;
                    }
                });

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .addMapConfig(mapConfig);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        try {
            map.put("1", "1");
            fail();
        } catch (Exception ignored) {
        }
        SplitBrainProtection splitBrainProtection = hazelcastInstance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName);
        assertFalse(splitBrainProtection.isMinimumClusterSizeSatisfied());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void testCustomSplitBrainProtectionFunctionFailsForAllNodes() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return false;
                    }
                });

        Config config = new Config()
                .addMapConfig(mapConfig)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        factory.newHazelcastInstance(config);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        IMap<Object, Object> map2 = hz.getMap(mapName);
        map2.put("1", "1");
    }

    @Test
    public void testCustomSplitBrainProtectionFunctionFailsThenSuccess() {
        final AtomicInteger count = new AtomicInteger(1);
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        if (count.get() == 1) {
                            count.incrementAndGet();
                            return false;
                        } else {
                            return true;
                        }
                    }
                });

        Config config = new Config()
                .addMapConfig(mapConfig)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        try {
            map.put("1", "1");
            fail();
        } catch (Exception e) {
            e.printStackTrace();
        }
        factory.newHazelcastInstance(config);
        map.put("1", "1");
        factory.shutdownAll();
    }

    @Test
    public void testOneSplitBrainProtectionsFailsOneSplitBrainProtectionSuccessForDifferentMaps() {
        String fourNodeSplitBrainProtection = randomString();
        String threeNodeSplitBrainProtection = randomString();

        MapConfig fourNodeMapConfig = new MapConfig("fourNode")
                .setSplitBrainProtectionName(fourNodeSplitBrainProtection);
        MapConfig threeNodeMapConfig = new MapConfig("threeNode")
                .setSplitBrainProtectionName(threeNodeSplitBrainProtection);

        SplitBrainProtectionConfig fourNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(fourNodeSplitBrainProtection, true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return members.size() == 4;
                    }
                });
        SplitBrainProtectionConfig threeNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(threeNodeSplitBrainProtection, true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return members.size() == 3;
                    }
                });

        Config config = new Config()
                .addMapConfig(fourNodeMapConfig)
                .addMapConfig(threeNodeMapConfig)
                .addSplitBrainProtectionConfig(threeNodeSplitBrainProtectionConfig)
                .addSplitBrainProtectionConfig(fourNodeSplitBrainProtectionConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Object, Object> fourNode = hz.getMap("fourNode");
        IMap<Object, Object> threeNode = hz.getMap("threeNode");
        threeNode.put(generateKeyOwnedBy(hz), "bar");
        try {
            fourNode.put(generateKeyOwnedBy(hz), "bar");
            fail();
        } catch (Exception ignored) {
        }
    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/9792
     */
    @Test
    public void oneSplitBrainProtectionShouldNotAffectSplitBrainProtectionAwareOperationsOnDataStructuresWithoutSplitBrainProtectionConfiguration() {
        String splitBrainProtectionName = randomString();

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return members.size() == 3;
                    }
                });

        MapConfig mapConfig = new MapConfig("splitBrainProtectionMap")
                .setSplitBrainProtectionName(splitBrainProtectionName);

        Config config = new Config()
                .addMapConfig(mapConfig)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Object, Object> splitBrainProtectionMap = hz.getMap("splitBrainProtectionMap");
        splitBrainProtectionMap.put(generateKeyOwnedBy(hz), "bar");

        ILock lock = hz.getLock("noSplitBrainProtectionLock");
        try {
            lock.lock();
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void givenSplitBrainProtectionFunctionConfigured_whenImplementsHazelcastInstanceAware_thenHazelcastInjectsItsInstance() {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(randomString(), true)
                .setFunctionClassName(HazelcastInstanceAwareSplitBrainProtectionFunction.class.getName());

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        assertEquals(instance, HazelcastInstanceAwareSplitBrainProtectionFunction.instance);
    }

    @Test
    public void givenSplitBrainProtectionFunctionInstanceConfigured_whenImplementsHazelcastInstanceAware_thenHazelcastInjectsItsInstance() {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(randomString(), true)
                .setFunctionImplementation(new HazelcastInstanceAwareSplitBrainProtectionFunction());

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        assertEquals(instance, HazelcastInstanceAwareSplitBrainProtectionFunction.instance);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenProbabilisticSplitBrainProtection_whenAcceptableHeartbeatPause_greaterThanMaxNoHeartbeat_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        SplitBrainProtectionConfig probabilisticSplitBrainProtectionConfig = SplitBrainProtectionConfig.newProbabilisticSplitBrainProtectionConfigBuilder("prob-split-brain-protection", 3)
                .withAcceptableHeartbeatPauseMillis(13000)
                .build();

        config.addSplitBrainProtectionConfig(probabilisticSplitBrainProtectionConfig);

        createHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenProbabilisticSplitBrainProtection_whenAcceptableHeartbeatPause_lessThanHeartbeatInterval_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "5");
        SplitBrainProtectionConfig probabilisticSplitBrainProtectionConfig = SplitBrainProtectionConfig.newProbabilisticSplitBrainProtectionConfigBuilder("prob-split-brain-protection", 3)
                .withAcceptableHeartbeatPauseMillis(3000)
                .build();

        config.addSplitBrainProtectionConfig(probabilisticSplitBrainProtectionConfig);

        createHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenRecentlyActiveSplitBrainProtection_whenHeartbeatTolerance_greaterThanMaxNoHeartbeat_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        SplitBrainProtectionConfig recentlyActiveSplitBrainProtectionConfig = SplitBrainProtectionConfig
                .newRecentlyActiveSplitBrainProtectionConfigBuilder("test-splitBrainProtection", 3, 13000)
                .build();

        config.addSplitBrainProtectionConfig(recentlyActiveSplitBrainProtectionConfig);

        createHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenRecentlyActiveSplitBrainProtection_whenHeartbeatTolerance_lessThanHeartbeatInterval_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "5");
        SplitBrainProtectionConfig recentlyActiveSplitBrainProtectionConfig = SplitBrainProtectionConfig
                .newRecentlyActiveSplitBrainProtectionConfigBuilder("test-splitBrainProtection", 3, 3000)
                .build();

        config.addSplitBrainProtectionConfig(recentlyActiveSplitBrainProtectionConfig);

        createHazelcastInstance(config);
    }

    private static class HazelcastInstanceAwareSplitBrainProtectionFunction implements SplitBrainProtectionFunction, HazelcastInstanceAware {

        private static volatile HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            HazelcastInstanceAwareSplitBrainProtectionFunction.instance = instance;
        }

        @Override
        public boolean apply(Collection<Member> members) {
            return false;
        }
    }

    private static class RecordingSplitBrainProtectionFunction implements SplitBrainProtectionFunction {

        private volatile boolean wasCalled;

        @Override
        public boolean apply(Collection<Member> members) {
            wasCalled = true;
            return false;
        }
    }
}
