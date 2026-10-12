/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.LocalGetOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapGetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.instance.impl.TestUtil.toData;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CPMapAdvancedSnapshotTest extends HazelcastRaftTestSupport {

    private static final int SNAPSHOT_THRESHOLD = 100;

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        instances = newInstances(3);
    }

    protected CPSubsystem getCPSubsystem() {
        return instances[0].getCPSubsystem();
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(SNAPSHOT_THRESHOLD);
        return config;
    }

    @Test
    public void singleCPGroupWithUniqueKeys() throws Exception {
        String mapName = "testCPMap";
        CPMap<String, String> testCPMap = getCPSubsystem().getMap(mapName);
        CPGroupId groupId = ((CPMapProxy) testCPMap).getGroupId();

        Map<String, String> localMap = new HashMap<>();

        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            String key = randomString();
            String value = randomString();
            testCPMap.put(key, value);
            localMap.put(key, value);
        }

        checkSnapshot(getLeaderNode(instances, groupId));

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        // Read from local CP member, which should install snapshot after promotion
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance);
        for (Map.Entry<String, String> entry : localMap.entrySet()) {
            assertTrueEventually(() -> {
                InternalCompletableFuture<Object> future
                        = invocationManager.queryLocally(
                        groupId,
                        getQueryRaftOp(mapName, entry.getKey()),
                        QueryPolicy.ANY_LOCAL
                );
                try {
                    String value = getValue(future);
                    assertEquals(entry.getValue(), value);
                } catch (CPSubsystemException e) {
                    // Raft node may not be created yet...
                    throw new AssertionError(e);
                }
            });
        }

        checkSnapshot(getRaftNode(instance, groupId));
    }

    @Test
    // testing that snapshot can restore if no CPMap objects in snapshot
    public void noCpMapInCPGroup() throws Exception {
        String aLongName = "testAtomicLong";
        IAtomicLong aLong = getCPSubsystem().getAtomicLong(aLongName);
        CPGroupId groupId = ((AtomicLongProxy) aLong).getGroupId();

        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            aLong.incrementAndGet();
        }
        long val = aLong.get();

        checkSnapshot(getLeaderNode(instances, groupId));

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        // Read from local CP member, which should install snapshot after promotion
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance);
        assertTrueEventually(() -> {
            InternalCompletableFuture<Object> future
                    = invocationManager.queryLocally(
                    groupId,
                    new LocalGetOp(aLongName),
                    QueryPolicy.ANY_LOCAL
            );
            try {
                long value = getValue(future);
                assertEquals(val, value);
            } catch (CPSubsystemException e) {
                // Raft node may not be created yet...
                throw new AssertionError(e);
            }
        });

        checkSnapshot(getRaftNode(instance, groupId));
    }

    @Test
    public void singleCPGroupWithSeveralMaps() throws Exception {
        String mapName1 = "testCPMap1";
        String mapName2 = "testCPMap2";
        CPMap<String, String> testCPMap1 = getCPSubsystem().getMap(mapName1);
        CPMap<String, String> testCPMap2 = getCPSubsystem().getMap(mapName2);
        CPGroupId groupId = ((CPMapProxy) testCPMap1).getGroupId();

        Map<String, String> localMap = new HashMap<>();

        for (int i = 0; i < (SNAPSHOT_THRESHOLD / 2); i++) {
            String key = randomString();
            String value = randomString();
            testCPMap1.put(key, value);
            testCPMap2.put(key, value);
            localMap.put(key, value);
        }

        checkSnapshot(getLeaderNode(instances, groupId));

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        // Read from local CP member, which should install snapshot after promotion
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance);
        for (Map.Entry<String, String> entry : localMap.entrySet()) {
            assertTrueEventually(() -> {
                InternalCompletableFuture<Object> future1
                        = invocationManager.queryLocally(
                        groupId,
                        getQueryRaftOp(mapName1, entry.getKey()),
                        QueryPolicy.ANY_LOCAL
                );
                InternalCompletableFuture<Object> future2
                        = invocationManager.queryLocally(
                        groupId,
                        getQueryRaftOp(mapName2, entry.getKey()),
                        QueryPolicy.ANY_LOCAL
                );
                try {
                    String value1 = getValue(future1);
                    assertEquals(entry.getValue(), value1);
                    String value2 = getValue(future2);
                    assertEquals(entry.getValue(), value2);
                } catch (CPSubsystemException e) {
                    // Raft node may not be created yet...
                    throw new AssertionError(e);
                }
            });
        }

        checkSnapshot(getRaftNode(instance, groupId));
    }

    @Test
    public void severalCPGroups() throws Exception {
        String mapName1 = "testCPMap1";
        String mapName2 = "testCPMap2";
        CPMap<String, String> testCPMap1 = getCPSubsystem().getMap(mapName1 + "@group1");
        CPMap<String, String> testCPMap2 = getCPSubsystem().getMap(mapName2 + "@group2");
        CPGroupId groupId1 = ((CPMapProxy) testCPMap1).getGroupId();
        CPGroupId groupId2 = ((CPMapProxy) testCPMap2).getGroupId();

        Map<String, String> localMap = new HashMap<>();

        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            String key = randomString();
            String value = randomString();
            testCPMap1.put(key, value);
            testCPMap2.put(key, value);
            localMap.put(key, value);
        }

        checkSnapshot(getLeaderNode(instances, groupId1));
        checkSnapshot(getLeaderNode(instances, groupId2));

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        // Read from local CP member, which should install snapshot after promotion
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance);
        for (Map.Entry<String, String> entry : localMap.entrySet()) {
            assertTrueEventually(() -> {
                InternalCompletableFuture<Object> future1
                        = invocationManager.queryLocally(
                        groupId1,
                        getQueryRaftOp(mapName1, entry.getKey()),
                        QueryPolicy.ANY_LOCAL
                );
                InternalCompletableFuture<Object> future2
                        = invocationManager.queryLocally(
                        groupId2,
                        getQueryRaftOp(mapName2, entry.getKey()),
                        QueryPolicy.ANY_LOCAL
                );
                try {
                    String value1 = getValue(future1);
                    assertEquals(entry.getValue(), value1);
                    String value2 = getValue(future2);
                    assertEquals(entry.getValue(), value2);
                } catch (CPSubsystemException e) {
                    // Raft node may not be created yet...
                    throw new AssertionError(e);
                }
            });
        }

        checkSnapshot(getRaftNode(instance, groupId1));
        checkSnapshot(getRaftNode(instance, groupId2));
    }

    @Test
    public void destroyedMapNamesSnapshotted() throws Exception {
        String mapName1 = "testCPMap1";
        String mapName2 = "testCPMap2";
        CPMap<String, String> testCPMap1 = getCPSubsystem().getMap(mapName1);
        CPMap<String, String> testCPMap2 = getCPSubsystem().getMap(mapName2);
        CPGroupId groupId = ((CPMapProxy) testCPMap1).getGroupId();

        testCPMap2.put("key", "value");
        testCPMap2.destroy();

        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            String key = randomString();
            String value = randomString();
            testCPMap1.put(key, value);
        }

        checkSnapshot(getLeaderNode(instances, groupId));

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        // Read from local CP member, which should install snapshot after promotion
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance);
        assertTrueEventually(() -> {
            InternalCompletableFuture<Object> future
                    = invocationManager.queryLocally(
                    groupId,
                    getQueryRaftOp(mapName2, "key"),
                    QueryPolicy.ANY_LOCAL
            );
            try {
                assertThrows(DistributedObjectDestroyedException.class, () -> getValue(future));
            } catch (CPSubsystemException e) {
                // Raft node may not be created yet...
                throw new AssertionError(e);
            }
        });

        checkSnapshot(getRaftNode(instance, groupId));
    }

    private RaftOp getQueryRaftOp(String mapName, String key) {
        return new CPMapGetOp(mapName, toData(key));
    }

    private void checkSnapshot(RaftNodeImpl instance) {
        assertTrueEventually(() -> {
            SnapshotEntry snapshotEntry = instance.state().log().snapshot();
            assertGreaterOrEquals("snapshot size", snapshotEntry.index(), SNAPSHOT_THRESHOLD);
        });
    }

    protected <T> T getValue(InternalCompletableFuture<Object> future) {
        return (T) future.joinInternal();
    }
}
