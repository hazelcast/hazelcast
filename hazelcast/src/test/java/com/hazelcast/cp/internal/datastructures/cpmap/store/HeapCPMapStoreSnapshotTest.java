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

package com.hazelcast.cp.internal.datastructures.cpmap.store;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HeapCPMapStoreSnapshotTest extends HazelcastRaftTestSupport {

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
    public void usedDataBytesSnapshotted() throws Exception {
        String mapName = "testCPMap";
        CPMap<String, String> testCPMap = getCPSubsystem().getMap(mapName);
        CPGroupId groupId = ((CPMapProxy) testCPMap).getGroupId();

        for (int i = 0; i < SNAPSHOT_THRESHOLD; i++) {
            String key = randomString();
            String value = randomString();
            testCPMap.put(key, value);
        }

        checkSnapshot(getLeaderNode(instances, groupId));

        Node leaderNode = ((HazelcastInstanceProxy) getLeaderInstance(instances, groupId)).getOriginal().node;
        CPMapService leaderCpMapService = leaderNode.nodeEngine.getService(CPMapService.SERVICE_NAME);
        com.hazelcast.cp.internal.datastructures.cpmap.store.HeapCPMapStore leaderHeapCPMapStore = (HeapCPMapStore) leaderCpMapService.getOrInitMapStore(groupId, mapName);
        int leaderUsedDataBytes = leaderHeapCPMapStore.getUsedDataBytes();
        assertTrue(leaderUsedDataBytes > 0);

        // shutdown the last instance
        instances[instances.length - 1].shutdown();

        HazelcastInstance instance = factory.newHazelcastInstance(createConfig(3, 3));
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        assertTrueEventually(() -> assertNotNull(getRaftNode(instance, groupId)));
        checkSnapshot(getRaftNode(instance, groupId));

        Node node = ((HazelcastInstanceProxy) instance).getOriginal().node;
        CPMapService cpMapService = node.nodeEngine.getService(CPMapService.SERVICE_NAME);
        HeapCPMapStore heapCPMapStore = (HeapCPMapStore) cpMapService.getOrInitMapStore(groupId, mapName);
        assertTrueEventually(() -> assertEquals(leaderUsedDataBytes, heapCPMapStore.getUsedDataBytes()), 10);
    }

    private void checkSnapshot(RaftNodeImpl instance) {
        assertTrueEventually(() -> {
            SnapshotEntry snapshotEntry = instance.state().log().snapshot();
            assertGreaterOrEquals("snapshot size", snapshotEntry.index(), SNAPSHOT_THRESHOLD);
        });
    }

}
