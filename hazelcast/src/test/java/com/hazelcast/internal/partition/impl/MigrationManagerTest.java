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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationManagerTest {

    Node node;
    InternalPartitionServiceImpl partitionService;
    PartitionStateManager partitionStateManager;
    ClusterServiceImpl clusterService;
    Lock lock = new ReentrantLock();
    MigrationManager.RedoPartitioningTask task;

    @Before
    public void setup() {
        setupMocks();
        MigrationManager migrationManager = new MigrationManager(node, partitionService, lock);
        task = migrationManager.new RedoPartitioningTask();
    }

    @Test
    public void testCheckSnapshots_whenNoSnapshotsExist()
            throws UnknownHostException {
        when(clusterService.getMembers(any())).thenReturn(members(4));
        assertNull(task.checkSnapshots());
    }

    @Test
    public void testCheckSnapshots_whenSingleMatchingSnapshotExists()
            throws UnknownHostException {
        // cluster had 3 members; one crashed ->
        //   snapshot partition table references 3 members
        //   current partition table references 2 remaining members
        //   crashed member is rejoining, cluster service reports 3 data members
        List<Member> newClusterMembers = members(3);
        List<Member> currentClusterMembers = new ArrayList<>(newClusterMembers);
        currentClusterMembers.remove(2);
        PartitionTableView currentPartitionTable = new PartitionTableView(partitions(currentClusterMembers));
        PartitionTableView snapshot = new PartitionTableView(partitions(newClusterMembers));

        when(clusterService.getMembers(any())).thenReturn(newClusterMembers);
        when(partitionStateManager.getPartitionTable())
                .thenReturn(currentPartitionTable);
        when(partitionStateManager.snapshots()).thenReturn(Collections.singletonList(snapshot));

        assertArrayEquals(snapshot.toArray(new HashMap<>()), task.checkSnapshots());
    }

    @Test
    public void testCheckSnapshots_picksMatching_whenOtherSnapshotsExist()
            throws UnknownHostException {
        // cluster had 3 members; one crashed ->
        //   snapshot partition table references 3 members
        //   current partition table references 2 remaining members
        //   crashed member is rejoining, cluster service reports 3 data members
        List<Member> newClusterMembers = members(3);
        List<Member> currentClusterMembers = new ArrayList<>(newClusterMembers);
        currentClusterMembers.remove(2);
        PartitionTableView currentPartitionTable = new PartitionTableView(partitions(currentClusterMembers));
        PartitionTableView matchingSnapshot = new PartitionTableView(partitions(newClusterMembers));
        // add an unrelated snapshot
        PartitionTableView unrelated = new PartitionTableView(partitions(members(3)));
        Collection<PartitionTableView> snapshots = new ArrayList<>();
        snapshots.add(matchingSnapshot);
        snapshots.add(unrelated);

        when(clusterService.getMembers(any())).thenReturn(newClusterMembers);
        when(partitionStateManager.getPartitionTable()).thenReturn(currentPartitionTable);
        when(partitionStateManager.snapshots()).thenReturn(snapshots);

        assertArrayEquals(matchingSnapshot.toArray(new HashMap<>()), task.checkSnapshots());
    }

    @Test
    public void testCheckSnapshots_picksBest_whenMultipleSnapshotsMatch()
            throws UnknownHostException {
        List<Member> membersWhenRepartitioning = members(3);
        // cluster had 3 members; one crashed ->
        //   snapshot partition table references 3 members
        PartitionTableView snapshotBestMatch = new PartitionTableView(partitions(membersWhenRepartitioning));

        // another crashed ->
        //   snapshots contains another partition table with a single member
        List<Member> singleMemberCluster = cloneRemoving(membersWhenRepartitioning, 2);
        PartitionTableView snapshotSingleMember = new PartitionTableView(partitions(singleMemberCluster));

        // one member already rejoined -> current partition table references 2 members
        // crashed member is rejoining, cluster service reports 3 data members
        //   -> snapshot partition table with 3 members is picked as better match
        List<Member> currentClusterMembers = cloneRemoving(membersWhenRepartitioning, 1);
        PartitionTableView currentPartitionTable = new PartitionTableView(partitions(currentClusterMembers));

        Collection<PartitionTableView> snapshots = new ArrayList<>();
        snapshots.add(snapshotSingleMember);
        snapshots.add(snapshotBestMatch);

        when(clusterService.getMembers(any())).thenReturn(membersWhenRepartitioning);
        when(partitionStateManager.getPartitionTable())
                .thenReturn(currentPartitionTable);
        when(partitionStateManager.snapshots()).thenReturn(snapshots);

        assertArrayEquals(snapshotBestMatch.toArray(new HashMap<>()), task.checkSnapshots());
    }

    @Test
    public void testCheckSnapshots_picksMatchingSnapshot_whenAddressChanged()
            throws UnknownHostException {
        // setup same as testCheckSnapshots_picksBest_whenMultipleSnapshotsMatch
        List<Member> membersWhenRepartitioning = members(3);
        PartitionTableView snapshotBestMatch = new PartitionTableView(partitions(membersWhenRepartitioning));
        List<Member> singleMemberCluster = cloneRemoving(membersWhenRepartitioning, 2);
        PartitionTableView snapshotSingleMember = new PartitionTableView(partitions(singleMemberCluster));
        List<Member> currentClusterMembers = cloneRemoving(membersWhenRepartitioning, 1);
        PartitionTableView currentPartitionTable = new PartitionTableView(partitions(currentClusterMembers));
        // but member 3 changed address as it is rejoining
        Member crashedMember = membersWhenRepartitioning.remove(2);
        Member rejoiningWithChangedAddress = new MemberImpl(new Address("127.0.0.1",
                crashedMember.getAddress().getPort() + 10), crashedMember.getVersion(), crashedMember.localMember(),
                crashedMember.getUuid());
        membersWhenRepartitioning.add(rejoiningWithChangedAddress);

        Collection<PartitionTableView> snapshots = new ArrayList<>();
        snapshots.add(snapshotSingleMember);
        snapshots.add(snapshotBestMatch);

        when(clusterService.getMembers(any())).thenReturn(membersWhenRepartitioning);
        when(partitionStateManager.getPartitionTable())
                .thenReturn(currentPartitionTable);
        when(partitionStateManager.snapshots()).thenReturn(snapshots);

        // apply address change in snapshotBestMatch for assertion
        Map<UUID, Address> addressTranslationMap = new HashMap<>();
        addressTranslationMap.put(rejoiningWithChangedAddress.getUuid(), rejoiningWithChangedAddress.getAddress());
        assertArrayEquals(snapshotBestMatch.toArray(addressTranslationMap), task.checkSnapshots());
    }

    // clone given members list, removing removeCount from the end
    List<Member> cloneRemoving(List<Member> members, int removeCount) {
        List<Member> result = new ArrayList<>(members);
        for (int i = 0; i < removeCount; i++) {
            result.remove(result.size() - 1);
        }
        return result;
    }

    List<Member> members(int count)
            throws UnknownHostException {
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            members.add(new MemberImpl(new Address("127.0.0.1", 5700 + i),
                    MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion()),
                    false,
                    UUID.randomUUID()));
        }
        return members;
    }

    InternalPartition[] partitions(List<Member> members) {
        InternalPartition[] partitions = new InternalPartition[3];
        for (int i = 0; i < 3; i++) {
            partitions[i] = new ReadonlyInternalPartition(arrange(members), i, 0);
        }
        return partitions;
    }

    // assign each member to a replica index, according to their order in the list
    PartitionReplica[] arrange(List<Member> members) {
        PartitionReplica[] replicas = new PartitionReplica[InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < members.size(); i++) {
            replicas[i] = new PartitionReplica(members.get(i).getAddress(),
                    members.get(i).getUuid());
        }
        return replicas;
    }

    private void setupMocks() {
        node = mock(Node.class);
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        HazelcastInstance instance = mock(HazelcastInstance.class);
        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ManagedExecutorService asyncExecutor = mock(ManagedExecutorService.class);
        clusterService = mock(ClusterServiceImpl.class);

        when(node.getProperties()).thenReturn(new HazelcastProperties(new Config()));
        when(node.getNodeEngine()).thenReturn(nodeEngine);
        when(node.getClusterService()).thenReturn(clusterService);
        when(instance.getName()).thenReturn("dev");
        when(nodeEngine.getHazelcastInstance()).thenReturn(instance);
        when(nodeEngine.getMetricsRegistry()).thenReturn(metricsRegistry);
        when(executionService.getExecutor(any(String.class))).thenReturn(asyncExecutor);
        when(nodeEngine.getExecutionService()).thenReturn(executionService);
        when(clusterService.getClusterVersion()).thenReturn(Versions.CURRENT_CLUSTER_VERSION);

        partitionStateManager = mock(PartitionStateManager.class);
        partitionService = mock(InternalPartitionServiceImpl.class);
        when(partitionService.getPartitionStateManager()).thenReturn(partitionStateManager);


        when(node.getConfig()).thenReturn(new Config());
    }
}
