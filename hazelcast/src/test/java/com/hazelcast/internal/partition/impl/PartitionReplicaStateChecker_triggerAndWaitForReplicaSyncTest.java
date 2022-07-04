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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.partition.impl.PartitionServiceState.MIGRATION_LOCAL;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.MIGRATION_ON_MASTER;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.REPLICA_NOT_OWNED;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.REPLICA_NOT_SYNC;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.SAFE;
import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaStateChecker_triggerAndWaitForReplicaSyncTest extends HazelcastTestSupport {

    private List<InternalPartition> partitions = new ArrayList<InternalPartition>();

    private Node node;
    private PartitionStateManager partitionStateManager;
    private MigrationManager migrationManager;

    private PartitionReplicaStateChecker replicaStateChecker;

    @Before
    public void setUp() {
        ILogger logger = getLogger(PartitionReplicaStateChecker_triggerAndWaitForReplicaSyncTest.class);
        ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
        when(clusterService.getClusterState()).thenReturn(ClusterState.ACTIVE);

        node = mock(Node.class);
        when(node.getLogger(any(Class.class))).thenReturn(logger);
        when(node.getClusterService()).thenReturn(clusterService);

        partitionStateManager = mock(PartitionStateManager.class);
        when(partitionStateManager.getPartitions()).thenAnswer(new Answer<InternalPartition[]>() {
            @Override
            public InternalPartition[] answer(InvocationOnMock invocationOnMock) throws Throwable {
                InternalPartition[] partitionsArray = new InternalPartition[partitions.size()];
                return partitions.toArray(partitionsArray);
            }
        });

        migrationManager = mock(MigrationManager.class);

        InternalPartitionServiceImpl partitionService = mock(InternalPartitionServiceImpl.class);
        when(partitionService.getPartitionStateManager()).thenReturn(partitionStateManager);
        when(partitionService.getMigrationManager()).thenReturn(migrationManager);

        replicaStateChecker = new PartitionReplicaStateChecker(node, partitionService);
    }

    @Test
    public void whenCalledWithZeroTimeout_thenDoNothing() {
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void whenHasMissingReplicaOwners_withAddress_thenWaitForMissingReplicaOwners() throws Exception {
        configureNeedsReplicaStateCheckResponse();

        Address address = new Address("127.0.0.1", 5701);
        PartitionReplica replica = new PartitionReplica(address, UuidUtil.newUnsecureUUID());
        InternalPartition partition = new ReadonlyInternalPartition(new PartitionReplica[]{replica}, 1, 0);
        partitions.add(partition);

        assertEquals(REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenHasMissingReplicaOwners_withoutAddress_thenWaitForMissingReplicaOwners() {
        configureNeedsReplicaStateCheckResponse();

        InternalPartition partition = new ReadonlyInternalPartition(new PartitionReplica[0], 1, 0);
        partitions.add(partition);

        assertEquals(REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenHasOngoingMigration_withLocalMigration_thenWaitForOngoingMigrations() {
        when(migrationManager.hasOnGoingMigration()).thenReturn(true);

        assertEquals(MIGRATION_LOCAL, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenHasOngoingMigration_withMigrationOnMaster_thenWaitForOngoingMigrations() {
        when(node.getMasterAddress()).thenReturn(null);
        when(node.getClusterService().isJoined()).thenReturn(true);

        assertEquals(MIGRATION_ON_MASTER, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenCheckAndTriggerReplicaSync() {
        configureNeedsReplicaStateCheckResponseOnEachSecondCall();

        InternalPartition partition = new ReadonlyInternalPartition(new PartitionReplica[]{null}, 1, 0);
        partitions.add(partition);

        assertEquals(REPLICA_NOT_SYNC, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenCheckAndTriggerReplicaSync_withNeedsReplicaStateCheck_thenReturnTrue() {
        configureNeedsReplicaStateCheckResponseOnEachSecondCall();

        assertEquals(SAFE, replicaStateChecker.getPartitionServiceState());
        assertTrue(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenCheckAndTriggerReplicaSync_withoutNeedsReplicaStateCheck_thenReturnTrue() {
        assertEquals(SAFE, replicaStateChecker.getPartitionServiceState());
        assertTrue(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    private void configureNeedsReplicaStateCheckResponse() {
        when(partitionStateManager.isInitialized()).thenReturn(true);
        when(partitionStateManager.getMemberGroupsSize()).thenReturn(1);
    }

    private void configureNeedsReplicaStateCheckResponseOnEachSecondCall() {
        when(partitionStateManager.isInitialized()).thenAnswer(new AlternatingAnswer());
        when(partitionStateManager.getMemberGroupsSize()).thenReturn(1);
    }

    /**
     * Alternately returns {@code true} and {@code false}, beginning with {@code false}.
     */
    private static class AlternatingAnswer implements Answer<Boolean> {

        private boolean state = true;

        @Override
        public Boolean answer(InvocationOnMock invocationOnMock) {
            state = !state;
            return state;
        }
    }
}
