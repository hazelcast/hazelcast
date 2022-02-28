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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.auditlog.impl.NoOpAuditlogService;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.LockGuard;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterStateManagerTest {

    private static final UUID TXN = UUID.randomUUID();
    private static final UUID ANOTHER_TXN = UUID.randomUUID();
    private static final MemberVersion CURRENT_NODE_VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    private static final Version CURRENT_CLUSTER_VERSION = Version.of(BuildInfoProvider.getBuildInfo().getVersion());
    private static final int MEMBERLIST_VERSION = 1;
    private static final long PARTITION_STAMP = 0;

    private final Node node = mock(Node.class);
    private final InternalPartitionService partitionService = mock(InternalPartitionService.class);
    private final MembershipManager membershipManager = mock(MembershipManager.class);
    private final ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
    private final Lock lock = mock(Lock.class);

    private ClusterStateManager clusterStateManager;

    @Before
    public void setup() {
        NodeExtension nodeExtension = mock(NodeExtension.class);
        when(nodeExtension.isStartCompleted()).thenReturn(true);
        when(nodeExtension.getAuditlogService()).thenReturn(NoOpAuditlogService.INSTANCE);
        when(nodeExtension.isNodeVersionCompatibleWith(ArgumentMatchers.any(Version.class))).thenReturn(true);

        when(node.getPartitionService()).thenReturn(partitionService);
        when(node.getClusterService()).thenReturn(clusterService);
        when(node.getNodeExtension()).thenReturn(nodeExtension);
        when(node.getLogger(ClusterStateManager.class)).thenReturn(mock(ILogger.class));
        when(node.getVersion()).thenReturn(CURRENT_NODE_VERSION);

        when(clusterService.getMembershipManager()).thenReturn(membershipManager);
        when(clusterService.getClusterJoinManager()).thenReturn(mock(ClusterJoinManager.class));

        when(clusterService.getMemberListVersion()).thenReturn(MEMBERLIST_VERSION);
        when(membershipManager.getMemberListVersion()).thenReturn(MEMBERLIST_VERSION);
        when(partitionService.getPartitionStateStamp()).thenReturn(PARTITION_STAMP);

        clusterStateManager = new ClusterStateManager(node, lock);
    }

    @Test
    public void test_defaultState() {
        assertEquals(ACTIVE, clusterStateManager.getState());
    }

    @Test
    public void test_initialClusterState_ACTIVE() {
        clusterStateManager.initialClusterState(ACTIVE, CURRENT_CLUSTER_VERSION);
        assertEquals(ACTIVE, clusterStateManager.getState());
        assertEquals(CURRENT_CLUSTER_VERSION, clusterStateManager.getClusterVersion());
    }

    @Test
    public void test_initialClusterState_FROZEN() {
        clusterStateManager.initialClusterState(FROZEN, CURRENT_CLUSTER_VERSION);
        assertEquals(FROZEN, clusterStateManager.getState());
        assertEquals(CURRENT_CLUSTER_VERSION, clusterStateManager.getClusterVersion());
    }

    @Test
    public void test_initialClusterState_PASSIVE() {
        clusterStateManager.initialClusterState(PASSIVE, CURRENT_CLUSTER_VERSION);
        assertEquals(PASSIVE, clusterStateManager.getState());
        assertEquals(CURRENT_CLUSTER_VERSION, clusterStateManager.getClusterVersion());
    }

    @Test
    public void test_initialClusterState_rejected() {
        clusterStateManager.initialClusterState(FROZEN, CURRENT_CLUSTER_VERSION);
        clusterStateManager.initialClusterState(ACTIVE, CURRENT_CLUSTER_VERSION);

        assertEquals(FROZEN, clusterStateManager.getState());
        assertEquals(CURRENT_CLUSTER_VERSION, clusterStateManager.getClusterVersion());
    }

    @Test(expected = NullPointerException.class)
    public void test_lockClusterState_nullState() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(null, initiator, TXN, 1000, MEMBERLIST_VERSION, PARTITION_STAMP);
    }

    @Test(expected = NullPointerException.class)
    public void test_lockClusterState_nullInitiator() {
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), null, TXN, 1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
    }

    @Test(expected = NullPointerException.class)
    public void test_lockClusterState_nullTransactionId() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, null, 1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_lockClusterState_nonPositiveLeaseTime() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, -1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
    }

    @Test
    public void test_lockClusterState_success() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, 1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);

        assertLockedBy(initiator);
    }

    @Test(expected = TransactionException.class)
    public void test_lockClusterState_fail() throws Exception {
        Address initiator = newAddress();
        ClusterStateChange newState = ClusterStateChange.from(FROZEN);
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000, MEMBERLIST_VERSION, PARTITION_STAMP);

        clusterStateManager.lockClusterState(newState, initiator, ANOTHER_TXN, 1000, MEMBERLIST_VERSION, PARTITION_STAMP);
    }

    @Test(expected = IllegalStateException.class)
    public void test_lockClusterState_forFrozenState_whenHasOnGoingMigration() throws Exception {
        when(partitionService.hasOnGoingMigrationLocal()).thenReturn(true);

        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, 1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
    }

    @Test(expected = IllegalStateException.class)
    public void test_lockClusterState_forActiveState_whenHasOnGoingMigration() throws Exception {
        when(partitionService.hasOnGoingMigrationLocal()).thenReturn(true);
        clusterStateManager.initialClusterState(FROZEN, CURRENT_CLUSTER_VERSION);

        Address initiator = newAddress();
        ClusterStateChange newState = ClusterStateChange.from(ACTIVE);
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000, MEMBERLIST_VERSION, PARTITION_STAMP);

        assertLockedBy(initiator);
    }

    @Test(expected = IllegalStateException.class)
    public void test_lockClusterState_forPassiveState_whenHasOnGoingMigration() throws Exception {
        when(partitionService.hasOnGoingMigrationLocal()).thenReturn(true);
        clusterStateManager.initialClusterState(FROZEN, CURRENT_CLUSTER_VERSION);

        Address initiator = newAddress();
        ClusterStateChange newState = ClusterStateChange.from(PASSIVE);
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000, MEMBERLIST_VERSION, PARTITION_STAMP);

        assertLockedBy(initiator);
    }

    @Test(expected = NullPointerException.class)
    public void test_unlockClusterState_nullTransactionId() {
        clusterStateManager.rollbackClusterState(null);
    }

    @Test
    public void test_unlockClusterState_fail_whenNotLocked() {
        assertFalse(clusterStateManager.rollbackClusterState(TXN));
    }

    @Test
    public void test_unlockClusterState_fail_whenLockedByElse() throws Exception {
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), newAddress(), TXN, 1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
        assertFalse(clusterStateManager.rollbackClusterState(ANOTHER_TXN));
    }

    @Test(expected = IllegalStateException.class)
    public void test_lockClusterState_fail_withDifferentPartitionStateStamps() throws Exception {
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), newAddress(), TXN, 1000, MEMBERLIST_VERSION, PARTITION_STAMP
                + 1);
    }

    @Test(expected = IllegalStateException.class)
    public void test_lockClusterState_fail_withDifferentMemberListVersions() throws Exception {
        clusterStateManager.clusterVersion = Versions.CURRENT_CLUSTER_VERSION;
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), newAddress(), TXN, 1000, MEMBERLIST_VERSION - 1,
                PARTITION_STAMP);
    }

    @Test
    public void test_unlockClusterState_success() throws Exception {
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), newAddress(), TXN, 1000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
        assertTrue(clusterStateManager.rollbackClusterState(TXN));
    }

    @Test
    public void test_lockClusterState_getLockExpiryTime() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, TimeUnit.DAYS.toMillis(1), MEMBERLIST_VERSION,
                PARTITION_STAMP);

        LockGuard stateLock = clusterStateManager.getStateLock();
        assertTrue(Clock.currentTimeMillis() + TimeUnit.HOURS.toMillis(12) < stateLock.getLockExpiryTime());
    }

    @Test
    public void test_lockClusterState_extendLease() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, 10000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, TimeUnit.DAYS.toMillis(1), MEMBERLIST_VERSION,
                PARTITION_STAMP);

        LockGuard stateLock = clusterStateManager.getStateLock();
        assertTrue(Clock.currentTimeMillis() + TimeUnit.HOURS.toMillis(12) < stateLock.getLockExpiryTime());
    }

    private void assertLockedBy(Address initiator) {
        assertEquals(IN_TRANSITION, clusterStateManager.getState());
        LockGuard stateLock = clusterStateManager.getStateLock();
        assertTrue(stateLock.isLocked());
        assertEquals(TXN, stateLock.getLockOwnerId());
        assertEquals(initiator, stateLock.getLockOwner());
    }

    @Test
    public void test_lockClusterState_expiry() throws Exception {
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), newAddress(), TXN, 1, MEMBERLIST_VERSION,
                PARTITION_STAMP);
        assertTrueEventually(() -> {
            LockGuard stateLock = clusterStateManager.getStateLock();
            assertFalse(stateLock.isLocked());
            assertEquals(ACTIVE, clusterStateManager.getState());
        });
    }

    @Test(expected = NullPointerException.class)
    public void test_changeLocalClusterState_nullState() throws Exception {
        clusterStateManager.commitClusterState(null, newAddress(), TXN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_changeLocalClusterState_IN_TRANSITION() throws Exception {
        clusterStateManager.commitClusterState(ClusterStateChange.from(IN_TRANSITION), newAddress(), TXN);
    }

    @Test(expected = NullPointerException.class)
    public void test_changeLocalClusterState_nullTransactionId() throws Exception {
        clusterStateManager.commitClusterState(ClusterStateChange.from(FROZEN), newAddress(), null);
    }

    @Test(expected = TransactionException.class)
    public void test_changeLocalClusterState_fail_notLocked() throws Exception {
        clusterStateManager.commitClusterState(ClusterStateChange.from(FROZEN), newAddress(), TXN);
    }

    @Test(expected = TransactionException.class)
    public void test_changeLocalClusterState_fail_whenLockedByElse() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(ClusterStateChange.from(FROZEN), initiator, TXN, 10000, MEMBERLIST_VERSION,
                PARTITION_STAMP);
        clusterStateManager.commitClusterState(ClusterStateChange.from(FROZEN), initiator, ANOTHER_TXN);
    }

    @Test
    public void test_changeLocalClusterState_success() throws Exception {
        ClusterStateChange newState = ClusterStateChange.from(FROZEN);
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(newState, initiator, TXN, 10000, MEMBERLIST_VERSION, PARTITION_STAMP);
        clusterStateManager.commitClusterState(newState, initiator, TXN);

        assertEquals(newState.getNewState(), clusterStateManager.getState());
        LockGuard stateLock = clusterStateManager.getStateLock();
        assertFalse(stateLock.isLocked());
    }

    @Test
    public void changeLocalClusterState_shouldChangeNodeStateToShuttingDown_whenStateBecomes_PASSIVE() throws Exception {
        ClusterStateChange newState = ClusterStateChange.from(PASSIVE);
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(newState, initiator, TXN, 10000, MEMBERLIST_VERSION, PARTITION_STAMP);
        clusterStateManager.commitClusterState(newState, initiator, TXN);

        assertEquals(newState.getNewState(), clusterStateManager.getState());
        verify(node, times(1)).changeNodeStateToPassive();
    }

    @Test
    public void changeLocalClusterState_shouldRemoveMembersDeadWhileFrozen_whenStateBecomes_ACTIVE() throws Exception {
        ClusterStateChange newState = ClusterStateChange.from(ACTIVE);
        Address initiator = newAddress();
        clusterStateManager.initialClusterState(FROZEN, CURRENT_CLUSTER_VERSION);
        clusterStateManager.lockClusterState(newState, initiator, TXN, 10000, MEMBERLIST_VERSION, PARTITION_STAMP);
        clusterStateManager.commitClusterState(newState, initiator, TXN);

        assertEquals(newState.getNewState(), clusterStateManager.getState());
        verify(membershipManager, times(1)).removeAllMissingMembers();
    }

    private Address newAddress() throws UnknownHostException {
        return new Address(InetAddress.getLocalHost(), 5000);
    }
}
