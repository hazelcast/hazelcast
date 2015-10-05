/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.cluster.ClusterState.SHUTTING_DOWN;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterStateManagerTest {

    private static final String TXN = "txn";
    private static final String ANOTHER_TXN = "another-txn";

    private final Node node = mock(Node.class);
    private final InternalPartitionService partitionService = mock(InternalPartitionService.class);
    private final ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
    private final Lock lock = mock(Lock.class);
    private final ClusterStateManager clusterStateManager = new ClusterStateManager(node, lock);

    @Before
    public void setup() {
        when(node.getPartitionService()).thenReturn(partitionService);
        when(node.getClusterService()).thenReturn(clusterService);
    }

    @Test
    public void test_defaultState() {
        assertEquals(ACTIVE, clusterStateManager.getState());
    }

    @Test
    public void test_initialClusterState_ACTIVE() {
        clusterStateManager.initialClusterState(ACTIVE);
        assertEquals(ACTIVE, clusterStateManager.getState());
    }

    @Test
    public void test_initialClusterState_FROZEN() {
        clusterStateManager.initialClusterState(FROZEN);
        assertEquals(FROZEN, clusterStateManager.getState());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_initialClusterState_SHUTTING_DOWN() {
        clusterStateManager.initialClusterState(SHUTTING_DOWN);
    }

    @Test
    public void test_initialClusterState_rejected() {
        clusterStateManager.initialClusterState(FROZEN);

        try {
            clusterStateManager.initialClusterState(ACTIVE);
            fail("Second 'initialClusterState' should fail!");
        } catch (IllegalStateException expected) {
        }
    }

    @Test(expected = NullPointerException.class)
    public void test_lockClusterState_nullState() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(null, initiator, TXN, 1000);
    }

    @Test(expected = NullPointerException.class)
    public void test_lockClusterState_nullInitiator() throws Exception {
        clusterStateManager.lockClusterState(FROZEN, null, TXN, 1000);
    }

    @Test(expected = NullPointerException.class)
    public void test_lockClusterState_nullTransactionId() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(FROZEN, initiator, null, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_lockClusterState_nonPositiveLeaseTime() throws Exception {
        Address initiator = newAddress();
        clusterStateManager.lockClusterState(FROZEN, initiator, TXN, -1000);
    }

    @Test
    public void test_lockClusterState_success() throws Exception {
        Address initiator = newAddress();
        final ClusterState newState = FROZEN;
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000);

        assertLockedBy(initiator);
    }

    @Test(expected = TransactionException.class)
    public void test_lockClusterState_fail() throws Exception {
        Address initiator = newAddress();
        final ClusterState newState = FROZEN;
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000);

        clusterStateManager.lockClusterState(newState, initiator, ANOTHER_TXN, 1000);
    }

    @Test(expected = IllegalStateException.class)
    public void test_lockClusterState_forFrozenState_whenHasOnGoingMigration() throws Exception {
        when(partitionService.hasOnGoingMigrationLocal()).thenReturn(true);

        Address initiator = newAddress();
        final ClusterState newState = FROZEN;
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000);
    }

    @Test
    public void test_lockClusterState_forActiveState_whenHasOnGoingMigration() throws Exception {
        when(partitionService.hasOnGoingMigrationLocal()).thenReturn(true);
        clusterStateManager.initialClusterState(FROZEN);

        Address initiator = newAddress();
        final ClusterState newState = ACTIVE;
        clusterStateManager.lockClusterState(newState, initiator, TXN, 1000);

        assertLockedBy(initiator);
    }

    @Test(expected = NullPointerException.class)
    public void test_unlockClusterState_nullTransactionId() throws Exception {
        clusterStateManager.rollbackClusterState(null);
    }

    @Test
    public void test_unlockClusterState_fail_whenNotLocked() throws Exception {
        assertFalse(clusterStateManager.rollbackClusterState(TXN));
    }

    @Test
    public void test_unlockClusterState_fail_whenLockedByElse() throws Exception {
        clusterStateManager.lockClusterState(FROZEN, newAddress(), TXN, 1000);
        assertFalse(clusterStateManager.rollbackClusterState(ANOTHER_TXN));
    }

    @Test
    public void test_unlockClusterState_success() throws Exception {
        clusterStateManager.lockClusterState(FROZEN, newAddress(), TXN, 1000);
        assertTrue(clusterStateManager.rollbackClusterState(TXN));
    }

    @Test
    public void test_lockClusterState_getLockExpiryTime() throws Exception {
        final Address initiator = newAddress();
        clusterStateManager.lockClusterState(FROZEN, initiator, TXN, TimeUnit.DAYS.toMillis(1));

        final ClusterStateLock stateLock = clusterStateManager.getStateLock();
        assertTrue(Clock.currentTimeMillis() + TimeUnit.HOURS.toMillis(12) < stateLock.getLockExpiryTime());
    }

    @Test
    public void test_lockClusterState_extendLease() throws Exception {
        final Address initiator = newAddress();
        clusterStateManager.lockClusterState(FROZEN, initiator, TXN, 10000);
        clusterStateManager.lockClusterState(FROZEN, initiator, TXN, TimeUnit.DAYS.toMillis(1));

        final ClusterStateLock stateLock = clusterStateManager.getStateLock();
        assertTrue(Clock.currentTimeMillis() + TimeUnit.HOURS.toMillis(12) < stateLock.getLockExpiryTime());
    }

    private void assertLockedBy(Address initiator) {
        assertEquals(IN_TRANSITION, clusterStateManager.getState());
        ClusterStateLock stateLock = clusterStateManager.getStateLock();
        assertTrue(stateLock.isLocked());
        assertEquals(TXN, stateLock.getTransactionId());
        assertEquals(initiator, stateLock.getLockOwner());
    }

    @Test
    public void test_lockClusterState_expiry() throws Exception {
        clusterStateManager.lockClusterState(FROZEN, newAddress(), TXN, 1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final ClusterStateLock stateLock = clusterStateManager.getStateLock();
                assertFalse(stateLock.isLocked());
                assertEquals(ACTIVE, clusterStateManager.getState());
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void test_changeLocalClusterState_nullState() throws Exception {
        clusterStateManager.commitClusterState(null, newAddress(), TXN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_changeLocalClusterState_IN_TRANSITION() throws Exception {
        clusterStateManager.commitClusterState(IN_TRANSITION, newAddress(), TXN);
    }

    @Test(expected = NullPointerException.class)
    public void test_changeLocalClusterState_nullTransactionId() throws Exception {
        clusterStateManager.commitClusterState(FROZEN, newAddress(), null);
    }

    @Test(expected = TransactionException.class)
    public void test_changeLocalClusterState_fail_notLocked() throws Exception {
        clusterStateManager.commitClusterState(FROZEN, newAddress(), TXN);
    }

    @Test(expected = TransactionException.class)
    public void test_changeLocalClusterState_fail_whenLockedByElse() throws Exception {
        final Address initiator = newAddress();
        clusterStateManager.lockClusterState(FROZEN, initiator, TXN, 10000);
        clusterStateManager.commitClusterState(FROZEN, initiator, ANOTHER_TXN);
    }

    @Test
    public void test_changeLocalClusterState_success() throws Exception {
        final ClusterState newState = FROZEN;
        final Address initiator = newAddress();
        clusterStateManager.lockClusterState(newState, initiator, TXN, 10000);
        clusterStateManager.commitClusterState(newState, initiator, TXN);

        assertEquals(newState, clusterStateManager.getState());
        final ClusterStateLock stateLock = clusterStateManager.getStateLock();
        assertFalse(stateLock.isLocked());
    }

    @Test
    public void changeLocalClusterState_shouldChangeNodeStateToShuttingDown_whenStateBecomes_SHUTTING_DOWN() throws Exception {
        final ClusterState newState = SHUTTING_DOWN;
        final Address initiator = newAddress();
        clusterStateManager.lockClusterState(newState, initiator, TXN, 10000);
        clusterStateManager.commitClusterState(newState, initiator, TXN);

        assertEquals(newState, clusterStateManager.getState());
        verify(node, times(1)).changeStateToShuttingDown();
    }

    @Test
    public void changeLocalClusterState_shouldRemoveMembersDeadWhileFrozen_whenStateBecomes_ACTIVE() throws Exception {
        final ClusterState newState = ACTIVE;
        final Address initiator = newAddress();
        clusterStateManager.initialClusterState(FROZEN);
        clusterStateManager.lockClusterState(newState, initiator, TXN, 10000);
        clusterStateManager.commitClusterState(newState, initiator, TXN);

        assertEquals(newState, clusterStateManager.getState());
        verify(clusterService, times(1)).removeMembersDeadWhileFrozen();
    }

    private Address newAddress() throws UnknownHostException {
        return new Address(InetAddress.getLocalHost(), 5000);
    }
}
