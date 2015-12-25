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
import com.hazelcast.cluster.impl.operations.LockClusterStateOperation;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.cluster.impl.ClusterServiceImpl.SERVICE_NAME;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

/**
 * ClusterStateManager stores cluster state and manages cluster state transactions.
 * <p/>
 * When a node joins to the cluster, its initial state is set.
 * <p/>
 * When a cluster state change is requested, a cluster-wide transaction is started
 * and state is changed all over the cluster atomically.
 */
public class ClusterStateManager {

    private static final TransactionOptions DEFAULT_TX_OPTIONS = new TransactionOptions()
            .setDurability(1)
            .setTimeout(1, TimeUnit.MINUTES)
            .setTransactionType(TransactionType.TWO_PHASE);

    private static final long LOCK_LEASE_EXTENSION_MILLIS = TimeUnit.SECONDS.toMillis(20);

    private final Node node;
    private final ILogger logger;
    private final Lock clusterServiceLock;
    private final AtomicReference<ClusterStateLock> stateLockRef
            = new AtomicReference<ClusterStateLock>(ClusterStateLock.NOT_LOCKED);
    private volatile ClusterState state = ClusterState.ACTIVE;

    ClusterStateManager(Node node, Lock clusterServiceLock) {
        this.node = node;
        this.clusterServiceLock = clusterServiceLock;
        logger = node.getLogger(getClass());
    }

    public ClusterState getState() {
        ClusterStateLock stateLock = getStateLock();
        return stateLock.isLocked() ? ClusterState.IN_TRANSITION : state;
    }

    ClusterStateLock getStateLock() {
        ClusterStateLock stateLock = stateLockRef.get();
        while (stateLock.isLeaseExpired()) {
            if (stateLockRef.compareAndSet(stateLock, ClusterStateLock.NOT_LOCKED)) {
                stateLock = ClusterStateLock.NOT_LOCKED;
                break;
            }
            stateLock = stateLockRef.get();
        }
        return stateLock;
    }

    void initialClusterState(ClusterState initialState) {
        clusterServiceLock.lock();
        try {
            final ClusterState currentState = getState();
            if (currentState != ClusterState.ACTIVE && currentState != initialState) {
                logger.warning("Initial state is already set! " + "Current state: " + currentState + ", Given state: "
                        + initialState);
                return;
            }
            this.state = initialState;
            changeNodeState(initialState);
            node.getNodeExtension().onClusterStateChange(initialState, false);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void setClusterState(ClusterState newState, boolean persistentChange) {
        clusterServiceLock.lock();
        try {
            this.state = newState;
            stateLockRef.set(ClusterStateLock.NOT_LOCKED);
            changeNodeState(newState);
            node.getNodeExtension().onClusterStateChange(newState, persistentChange);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void reset() {
        clusterServiceLock.lock();
        try {
            state = ClusterState.ACTIVE;
            stateLockRef.set(ClusterStateLock.NOT_LOCKED);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    public void lockClusterState(ClusterState newState, Address initiator, String txnId,
                                 long leaseTime, int partitionStateVersion) {
        Preconditions.checkNotNull(newState);
        clusterServiceLock.lock();
        try {
            checkMigrationsAndPartitionStateVersion(newState, partitionStateVersion);

            final ClusterStateLock currentLock = getStateLock();
            if (!currentLock.allowsLock(txnId)) {
                throw new TransactionException("Locking failed for " + initiator + ", tx: " + txnId
                        + ", current state: " + toString());
            }

            stateLockRef.set(new ClusterStateLock(initiator, txnId, leaseTime));

            try {
                // check migration status and partition-state version again
                // if partition state is changed then release the lock and fail.
                checkMigrationsAndPartitionStateVersion(newState, partitionStateVersion);
            } catch (IllegalStateException e) {
                stateLockRef.set(ClusterStateLock.NOT_LOCKED);
                throw e;
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void checkMigrationsAndPartitionStateVersion(ClusterState newState, int partitionStateVersion) {
        final InternalPartitionService partitionService = node.getPartitionService();
        final int thisPartitionStateVersion = partitionService.getPartitionStateVersion();

        if (partitionService.hasOnGoingMigrationLocal()) {
            throw new IllegalStateException("Still have pending migration tasks, "
                    + "cannot lock cluster state! New state: " + newState
                    + ", current state: " + getState());
        } else  if (partitionStateVersion != thisPartitionStateVersion) {
            throw new IllegalStateException("Can not lock cluster state! Partition tables have different versions! "
                    + "Expected version: " + partitionStateVersion + " Current version: " + thisPartitionStateVersion);
        }
    }

    public boolean rollbackClusterState(String txnId) {
        clusterServiceLock.lock();
        try {
            final ClusterStateLock currentLock = getStateLock();
            if (!currentLock.allowsUnlock(txnId)) {
                return false;
            }

            stateLockRef.set(ClusterStateLock.NOT_LOCKED);
            return true;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    public void commitClusterState(ClusterState newState, Address initiator, String txnId) {
        Preconditions.checkNotNull(newState);
        if (newState == ClusterState.IN_TRANSITION) {
            throw new IllegalArgumentException("IN_TRANSITION is an internal state!");
        }

        clusterServiceLock.lock();
        try {
            final ClusterStateLock stateLock = getStateLock();
            if (!stateLock.allowsUnlock(txnId)) {
                throw new TransactionException(
                        "Cluster state change [" + state + " -> " + newState + "] failed for "
                                + initiator + ", current state: " + stateToString());
            }

            this.state = newState;
            stateLockRef.set(ClusterStateLock.NOT_LOCKED);
            changeNodeState(newState);
            node.getNodeExtension().onClusterStateChange(newState, true);

            if (newState == ClusterState.ACTIVE) {
                node.getClusterService().removeMembersDeadWhileClusterIsNotActive();
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void changeNodeState(ClusterState newState) {
        if (newState == ClusterState.PASSIVE) {
            node.changeNodeStateToPassive();
        } else {
            node.changeNodeStateToActive();
        }
    }

    void changeClusterState(ClusterState newState, Collection<Member> members, int partitionStateVersion) {
        changeClusterState(newState, members, DEFAULT_TX_OPTIONS, partitionStateVersion);
    }

    void changeClusterState(ClusterState newState, Collection<Member> members,
            TransactionOptions options, int partitionStateVersion) {
        checkParameters(newState, options);
        if (getState() == newState) {
            return;
        }

        NodeEngineImpl nodeEngine = node.getNodeEngine();
        TransactionManagerServiceImpl txManagerService
                = (TransactionManagerServiceImpl) nodeEngine.getTransactionManagerService();
        Transaction tx = txManagerService.newAllowedDuringPassiveStateTransaction(options);
        tx.begin();

        try {
            String txnId = tx.getTxnId();

            addTransactionRecords(newState, tx, members, partitionStateVersion);

            lockClusterState(newState, nodeEngine, options.getTimeoutMillis(), txnId, members, partitionStateVersion);

            checkMemberListChange(members);

            tx.prepare();
            tx.commit();

        } catch (Throwable e) {
            tx.rollback();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void lockClusterState(ClusterState newState, NodeEngineImpl nodeEngine, long leaseTime, String txnId,
            Collection<Member> members, int partitionStateVersion) {

        Collection<Future> futures = new ArrayList<Future>(members.size());

        final Address thisAddress = node.getThisAddress();
        for (Member member : members) {
            Operation op = new LockClusterStateOperation(newState, thisAddress, txnId, leaseTime, partitionStateVersion);
            Future future = nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, op, member.getAddress());
            futures.add(future);
        }

        StateManagerExceptionHandler exceptionHandler = new StateManagerExceptionHandler(logger);
        waitWithDeadline(futures, leaseTime, TimeUnit.MILLISECONDS, exceptionHandler);
        exceptionHandler.rethrowIfFailed();
    }

    private void addTransactionRecords(ClusterState newState, Transaction tx,
                                       Collection<Member> members, int partitionStateVersion) {
        long leaseTime = Math.min(tx.getTimeoutMillis(), LOCK_LEASE_EXTENSION_MILLIS);
        for (Member member : members) {
            tx.add(new ClusterStateTransactionLogRecord(newState, node.getThisAddress(),
                    member.getAddress(), tx.getTxnId(), leaseTime, partitionStateVersion));
        }
    }

    private void checkMemberListChange(Collection<Member> members) {
        Collection<Member> currentMembers = node.getClusterService().getMembers();
        if (members.size() != currentMembers.size()) {
            throw new IllegalStateException("Cluster members changed during state change!");
        }

        for (Member member : currentMembers) {
            if (!members.contains(member)) {
                throw new IllegalStateException("Cluster members changed during state change!");
            }
        }
    }

    private void checkParameters(ClusterState newState, TransactionOptions options) {
        Preconditions.checkNotNull(newState);
        Preconditions.checkNotNull(options);
        if (newState == ClusterState.IN_TRANSITION) {
            throw new IllegalArgumentException("IN_TRANSITION is an internal state!");
        }
        if (options.getTransactionType() != TransactionType.TWO_PHASE) {
            throw new IllegalArgumentException("Changing cluster state requires 2PC transaction!");
        }
    }

    String stateToString() {
        return "ClusterState{state=" + state + ", lock=" + stateLockRef.get() + '}';
    }

    @Override
    public String toString() {
        return "ClusterStateManager{stateLockRef=" + stateLockRef + ", state=" + state + '}';
    }

    private static final class StateManagerExceptionHandler implements FutureUtil.ExceptionHandler {
        private final ILogger logger;
        // written and read by same/single thread
        private Throwable error;

        private StateManagerExceptionHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void handleException(final Throwable throwable) {
            Throwable cause = throwable;
            if (throwable instanceof ExecutionException
                    && throwable.getCause() != null) {
                cause = throwable.getCause();
            }
            if (error == null) {
                error = cause;
            }
            log(cause);
        }

        private void log(Throwable cause) {
            if (logger.isFinestEnabled()) {
                logger.finest(cause);
            }
        }

        void rethrowIfFailed() {
            if (error != null) {
                throw ExceptionUtil.rethrow(error);
            }
        }
    }
}
