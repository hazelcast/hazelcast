/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOperation;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.Preconditions;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SERVICE_NAME;
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
    private volatile Version version;

    ClusterStateManager(Node node, Lock clusterServiceLock) {
        this.node = node;
        this.clusterServiceLock = clusterServiceLock;
        this.logger = node.getLogger(getClass());
        this.version = node.getVersion();
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
            version = node.getVersion();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    public void lockClusterState(ClusterStateChange stateChange, Address initiator, String txnId,
                                 long leaseTime, int partitionStateVersion) {
        Preconditions.checkNotNull(stateChange);
        clusterServiceLock.lock();
        try {
            if (!node.getNodeExtension().isStartCompleted()) {
                throw new IllegalStateException("Can not lock cluster state! Startup is not completed yet!");
            }

            checkMigrationsAndPartitionStateVersion(stateChange, partitionStateVersion);

            final ClusterStateLock currentLock = getStateLock();
            if (!currentLock.allowsLock(txnId)) {
                throw new TransactionException("Locking failed for " + initiator + ", tx: " + txnId
                        + ", current state: " + toString());
            }

            stateLockRef.set(new ClusterStateLock(initiator, txnId, leaseTime));

            try {
                // check migration status and partition-state version again
                // if partition state is changed then release the lock and fail.
                checkMigrationsAndPartitionStateVersion(stateChange, partitionStateVersion);
            } catch (IllegalStateException e) {
                stateLockRef.set(ClusterStateLock.NOT_LOCKED);
                throw e;
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void checkMigrationsAndPartitionStateVersion(ClusterStateChange stateChange, int partitionStateVersion) {
        final InternalPartitionService partitionService = node.getPartitionService();
        final int thisPartitionStateVersion = partitionService.getPartitionStateVersion();

        if (partitionService.hasOnGoingMigrationLocal()) {
            throw new IllegalStateException("Still have pending migration tasks, "
                    + "cannot lock cluster state! New state: " + stateChange
                    + ", current state: " + getState());
        } else if (partitionStateVersion != thisPartitionStateVersion) {
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

            // if state remains ACTIVE after rollback, then remove all members which left during transaction.
            if (state == ClusterState.ACTIVE) {
                node.getClusterService().removeMembersDeadWhileClusterIsNotActive();
            }
            return true;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    public void commitClusterState(ClusterStateChange stateChange, Address initiator, String txnId) {
        Preconditions.checkNotNull(stateChange);
        stateChange.validate();

        clusterServiceLock.lock();
        try {
            final ClusterStateLock stateLock = getStateLock();
            if (!stateLock.allowsUnlock(txnId)) {
                throw new TransactionException(
                        "Cluster state change [" + state + " -> " + stateChange + "] failed for "
                                + initiator + ", current state: " + stateToString());
            }

            doChangeState(stateChange);

        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void doChangeState(ClusterStateChange stateChange) {
        if (stateChange.isOfType(ClusterState.class)) {
            ClusterState newState = (ClusterState) stateChange.getNewState();
            this.state = newState;
            stateLockRef.set(ClusterStateLock.NOT_LOCKED);
            changeNodeState(newState);
            node.getNodeExtension().onClusterStateChange(newState, true);

            // if state is changed to ACTIVE, then remove all members which left while not active.
            if (newState == ClusterState.ACTIVE) {
                node.getClusterService().removeMembersDeadWhileClusterIsNotActive();
            }
        } else if (stateChange.isOfType(ClusterVersionService.class)) {
            this.version = (Version) stateChange.getNewState();
        }
        throw new IllegalArgumentException("Illegal cluster state change " + stateChange);
    }

    private void changeNodeState(ClusterState newState) {
        if (newState == ClusterState.PASSIVE) {
            node.changeNodeStateToPassive();
        } else {
            node.changeNodeStateToActive();
        }
    }

    void changeClusterState(ClusterStateChange newState, Collection<Member> members, int partitionStateVersion) {
        changeClusterState(newState, members, DEFAULT_TX_OPTIONS, partitionStateVersion);
    }

    void changeClusterState(ClusterStateChange stateChange, Collection<Member> members,
                            TransactionOptions options, int partitionStateVersion) {
        checkParameters(stateChange, options);
        if (isCurrentStateEqualToRequestedOne(stateChange)) {
            return;
        }

        NodeEngineImpl nodeEngine = node.getNodeEngine();
        TransactionManagerServiceImpl txManagerService
                = (TransactionManagerServiceImpl) nodeEngine.getTransactionManagerService();
        Transaction tx = txManagerService.newAllowedDuringPassiveStateTransaction(options);
        tx.begin();

        try {
            String txnId = tx.getTxnId();

            addTransactionRecords(stateChange, tx, members, partitionStateVersion);

            lockClusterState(stateChange, nodeEngine, options.getTimeoutMillis(), txnId, members, partitionStateVersion);

            checkMemberListChange(members);

            tx.prepare();

        } catch (Throwable e) {
            tx.rollback();
            throw ExceptionUtil.rethrow(e);
        }

        try {
            tx.commit();
        } catch (Throwable e) {
            if (e instanceof TargetNotMemberException || e.getCause() instanceof MemberLeftException) {
                // Member left while tx is being committed after prepare successful.
                // We cannot rollback tx after this point. Cluster state change is done
                // on other members.
                // Left members will be added to the members-removed-in-not-active-state-list
                // if new state is passive or frozen. They will be able to rejoin later.
                return;
            }
            throw ExceptionUtil.rethrow(e);
        }
    }

    boolean isCurrentStateEqualToRequestedOne(ClusterStateChange change) {
        if (change.isOfType(ClusterState.class)) {
            return state == change.getNewState();
        } else if (change.isOfType(Version.class)) {
            return version.equals(change.getNewState());
        }
        return false;
    }

    private void lockClusterState(ClusterStateChange stateChange, NodeEngineImpl nodeEngine, long leaseTime, String txnId,
                                  Collection<Member> members, int partitionStateVersion) {

        Collection<Future> futures = new ArrayList<Future>(members.size());

        final Address thisAddress = node.getThisAddress();
        for (Member member : members) {
            Operation op = new LockClusterStateOperation(stateChange, thisAddress, txnId, leaseTime, partitionStateVersion);
            Future future = nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, op, member.getAddress());
            futures.add(future);
        }

        StateManagerExceptionHandler exceptionHandler = new StateManagerExceptionHandler(logger);
        waitWithDeadline(futures, leaseTime, TimeUnit.MILLISECONDS, exceptionHandler);
        exceptionHandler.rethrowIfFailed();
    }

    private void addTransactionRecords(ClusterStateChange stateChange, Transaction tx,
                                       Collection<Member> members, int partitionStateVersion) {
        long leaseTime = Math.min(tx.getTimeoutMillis(), LOCK_LEASE_EXTENSION_MILLIS);
        for (Member member : members) {
            tx.add(new ClusterStateTransactionLogRecord(stateChange, node.getThisAddress(),
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

    private void checkParameters(ClusterStateChange stateChange, TransactionOptions options) {
        Preconditions.checkNotNull(stateChange);
        Preconditions.checkNotNull(options);
        stateChange.validate();

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
            if (logger.isFineEnabled()) {
                logger.log(Level.FINE, "failure during cluster state change", cause);
            }
        }

        void rethrowIfFailed() {
            if (error != null) {
                throw ExceptionUtil.rethrow(error);
            }
        }
    }

}
