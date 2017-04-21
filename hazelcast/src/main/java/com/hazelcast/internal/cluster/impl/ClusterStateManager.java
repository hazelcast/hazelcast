/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOp;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.LockGuard;
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

    // this is the version at which the cluster operates. see Cluster#getClusterVersion
    volatile Version clusterVersion = Version.UNKNOWN;

    private final Node node;
    private final ILogger logger;
    private final Lock clusterServiceLock;
    private final AtomicReference<LockGuard> stateLockRef = new AtomicReference<LockGuard>(LockGuard.NOT_LOCKED);

    private volatile ClusterState state = ClusterState.ACTIVE;

    ClusterStateManager(Node node, Lock clusterServiceLock) {
        this.node = node;
        this.clusterServiceLock = clusterServiceLock;
        this.logger = node.getLogger(getClass());
    }

    public ClusterState getState() {
        LockGuard stateLock = getStateLock();
        return stateLock.isLocked() ? ClusterState.IN_TRANSITION : state;
    }

    public Version getClusterVersion() {
        // if version is locked we still operate using the "old" version, so we return this one
        return clusterVersion;
    }

    LockGuard getStateLock() {
        LockGuard stateLock = stateLockRef.get();
        while (stateLock.isLeaseExpired()) {
            if (stateLockRef.compareAndSet(stateLock, LockGuard.NOT_LOCKED)) {
                stateLock = LockGuard.NOT_LOCKED;
                break;
            }
            stateLock = stateLockRef.get();
        }
        return stateLock;
    }

    void initialClusterState(ClusterState initialState, Version version) {
        clusterServiceLock.lock();
        try {
            final ClusterState currentState = getState();
            if (currentState != ClusterState.ACTIVE && currentState != initialState) {
                logger.warning("Initial state is already set! " + "Current state: " + currentState + ", Given state: "
                        + initialState);
                return;
            }
            // no need to validate again
            logger.fine("Setting initial cluster state: " + initialState + " and version: " + version);
            setClusterStateAndVersion(initialState, version, true);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void setClusterState(ClusterState newState, boolean isTransient) {
        clusterServiceLock.lock();
        try {
            doSetClusterState(newState, isTransient);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    public void setClusterVersion(Version newVersion) {
        clusterServiceLock.lock();
        try {
            doSetClusterVersion(newVersion);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void setClusterStateAndVersion(ClusterState newState, Version newVersion, boolean isTransient) {
        this.state = newState;
        this.clusterVersion = newVersion;
        stateLockRef.set(LockGuard.NOT_LOCKED);
        changeNodeState(newState);
        node.getNodeExtension().onClusterStateChange(newState, isTransient);
        node.getNodeExtension().onClusterVersionChange(newVersion);
    }

    private void doSetClusterState(ClusterState newState, boolean isTransient) {
        this.state = newState;
        stateLockRef.set(LockGuard.NOT_LOCKED);
        changeNodeState(newState);
        node.getNodeExtension().onClusterStateChange(newState, isTransient);
    }

    private void doSetClusterVersion(Version newVersion) {
        this.clusterVersion = newVersion;
        stateLockRef.set(LockGuard.NOT_LOCKED);
        node.getNodeExtension().onClusterVersionChange(newVersion);
    }

    void reset() {
        clusterServiceLock.lock();
        try {
            state = ClusterState.ACTIVE;
            // not notifying cluster version listeners about change to UNKNOWN. consider for example the following scenario:
            // - node starts with codebase version 3.9, overrides init cluster version via group property to 3.8
            // - node joins an existing 3.8 cluster which is undergoing rolling-upgrade to 3.9
            // - once all cluster members are on 3.9, cluster version is upgraded to 3.9.0
            // - clusterVersion is reset to UNKNOWN
            // - if cluster version listener is notified of null cluster version, it should receive the overridden one (3.8)
            // - if 3.8 discovery & join messages are incompatible, node will not be able to join 3.9 cluster
            // Instead, not notifying cluster version listeners will let the node use its last set cluster version for discovery &
            // join messages and join the cluster.
            clusterVersion = Version.UNKNOWN;
            stateLockRef.set(LockGuard.NOT_LOCKED);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /**
     * Validate cluster state change requested and set a {@code ClusterStateLock}.
     * @param stateChange
     * @param initiator
     * @param txnId
     * @param leaseTime
     * @param partitionStateVersion
     */
    public void lockClusterState(ClusterStateChange stateChange, Address initiator, String txnId,
                                 long leaseTime, int partitionStateVersion) {
        Preconditions.checkNotNull(stateChange);
        clusterServiceLock.lock();
        try {
            if (!node.getNodeExtension().isStartCompleted()) {
                throw new IllegalStateException("Can not lock cluster state! Startup is not completed yet!");
            }

            if (node.getClusterService().getClusterJoinManager().isMastershipClaimInProgress()) {
                throw new IllegalStateException("Can not lock cluster state! Mastership claim is in progress!");
            }

            if (stateChange.isOfType(Version.class)) {
                validateNodeCompatibleWith((Version) stateChange.getNewState());
                validateClusterVersionChange((Version) stateChange.getNewState());
            }

            checkMigrationsAndPartitionStateVersion(stateChange, partitionStateVersion);

            final LockGuard currentLock = getStateLock();
            if (!currentLock.allowsLock(txnId)) {
                throw new TransactionException("Locking failed for " + initiator + ", tx: " + txnId
                        + ", current state: " + toString());
            }

            stateLockRef.set(new LockGuard(initiator, txnId, leaseTime));

            try {
                // check migration status and partition-state version again
                // if partition state is changed then release the lock and fail.
                checkMigrationsAndPartitionStateVersion(stateChange, partitionStateVersion);
            } catch (IllegalStateException e) {
                stateLockRef.set(LockGuard.NOT_LOCKED);
                throw e;
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    // check if current node is compatible with requested cluster version
    // wraps NodeExtension#isNodeVersionCompatibleWith(Version) and throws a VersionMismatchException if incompatibility is found.
    private void validateNodeCompatibleWith(Version clusterVersion) {
        if (!node.getNodeExtension().isNodeVersionCompatibleWith(clusterVersion)) {
            throw new VersionMismatchException("Node's codebase version " + node.getVersion() + " is incompatible with "
                    + "the requested cluster version " + clusterVersion);
        }
    }

    // validate transition from current to newClusterVersion is allowed
    private void validateClusterVersionChange(Version newClusterVersion) {
        if (!clusterVersion.isUnknown() && clusterVersion.getMajor() != newClusterVersion.getMajor()) {
            throw new IllegalArgumentException("Transition to requested version " + newClusterVersion
                    + " not allowed for current cluster version " + clusterVersion);
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
            final LockGuard currentLock = getStateLock();
            if (!currentLock.allowsUnlock(txnId)) {
                return false;
            }

            stateLockRef.set(LockGuard.NOT_LOCKED);

            // if state allows join after rollback, then remove all members which left during transaction.
            if (state.isJoinAllowed()) {
                node.getClusterService().getMembershipManager().removeMembersDeadInNotJoinableState();
            }
            return true;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    // for tests only
    void commitClusterState(ClusterStateChange newState, Address initiator, String txnId) {
        commitClusterState(newState, initiator, txnId, false);
    }

    public void commitClusterState(ClusterStateChange stateChange, Address initiator, String txnId, boolean isTransient) {
        Preconditions.checkNotNull(stateChange);
        stateChange.validate();

        clusterServiceLock.lock();
        try {
            final LockGuard stateLock = getStateLock();
            if (!stateLock.allowsUnlock(txnId)) {
                throw new TransactionException(
                        "Cluster state change [" + state + " -> " + stateChange + "] failed for "
                                + initiator + ", current state: " + stateToString());
            }

            if (stateChange.isOfType(ClusterState.class)) {
                ClusterState newState = (ClusterState) stateChange.getNewState();
                doSetClusterState(newState, isTransient);

                // if state is changed to allow joins, then remove all members which left while not active.
                if (newState.isJoinAllowed()) {
                    node.getClusterService().getMembershipManager().removeMembersDeadInNotJoinableState();
                }
            } else if (stateChange.isOfType(Version.class)) {
                // version is validated on cluster-state-lock, thus we can commit without checking compatibility
                doSetClusterVersion((Version) stateChange.getNewState());
            } else {
                throw new IllegalArgumentException("Illegal ClusterStateChange of type " + stateChange.getType() + ".");
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

    void changeClusterState(ClusterStateChange newState, MemberMap memberMap, int partitionStateVersion,
                            boolean isTransient) {
        changeClusterState(newState, memberMap, DEFAULT_TX_OPTIONS, partitionStateVersion, isTransient);
    }

    void changeClusterState(ClusterStateChange newState, MemberMap memberMap,
            TransactionOptions options, int partitionStateVersion, boolean isTransient) {
        checkParameters(newState, options);
        if (isCurrentStateEqualToRequestedOne(newState)) {
            return;
        }

        NodeEngineImpl nodeEngine = node.getNodeEngine();
        TransactionManagerServiceImpl txManagerService
                = (TransactionManagerServiceImpl) nodeEngine.getTransactionManagerService();
        Transaction tx = txManagerService.newAllowedDuringPassiveStateTransaction(options);
        tx.begin();

        try {
            String txnId = tx.getTxnId();
            Collection<MemberImpl> members = memberMap.getMembers();

            addTransactionRecords(newState, tx, members, partitionStateVersion, isTransient);

            lockClusterStateOnAllMembers(newState, nodeEngine, options.getTimeoutMillis(), txnId, members, partitionStateVersion);

            checkMemberListChange(memberMap.getVersion());

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

    private boolean isCurrentStateEqualToRequestedOne(ClusterStateChange change) {
        if (change.isOfType(ClusterState.class)) {
            return getState() == change.getNewState();
        } else if (change.isOfType(Version.class)) {
            return clusterVersion != null && clusterVersion.equals(change.getNewState());
        }
        return false;
    }

    private void lockClusterStateOnAllMembers(ClusterStateChange stateChange, NodeEngineImpl nodeEngine, long leaseTime,
                                              String txnId, Collection<MemberImpl> members, int partitionStateVersion) {

        Collection<Future> futures = new ArrayList<Future>(members.size());

        final Address thisAddress = node.getThisAddress();
        for (Member member : members) {
            Operation op = new LockClusterStateOp(stateChange, thisAddress, txnId, leaseTime, partitionStateVersion);
            Future future = nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, op, member.getAddress());
            futures.add(future);
        }

        StateManagerExceptionHandler exceptionHandler = new StateManagerExceptionHandler(logger);
        waitWithDeadline(futures, leaseTime, TimeUnit.MILLISECONDS, exceptionHandler);
        exceptionHandler.rethrowIfFailed();
    }

    private void addTransactionRecords(ClusterStateChange stateChange, Transaction tx,
                                       Collection<MemberImpl> members, int partitionStateVersion, boolean isTransient) {
        long leaseTime = Math.min(tx.getTimeoutMillis(), LOCK_LEASE_EXTENSION_MILLIS);
        for (Member member : members) {
            tx.add(new ClusterStateTransactionLogRecord(stateChange, node.getThisAddress(),
                    member.getAddress(), tx.getTxnId(), leaseTime, partitionStateVersion, isTransient));
        }
    }

    private void checkMemberListChange(int initialMemberListVersion) {
        int currentMemberListVersion = node.getClusterService().getMembershipManager().getMemberListVersion();

        if (initialMemberListVersion != currentMemberListVersion) {
            throw new IllegalStateException("Cluster members changed during state change! "
                    + "Initial version: " + initialMemberListVersion + ", Current version: " + currentMemberListVersion);
        }
    }

    private void checkParameters(ClusterStateChange newState, TransactionOptions options) {
        Preconditions.checkNotNull(newState);
        Preconditions.checkNotNull(options);

        newState.validate();
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
                logger.fine("failure during cluster state change", cause);
            }
        }

        void rethrowIfFailed() {
            if (error != null) {
                throw ExceptionUtil.rethrow(error);
            }
        }
    }
}
