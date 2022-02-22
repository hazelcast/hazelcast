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

package com.hazelcast.transaction.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.impl.operations.BroadcastTxRollbackOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TRANSACTIONS_METRIC_COMMIT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TRANSACTIONS_METRIC_ROLLBACK_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TRANSACTIONS_METRIC_START_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TRANSACTIONS_PREFIX;
import static com.hazelcast.internal.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.internal.util.FutureUtil.logAllExceptions;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.transaction.impl.Transaction.State;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static java.util.Collections.shuffle;

public class TransactionManagerServiceImpl implements TransactionManagerService, ManagedService,
        MembershipAwareService, ClientAwareService {

    public static final String SERVICE_NAME = "hz:core:txManagerService";

    private static final Address[] EMPTY_ADDRESSES = new Address[0];

    final ConcurrentMap<UUID, TxBackupLog> txBackupLogs = new ConcurrentHashMap<>();

    // Due to mocking; the probes can't be made final.
    @Probe(name = TRANSACTIONS_METRIC_START_COUNT, level = ProbeLevel.MANDATORY)
    Counter startCount = MwCounter.newMwCounter();
    @Probe(name = TRANSACTIONS_METRIC_ROLLBACK_COUNT, level = ProbeLevel.MANDATORY)
    Counter rollbackCount = MwCounter.newMwCounter();
    @Probe(name = TRANSACTIONS_METRIC_COMMIT_COUNT, level = ProbeLevel.MANDATORY)
    Counter commitCount = MwCounter.newMwCounter();

    private final ExceptionHandler finalizeExceptionHandler;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    public TransactionManagerServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(TransactionManagerService.class);
        this.finalizeExceptionHandler = logAllExceptions(logger, "Error while rolling-back tx!", Level.WARNING);

        nodeEngine.getMetricsRegistry().registerStaticMetrics(this, TRANSACTIONS_PREFIX);
    }

    public String getClusterName() {
        return nodeEngine.getConfig().getClusterName();
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        checkNotNull(options, "TransactionOptions must not be null!");
        checkNotNull(task, "TransactionalTask is required!");

        TransactionContext context = newTransactionContext(options);
        context.beginTransaction();
        try {
            T value = task.execute(context);
            context.commitTransaction();
            return value;
        } catch (Throwable e) {
            context.rollbackTransaction();
            if (e instanceof TransactionException) {
                throw (TransactionException) e;
            }
            if (e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new TransactionException(e);
        }
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return new TransactionContextImpl(this, nodeEngine, options, null, false);
    }

    @Override
    public TransactionContext newClientTransactionContext(@Nonnull TransactionOptions options,
                                                          @Nullable UUID clientUuid) {
        return new TransactionContextImpl(this, nodeEngine, options, clientUuid, true);
    }

    /**
     * Creates a plain transaction object which can be used
     * while cluster state is {@link ClusterState#PASSIVE},
     * without wrapping it inside a TransactionContext.
     * <p>
     * A Transaction is a lower level API than TransactionContext.
     * It's not possible to create/access transactional
     * data structures without TransactionContext.
     * <p>
     * A Transaction object only allows starting/committing/rolling
     * back transaction, accessing state of the transaction
     * and adding TransactionLogRecord to the transaction.
     *
     * @param options transaction options
     * @return a new transaction which can be used while
     * cluster state is {@link ClusterState#PASSIVE}
     */
    public Transaction newAllowedDuringPassiveStateTransaction(TransactionOptions options) {
        return new AllowedDuringPassiveStateTransactionImpl(this, nodeEngine, options, null);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        txBackupLogs.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        MemberImpl member = event.getMember();
        final UUID uuid = member.getUuid();
        if (nodeEngine.isRunning()) {
            logger.info("Committing/rolling-back live transactions of " + member.getAddress() + ", UUID: " + uuid);
            nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, () -> finalizeTransactionsOf(uuid));
        } else if (logger.isFinestEnabled()) {
            logger.finest("Will not commit/roll-back transactions of " + member.getAddress() + ", UUID: " + uuid
                    + " because this member is not running");
        }
    }

    private void finalizeTransactionsOf(UUID callerUuid) {
        final Iterator<Map.Entry<UUID, TxBackupLog>> it = txBackupLogs.entrySet().iterator();

        while (it.hasNext()) {
            final Map.Entry<UUID, TxBackupLog> entry = it.next();
            final UUID txnId = entry.getKey();
            final TxBackupLog log = entry.getValue();
            if (finalizeTransaction(callerUuid, txnId, log)) {
                it.remove();
            }
        }
    }

    private boolean finalizeTransaction(UUID uuid, UUID txnId, TxBackupLog log) {
        OperationService operationService = nodeEngine.getOperationService();
        if (!uuid.equals(log.callerUuid)) {
            return false;
        }

        if (log.state == ACTIVE) {
            if (logger.isFinestEnabled()) {
                logger.finest("Rolling-back transaction[id:" + txnId + ", state:ACTIVE] of endpoint " + uuid);
            }
            Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
            Collection<Future> futures = new ArrayList<>(memberList.size());
            for (Member member : memberList) {
                Operation op = new BroadcastTxRollbackOperation(txnId);
                Future f = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                futures.add(f);
            }

            long timeoutMillis = TransactionOptions.getDefault().getTimeoutMillis();
            waitWithDeadline(futures, timeoutMillis, TimeUnit.MILLISECONDS, finalizeExceptionHandler);
        } else {
            TransactionImpl tx;
            if (log.allowedDuringPassiveState) {
                tx = new AllowedDuringPassiveStateTransactionImpl(this, nodeEngine, txnId, log.records,
                        log.timeoutMillis, log.startTime, log.callerUuid);
            } else {
                tx = new TransactionImpl(this, nodeEngine, txnId, log.records,
                        log.timeoutMillis, log.startTime, log.callerUuid);
            }

            if (log.state == COMMITTING) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Committing transaction[id:" + txnId + ", state:COMMITTING] of endpoint " + uuid);
                }
                try {
                    tx.commit();
                } catch (Throwable e) {
                    logger.warning("Error during committing from tx backup!", e);
                }
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Rolling-back transaction[id:" + txnId + ", state:" + log.state
                            + "] of endpoint " + uuid);
                }
                try {
                    tx.rollback();
                } catch (Throwable e) {
                    logger.warning("Error during rolling-back from tx backup!", e);
                }
            }
        }

        return true;
    }

    @Override
    public void clientDisconnected(UUID clientUuid) {
        logger.info("Committing/rolling-back live transactions of client, UUID: " + clientUuid);
        finalizeTransactionsOf(clientUuid);
    }

    Address[] pickBackupLogAddresses(int durability) {
        if (durability == 0) {
            return EMPTY_ADDRESSES;
        }

        // This should be cleaned up because this is quite a complex approach since it depends on
        // the number of members in the cluster and creates litter.

        ClusterService clusterService = nodeEngine.getClusterService();
        List<MemberImpl> members = new ArrayList<>(clusterService.getMemberImpls());
        members.remove(nodeEngine.getLocalMember());
        int c = Math.min(members.size(), durability);
        shuffle(members);
        Address[] addresses = new Address[c];
        for (int i = 0; i < c; i++) {
            addresses[i] = members.get(i).getAddress();
        }
        return addresses;
    }

    public void createBackupLog(UUID callerUuid, UUID txnId) {
        createBackupLog(callerUuid, txnId, false);
    }

    public void createAllowedDuringPassiveStateBackupLog(UUID callerUuid, UUID txnId) {
        createBackupLog(callerUuid, txnId, true);
    }

    private void createBackupLog(UUID callerUuid, UUID txnId, boolean allowedDuringPassiveState) {
        TxBackupLog log = new TxBackupLog(Collections.emptyList(), callerUuid,
                ACTIVE, -1, -1, allowedDuringPassiveState);
        if (txBackupLogs.putIfAbsent(txnId, log) != null) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    public void replicaBackupLog(List<TransactionLogRecord> records, UUID callerUuid, UUID txnId,
                                 long timeoutMillis, long startTime) {
        TxBackupLog beginLog = txBackupLogs.get(txnId);
        if (beginLog == null) {
            throw new TransactionException("Could not find begin tx log!");
        }
        if (beginLog.state != ACTIVE) {
            // the exception message is very strange
            throw new TransactionException("TxLog already exists!");
        }
        TxBackupLog newTxBackupLog = new TxBackupLog(records, callerUuid, COMMITTING, timeoutMillis, startTime,
                beginLog.allowedDuringPassiveState);
        if (!txBackupLogs.replace(txnId, beginLog, newTxBackupLog)) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    public void rollbackBackupLog(UUID txnId) {
        TxBackupLog log = txBackupLogs.get(txnId);
        if (log == null) {
            logger.warning("No tx backup log is found, tx -> " + txnId);
        } else {
            log.state = ROLLING_BACK;
        }
    }

    public void purgeBackupLog(UUID txnId) {
        txBackupLogs.remove(txnId);
    }

    static final class TxBackupLog {
        final List<TransactionLogRecord> records;
        final UUID callerUuid;
        final long timeoutMillis;
        final long startTime;
        final boolean allowedDuringPassiveState;
        volatile State state;

        private TxBackupLog(List<TransactionLogRecord> records, UUID callerUuid, State state,
                            long timeoutMillis, long startTime, boolean allowedDuringPassiveState) {
            this.records = records;
            this.callerUuid = callerUuid;
            this.state = state;
            this.timeoutMillis = timeoutMillis;
            this.startTime = startTime;
            this.allowedDuringPassiveState = allowedDuringPassiveState;
        }

        @Override
        public String toString() {
            return "TxBackupLog{"
                    + "records=" + records
                    + ", callerUuid='" + callerUuid + '\''
                    + ", timeoutMillis=" + timeoutMillis
                    + ", startTime=" + startTime
                    + ", state=" + state
                    + ", allowedDuringPassiveState=" + allowedDuringPassiveState
                    + '}';
        }
    }
}
