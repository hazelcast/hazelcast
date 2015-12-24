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

package com.hazelcast.transaction.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.impl.operations.BroadcastTxRollbackOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.transaction.impl.Transaction.State;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.shuffle;

public class TransactionManagerServiceImpl implements TransactionManagerService, ManagedService,
        MembershipAwareService, ClientAwareService {

    public static final String SERVICE_NAME = "hz:core:txManagerService";

    private static final Address[] EMPTY_ADDRESSES = new Address[0];

    final ConcurrentMap<String, TxBackupLog> txBackupLogs = new ConcurrentHashMap<String, TxBackupLog>();

    private final ExceptionHandler finalizeExceptionHandler;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    public TransactionManagerServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(TransactionManagerService.class);
        this.finalizeExceptionHandler = logAllExceptions(logger, "Error while rolling-back tx!", Level.WARNING);
    }

    public String getGroupName() {
        return nodeEngine.getConfig().getGroupConfig().getName();
    }

    @Override
    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        checkNotNull(task, "TransactionalTask is required!");

        final TransactionContextImpl context = new TransactionContextImpl(this, nodeEngine, options, null);
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
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextImpl(this, nodeEngine, options, null);
    }

    @Override
    public TransactionContext newClientTransactionContext(TransactionOptions options, String clientUuid) {
        return new TransactionContextImpl(this, nodeEngine, options, clientUuid);
    }

    /**
     * Creates a plain transaction object, without wrapping it
     * inside a TransactionContext.
     * <p/>
     * A Transaction is a lower level API than TransactionContext.
     * It's not possible to create/access transactional
     * data structures without TransactionContext.
     * <p/>
     * A Transaction object
     * only allows starting/committing/rolling back transaction,
     * accessing state of the transaction
     * and adding TransactionLogRecord to the transaction.
     *
     * @param options transaction options
     * @return a new transaction
     */
    public Transaction newTransaction(TransactionOptions options) {
        return new TransactionImpl(this, nodeEngine, options, null);
    }

    /**
     * Creates a plain transaction object which can be used while cluster state is {@link ClusterState#PASSIVE},
     * without wrapping it inside a TransactionContext.
     * <p/>
     * Also see {@link TransactionManagerServiceImpl#newTransaction(TransactionOptions)} for more details
     *
     * @param options transaction options
     * @return a new transaction which can be used while cluster state is {@link ClusterState#PASSIVE}
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
        final String uuid = member.getUuid();
        if (nodeEngine.isRunning()) {
            logger.info("Committing/rolling-back alive transactions of " + member + ", UUID: " + uuid);
            nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                @Override
                public void run() {
                    finalizeTransactionsOf(uuid);
                }
            });
        } else if (logger.isFinestEnabled()) {
            logger.finest("Will not commit/roll-back transactions of " + member + ", UUID: " + uuid
                    + " because this member is not running");
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    private void finalizeTransactionsOf(String callerUuid) {
        final Iterator<Map.Entry<String, TxBackupLog>> it = txBackupLogs.entrySet().iterator();

        while (it.hasNext()) {
            final Map.Entry<String, TxBackupLog> entry = it.next();
            final String txnId = entry.getKey();
            final TxBackupLog log = entry.getValue();
            if (finalize(callerUuid, txnId, log)) {
                it.remove();
            }
        }
    }

    private boolean finalize(String uuid, String txnId, TxBackupLog log) {
        OperationService operationService = nodeEngine.getOperationService();
        if (!uuid.equals(log.callerUuid)) {
            return false;
        }

        if (log.state == ACTIVE) {
            if (logger.isFinestEnabled()) {
                logger.finest("Rolling-back transaction[id:" + txnId + ", state:ACTIVE] of endpoint " + uuid);
            }
            Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
            Collection<Future> futures = new ArrayList<Future>(memberList.size());
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
    public void clientDisconnected(String clientUuid) {
        logger.info("Committing/rolling-back alive transactions of client, UUID: " + clientUuid);
        finalizeTransactionsOf(clientUuid);
    }

    Address[] pickBackupLogAddresses(int durability) {
        if (durability == 0) {
            return EMPTY_ADDRESSES;
        }

        // This should be cleaned up because this is quite a complex approach since it depends on
        // the number of members in the cluster and creates litter.

        ClusterService clusterService = nodeEngine.getClusterService();
        List<MemberImpl> members = new ArrayList<MemberImpl>(clusterService.getMemberImpls());
        members.remove(nodeEngine.getLocalMember());
        int c = Math.min(members.size(), durability);
        shuffle(members);
        Address[] addresses = new Address[c];
        for (int i = 0; i < c; i++) {
            addresses[i] = members.get(i).getAddress();
        }
        return addresses;
    }

    public void createBackupLog(String callerUuid, String txnId) {
        createBackupLog(callerUuid, txnId, false);
    }

    public void createAllowedDuringPassiveStateBackupLog(String callerUuid, String txnId) {
        createBackupLog(callerUuid, txnId, true);
    }

    private void createBackupLog(String callerUuid, String txnId, boolean allowedDuringPassiveState) {
        TxBackupLog log = new TxBackupLog(Collections.<TransactionLogRecord>emptyList(), callerUuid,
                ACTIVE, -1, -1, allowedDuringPassiveState);
        if (txBackupLogs.putIfAbsent(txnId, log) != null) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    public void replicaBackupLog(List<TransactionLogRecord> records, String callerUuid, String txnId,
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

    public void rollbackBackupLog(String txnId) {
        TxBackupLog log = txBackupLogs.get(txnId);
        if (log == null) {
            logger.warning("No tx backup log is found, tx -> " + txnId);
        } else {
            log.state = ROLLING_BACK;
        }
    }

    public void purgeBackupLog(String txnId) {
        txBackupLogs.remove(txnId);
    }

    static final class TxBackupLog {
        final List<TransactionLogRecord> records;
        final String callerUuid;
        final long timeoutMillis;
        final long startTime;
        volatile State state;
        final boolean allowedDuringPassiveState;

        private TxBackupLog(List<TransactionLogRecord> records, String callerUuid, State state,
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
