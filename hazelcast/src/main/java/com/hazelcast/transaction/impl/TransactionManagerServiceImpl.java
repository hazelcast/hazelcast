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
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ClientAwareService;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.transaction.impl.Transaction.State;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class TransactionManagerServiceImpl implements TransactionManagerService, ManagedService,
        MembershipAwareService, ClientAwareService {

    public static final String SERVICE_NAME = "hz:core:txManagerService";

    private final ExceptionHandler finalizeExceptionHandler;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final ConcurrentMap<String, TxBackupLog> txBackupLogs = new ConcurrentHashMap<String, TxBackupLog>();

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
            final T value = task.execute(context);
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
        final MemberImpl member = event.getMember();
        String uuid = member.getUuid();
        finalizeTransactionsOf(uuid);
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    private void finalizeTransactionsOf(String uuid) {
        for (Map.Entry<String, TxBackupLog> entry : txBackupLogs.entrySet()) {
            finalize(uuid, entry.getKey(), entry.getValue());
        }
    }

    private void finalize(String uuid, String txnId, TxBackupLog log) {
        OperationService operationService = nodeEngine.getOperationService();
        if (!uuid.equals(log.callerUuid)) {
            return;
        }

        //TODO shouldn't we remove TxBackupLog from map ?
        if (log.state == State.ACTIVE) {
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
            TransactionImpl tx = new TransactionImpl(this, nodeEngine, txnId, log.records,
                    log.timeoutMillis, log.startTime, log.callerUuid);
            if (log.state == State.COMMITTING) {
                try {
                    tx.commit();
                } catch (Throwable e) {
                    logger.warning("Error during committing from tx backup!", e);
                }
            } else {
                try {
                    tx.rollback();
                } catch (Throwable e) {
                    logger.warning("Error during rolling-back from tx backup!", e);
                }
            }
        }
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        finalizeTransactionsOf(clientUuid);
    }

    Address[] pickBackupAddresses(int durability) {
        final ClusterService clusterService = nodeEngine.getClusterService();
        final List<MemberImpl> members = new ArrayList<MemberImpl>(clusterService.getMemberImpls());
        members.remove(nodeEngine.getLocalMember());
        final int c = Math.min(members.size(), durability);
        Collections.shuffle(members);
        Address[] addresses = new Address[c];
        for (int i = 0; i < c; i++) {
            addresses[i] = members.get(i).getAddress();
        }
        return addresses;
    }

    public void beginTxBackupLog(String callerUuid, String txnId) {
        TxBackupLog log = new TxBackupLog(Collections.<TransactionLogRecord>emptyList(), callerUuid, State.ACTIVE, -1, -1);
        if (txBackupLogs.putIfAbsent(txnId, log) != null) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    public void prepareTxBackupLog(List<TransactionLogRecord> records, String callerUuid, String txnId,
                                   long timeoutMillis, long startTime) {
        TxBackupLog beginLog = txBackupLogs.get(txnId);
        if (beginLog == null) {
            throw new TransactionException("Could not find begin tx log!");
        }
        if (beginLog.state != State.ACTIVE) {
            throw new TransactionException("TxLog already exists!");
        }
        TxBackupLog newTxBackupLog = new TxBackupLog(records, callerUuid, State.COMMITTING, timeoutMillis, startTime);
        if (!txBackupLogs.replace(txnId, beginLog, newTxBackupLog)) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    public void rollbackTxBackupLog(String txnId) {
        final TxBackupLog log = txBackupLogs.get(txnId);
        if (log != null) {
            log.state = State.ROLLING_BACK;
        } else {
            logger.warning("No tx backup log is found, tx -> " + txnId);
        }
    }

    public void purgeTxBackupLog(String txnId) {
        txBackupLogs.remove(txnId);
    }

    private static final class TxBackupLog {
        private final List<TransactionLogRecord> records;
        private final String callerUuid;
        private final long timeoutMillis;
        private final long startTime;
        private volatile State state;

        private TxBackupLog(List<TransactionLogRecord> records, String callerUuid, State state,
                            long timeoutMillis, long startTime) {
            this.records = records;
            this.callerUuid = callerUuid;
            this.state = state;
            this.timeoutMillis = timeoutMillis;
            this.startTime = startTime;
        }

        @Override
        public String toString() {
            return "TxBackupLog{"
                    + "records=" + records
                    + ", callerUuid='" + callerUuid + '\''
                    + ", timeoutMillis=" + timeoutMillis
                    + ", startTime=" + startTime
                    + ", state=" + state
                    + '}';
        }
    }
}
