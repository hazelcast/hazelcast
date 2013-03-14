/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

/**
 * @mdogan 2/26/13
 */
public class TransactionManagerServiceImpl implements TransactionManagerService, ManagedService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:core:txManagerService";

    private static final int ONE_MIN_MS = 60 * 1000;

    private static final Object DUMMY_OBJECT = new Object();

    private final NodeEngineImpl nodeEngine;

    private final ConcurrentMap<TransactionKey, TransactionLog> txLogs = new ConcurrentHashMap<TransactionKey, TransactionLog>();

    private final EntryTaskScheduler<TransactionKey, Object> scheduler;

    private final ILogger logger;

    public TransactionManagerServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        final ScheduledExecutorService scheduledExecutor = nodeEngine.getExecutionService().getScheduledExecutor();
        this.scheduler = EntryTaskSchedulerFactory.newScheduler(scheduledExecutor, new FutureTransactionProcessor(), true);
        logger = nodeEngine.getLogger(TransactionManagerService.class);
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public <T> T executeTransaction(TransactionalTask<T> task, TransactionOptions options) throws TransactionException {
        final TransactionContextImpl context = new TransactionContextImpl(nodeEngine, options);
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
            throw new TransactionException(e);
        }
    }

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextImpl(nodeEngine, options);
    }

    ConcurrencyUtil.ConstructorFunction<TransactionKey, TransactionLog> logConstructor = new ConcurrencyUtil.ConstructorFunction<TransactionKey, TransactionLog>() {
        public TransactionLog createNew(TransactionKey key) {
            return new TransactionLog(key.txnId, key.partitionId);
        }
    };

    void addTransactionalOperation(int partitionId, TransactionalOperation transactionalOperation) throws TransactionException {
        final TransactionKey key = new TransactionKey(transactionalOperation.getTransactionId(), partitionId);
        final TransactionLog transactionLog = ConcurrencyUtil.getOrPutIfAbsent(txLogs, key, logConstructor);
        if (transactionLog.getState() != Transaction.State.ACTIVE) {
            throw new TransactionException("Tx is not active!");
        }
        transactionLog.addOperationRecord(transactionalOperation);
    }

    private class FutureTransactionProcessor implements ScheduledEntryProcessor<TransactionKey, Object> {
        public void process(EntryTaskScheduler<TransactionKey, Object> scheduler, Collection<ScheduledEntry<TransactionKey, Object>> scheduledEntries) {
            for (ScheduledEntry<TransactionKey, Object> entry : scheduledEntries) {
                final TransactionLog log = txLogs.get(entry.getKey());
                final PartitionService partitionService = nodeEngine.getPartitionService();
                if (log != null) {
                    if (log.isBeingProcessed() || partitionService.isPartitionMigrating(log.getPartitionId())) {
                        scheduler.schedule(ONE_MIN_MS / 2, entry.getKey(), DUMMY_OBJECT);
                    } else if (nodeEngine.getClusterService().getMember(log.getCallerUuid()) == null) {
                        txLogs.remove(entry.getKey());
                        final OperationService operationService = nodeEngine.getOperationService();
                        Operation op = null;
                        boolean broadcast = false;
                        switch (log.getState()) {
                            case PREPARED:
                                logger.log(Level.WARNING, "Rolling-back previously prepared transaction[" + log.getTxnId()
                                        + "], because caller is not a member of the cluster anymore!");
                                rollbackTransactionLog(log);
                                break;
                            case COMMITTED:
                                logger.log(Level.INFO, "Broadcasting COMMIT message for transaction[" + log.getTxnId()
                                        + "], because caller is not a member of the cluster anymore!");
                                op = new BroadcastCommitOperation(log.getTxnId());
                                broadcast = true;
                                break;
                            case ROLLED_BACK:
                                logger.log(Level.INFO, "Broadcasting ROLLBACK message for transaction[" + log.getTxnId()
                                        + "], because caller is not a member of the cluster anymore!");
                                op = new BroadcastRollbackOperation(log.getTxnId());
                                broadcast = true;
                        }
                        if (broadcast) {
                            op.setNodeEngine(nodeEngine).setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                            final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
                            for (MemberImpl member : members) {
                                if (member.localMember()) {
                                    operationService.executeOperation(op);
                                } else {
                                    final Invocation inv = operationService.createInvocationBuilder(SERVICE_NAME,
                                            op, member.getAddress()).build();
                                    inv.invoke();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    void prepare(String caller, String txnId, int partitionId) throws TransactionException {
        final TransactionKey key = new TransactionKey(txnId, partitionId);
        final TransactionLog log = txLogs.get(key);
        if (log == null) {
            throw new TransactionException("No tx available!");
        }
        if (!log.beginProcess()) {
            throw new TransactionException("Tx log is already being processed!");
        }
        try {
            log.setCallerUuid(caller);
            log.setState(Transaction.State.PREPARED);
            for (TransactionalOperation op : log.getOperationRecords()) {
                op.doPrepare();
            }
            scheduler.schedule(ONE_MIN_MS * 2, key, DUMMY_OBJECT);
            log.setScheduled(true);
        } finally {
            log.endProcess();
        }
    }

    void commit(String caller, String txnId, int partitionId) throws TransactionException {
        final TransactionKey key = new TransactionKey(txnId, partitionId);
        final TransactionLog log = txLogs.get(key);
        if (log == null) {
            throw new TransactionException("No tx available!");
        }
        if (!log.beginProcess()) {
            throw new TransactionException("Tx log is already being processed!");
        }
        try {
            log.setCallerUuid(caller);
            log.setState(Transaction.State.COMMITTED);
            commitTransactionLog(log);
            scheduler.schedule(ONE_MIN_MS, key, DUMMY_OBJECT);
            log.setScheduled(true);
        } finally {
            log.endProcess();
        }
    }

    void rollback(String caller, String txnId, int partitionId) throws TransactionException {
        final TransactionKey key = new TransactionKey(txnId, partitionId);
        final TransactionLog log = txLogs.get(key);
        if (log == null) {
            logger.log(Level.WARNING, "Ignoring roll-back, no transaction available!");
            return;
        }
        if (!log.beginProcess()) {
            throw new TransactionException("Tx log is already being processed!");
        }
        try {
            log.setCallerUuid(caller);
            log.setState(Transaction.State.ROLLED_BACK);
            rollbackTransactionLog(log);
            scheduler.schedule(ONE_MIN_MS, key, DUMMY_OBJECT);
            log.setScheduled(true);
        } finally {
            log.endProcess();
        }
    }

    public void commitAll(String txnId) {
        final Iterator<TransactionLog> iter = txLogs.values().iterator();
        while (iter.hasNext()) {
            final TransactionLog log = iter.next();
            if (txnId.equals(log.getTxnId())) {
                iter.remove();
                commitTransactionLog(log);
            }
        }
    }

    private void commitTransactionLog(TransactionLog log) {
        String txnId = log.getTxnId();
        for (TransactionalOperation op : log.getOperationRecords()) {
            try {
                op.doCommit();
            } catch (Throwable e) {
                logger.log(Level.WARNING, "Problem while committing the transaction[" + txnId + "]!", e);
            }
        }
    }

    public void rollbackAll(String txnId) {
        final Iterator<TransactionLog> iter = txLogs.values().iterator();
        while (iter.hasNext()) {
            final TransactionLog log = iter.next();
            if (txnId.equals(log.getTxnId())) {
                iter.remove();
                rollbackTransactionLog(log);
            }
        }
    }

    private void rollbackTransactionLog(TransactionLog log) {
        final List<TransactionalOperation> ops = log.getOperationRecords();
        // Rollback should be done in reverse order!
        final ListIterator<TransactionalOperation> iter = ops.listIterator(ops.size());
        while (iter.hasPrevious()) {
            final TransactionalOperation op = iter.previous();
            try {
                op.doRollback();
            } catch (Throwable e) {
                logger.log(Level.WARNING, "Problem while rolling-back the transaction[" + log.getTxnId() + "]!", e);
            }
        }
    }

    private int getTimeout(Transaction.State state) {
        return state == Transaction.State.PREPARED ? 2 * ONE_MIN_MS : ONE_MIN_MS;
    }

    void addLog(TransactionLog log) {
        txLogs.put(new TransactionKey(log.getTxnId(), log.getPartitionId()), log);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        scheduler.cancelAll();
        txLogs.clear();
    }

    public void shutdown() {
        reset();
    }

    public void beforeMigration(MigrationServiceEvent event) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        if (event.getReplicaIndex() != 0) {
            return null;
        }
        if (txLogs.isEmpty()) {
            return null;
        }
        final Collection<TransactionLog> logs = new LinkedList<TransactionLog>();
        for (TransactionLog log : txLogs.values()) {
            if (log.getPartitionId() == event.getPartitionId()) {
                logs.add(log);
            }
        }
        if (logs.isEmpty()) {
            return null;
        }
        return new TxLogMigrationOperation(logs);
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getReplicaIndex() != 0) {
            return;
        }
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE
                && event.getMigrationType() == MigrationType.MOVE) {
            clearPartition(event.getPartitionId());
        } else if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            for (Map.Entry<TransactionKey, TransactionLog> entry : txLogs.entrySet()) {
                final TransactionLog log = entry.getValue();
                if (!log.isScheduled()) {
                    scheduler.schedule(getTimeout(log.getState()), entry.getKey(), DUMMY_OBJECT);
                }
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getReplicaIndex() != 0) {
            return;
        }
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartition(event.getPartitionId());
        }
    }

    private void clearPartition(int partitionId) {
        final Iterator<TransactionKey> iter = txLogs.keySet().iterator();
        while (iter.hasNext()) {
            final TransactionKey key = iter.next();
            if (key.partitionId == partitionId) {
                iter.remove();
                scheduler.cancel(key);
            }
        }
    }

    public int getMaxBackupCount() {
        return 0;
    }

    private static class TransactionKey {
        private final String txnId;
        private final int partitionId;

        private TransactionKey(String txnId, int partitionId) {
            this.txnId = txnId;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransactionKey that = (TransactionKey) o;

            if (partitionId != that.partitionId) return false;
            if (txnId != null ? !txnId.equals(that.txnId) : that.txnId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = txnId != null ? txnId.hashCode() : 0;
            result = 31 * result + partitionId;
            return result;
        }
    }
}
