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

package com.hazelcast.transaction.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.*;
import com.hazelcast.util.ExceptionUtil;

import javax.transaction.xa.Xid;
import java.util.*;
import java.util.concurrent.*;

import static com.hazelcast.transaction.impl.Transaction.State;

/**
 * @author mdogan 2/26/13
 */
public class TransactionManagerServiceImpl implements TransactionManagerService, ManagedService,
        MembershipAwareService, ClientAwareService {

    public static final String SERVICE_NAME = "hz:core:txManagerService";

    public final static int RECOVER_TIMEOUT = 5000;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final ConcurrentMap<String, TxBackupLog> txBackupLogs = new ConcurrentHashMap<String, TxBackupLog>();
    private final ConcurrentMap<SerializableXID, Transaction>
            managedTransactions = new ConcurrentHashMap<SerializableXID, Transaction>();
    private final ConcurrentMap<SerializableXID, RecoveredTransaction>
            clientRecoveredTransactions = new ConcurrentHashMap<SerializableXID, RecoveredTransaction>();

    public TransactionManagerServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(TransactionManagerService.class);
    }

    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        if (task == null) {
            throw new NullPointerException("TransactionalTask is required!");
        }
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

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextImpl(this, nodeEngine, options, null);
    }

    public TransactionContext newClientTransactionContext(TransactionOptions options, String clientUuid) {
        return new TransactionContextImpl(this, nodeEngine, options, clientUuid);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        txBackupLogs.clear();
    }

    public void shutdown(boolean terminate) {
        reset();
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void addClientRecoveredTransaction(RecoveredTransaction rt) {
        clientRecoveredTransactions.put(rt.getXid(), rt);
    }

    public void recoverClientTransaction(SerializableXID sXid, boolean commit) {
        final RecoveredTransaction rt = clientRecoveredTransactions.remove(sXid);
        if (rt == null) {
            return;
        }
        TransactionImpl tx = new TransactionImpl(this, nodeEngine, rt.getTxnId(), rt.getTxLogs(),
                rt.getTimeoutMillis(), rt.getStartTime(), rt.getCallerUuid());
        if (commit) {
            try {
                tx.commit();
            } catch (Throwable e) {
                logger.warning("Error during committing recovered client transaction!", e);
            }
        } else {
            try {
                tx.rollback();
            } catch (Throwable e) {
                logger.warning("Error during rolling-back recovered client transaction!", e);
            }
        }
    }

    public void memberRemoved(MembershipServiceEvent event) {
        final MemberImpl member = event.getMember();
        String uuid = member.getUuid();
        finalizeTransactionsOf(uuid);
    }

    public void memberAttributeChanged(MemberAttributeServiceEvent event) {

    }

    public void addManagedTransaction(Xid xid, Transaction transaction) {
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        ((TransactionImpl) transaction).setXid(sXid);
        managedTransactions.put(sXid, transaction);
    }

    public Transaction getManagedTransaction(Xid xid) {
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        return managedTransactions.get(sXid);
    }

    public void removeManagedTransaction(Xid xid) {
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        managedTransactions.remove(sXid);
    }

    private void finalizeTransactionsOf(String uuid) {
        if (!txBackupLogs.isEmpty()) {
            for (Map.Entry<String, TxBackupLog> entry : txBackupLogs.entrySet()) {
                TxBackupLog log = entry.getValue();
                if (uuid.equals(log.callerUuid)) {
                    String txnId = entry.getKey();  //TODO shouldn't we remove TxBackupLog from map ?
                    if (log.state == State.ACTIVE) {
                        Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberList();
                        Collection<Future> futures = new ArrayList<Future>(memberList.size());
                        for (MemberImpl member : memberList) {
                            Operation op = new BroadcastTxRollbackOperation(txnId);
                            Future f = nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                            futures.add(f);
                        }
                        for (Future future : futures) {
                            try {
                                future.get(TransactionOptions.getDefault().getTimeoutMillis(), TimeUnit.MILLISECONDS);
                            } catch (Exception e) {
                                logger.warning("Error while rolling-back tx!");
                            }
                        }
                    } else {
                        if (log.state == State.COMMITTING && log.xid != null) {
                            logger.warning("This log is XA Managed " + log);
                            log.state = State.NO_TXN; //Marking for recovery
                            continue;
                        }
                        TransactionImpl tx = new TransactionImpl(this, nodeEngine, txnId, log.txLogs, log.timeoutMillis, log.startTime, log.callerUuid);
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
            }
        }
    }

    public void clientDisconnected(String clientUuid) {
        finalizeTransactionsOf(clientUuid);
    }

    Address[] pickBackupAddresses(int durability) {
        final ClusterService clusterService = nodeEngine.getClusterService();
        final List<MemberImpl> members = new ArrayList<MemberImpl>(clusterService.getMemberList());
        members.remove(nodeEngine.getLocalMember());
        final int c = Math.min(members.size(), durability);
        Collections.shuffle(members);
        Address[] addresses = new Address[c];
        for (int i = 0; i < c; i++) {
            addresses[i] = members.get(i).getAddress();
        }
        return addresses;
    }

    public void addTxBackupLogForClientRecovery(Transaction transaction) {
        TransactionImpl txnImpl = (TransactionImpl) transaction;
        final String callerUuid = txnImpl.getOwnerUuid();
        final SerializableXID xid = txnImpl.getXid();
        final List<TransactionLog> txLogs = txnImpl.getTxLogs();
        final long timeoutMillis = txnImpl.getTimeoutMillis();
        final long startTime = txnImpl.getStartTime();
        TxBackupLog log = new TxBackupLog(txLogs, callerUuid, State.COMMITTING, timeoutMillis, startTime, xid);
        txBackupLogs.put(txnImpl.getTxnId(), log);
    }

    void beginTxBackupLog(String callerUuid, String txnId, SerializableXID xid) {
        TxBackupLog log = new TxBackupLog(Collections.<TransactionLog>emptyList(), callerUuid, State.ACTIVE, -1, -1, xid);
        if (txBackupLogs.putIfAbsent(txnId, log) != null) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    void prepareTxBackupLog(List<TransactionLog> txLogs, String callerUuid, String txnId, long timeoutMillis, long startTime) {
        TxBackupLog beginLog = txBackupLogs.get(txnId);
        if (beginLog == null) {
            throw new TransactionException("Could not find begin tx log!");
        }
        if (beginLog.state != State.ACTIVE) {
            throw new TransactionException("TxLog already exists!");
        }
        if (!txBackupLogs.replace(txnId, beginLog, new TxBackupLog(txLogs, callerUuid, State.COMMITTING, timeoutMillis, startTime, beginLog.xid))) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    void rollbackTxBackupLog(String txnId) {
        final TxBackupLog log = txBackupLogs.get(txnId);
        if (log != null) {
            log.state = State.ROLLING_BACK;
        } else {
            logger.warning("No tx backup log is found, tx -> " + txnId);
        }
    }

    void purgeTxBackupLog(String txnId) {
        txBackupLogs.remove(txnId);
    }

    public Xid[] recover() {
        final OperationService operationService = nodeEngine.getOperationService();
        final ClusterService clusterService = nodeEngine.getClusterService();
        final Collection<MemberImpl> memberList = clusterService.getMemberList();
        List<Future<SerializableCollection>> futures = new ArrayList<Future<SerializableCollection>>(memberList.size() - 1);
        for (MemberImpl member : memberList) {
            if (member.localMember()) {
                continue;
            }
            final Future f = operationService.createInvocationBuilder(TransactionManagerServiceImpl.SERVICE_NAME,
                    new RecoverTxnOperation(), member.getAddress()).invoke();
            futures.add(f);
        }
        Set<SerializableXID> xidSet = new HashSet<SerializableXID>();
        for (Future<SerializableCollection> future : futures) {
            try {
                final SerializableCollection collectionWrapper = future.get(RECOVER_TIMEOUT, TimeUnit.MILLISECONDS);
                for (Data data : collectionWrapper) {
                    final RecoveredTransaction rt = (RecoveredTransaction) nodeEngine.toObject(data);
                    final SerializableXID xid = rt.getXid();
                    TransactionImpl tx = new TransactionImpl(this, nodeEngine, rt.getTxnId(), rt.getTxLogs(),
                            rt.getTimeoutMillis(), rt.getStartTime(), rt.getCallerUuid());
                    tx.setXid(xid);
                    xidSet.add(xid);
                    managedTransactions.put(xid, tx);
                }
            } catch (MemberLeftException e) {
                logger.warning("Member left while recovering: " + e);
            } catch (Throwable e) {
                if (e instanceof ExecutionException) {
                    e = e.getCause() != null ? e.getCause() : e;
                }
                if (e instanceof TargetNotMemberException) {
                    nodeEngine.getLogger(Transaction.class).warning("Member left while recovering: " + e);
                } else {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        final Set<RecoveredTransaction> localSet = recoverLocal();
        for (RecoveredTransaction rt : localSet) {
            TransactionImpl tx = new TransactionImpl(this, nodeEngine, rt.getTxnId(), rt.getTxLogs(),
                    rt.getTimeoutMillis(), rt.getStartTime(), rt.getCallerUuid());
            xidSet.add(rt.getXid());
            managedTransactions.put(rt.getXid(), tx);
        }
        return xidSet.toArray(new Xid[xidSet.size()]);
    }

    public Set<RecoveredTransaction> recoverLocal() {
        Set<RecoveredTransaction> recovered = new HashSet<RecoveredTransaction>();
        if (!txBackupLogs.isEmpty()) {
            final Set<Map.Entry<String, TxBackupLog>> entries = txBackupLogs.entrySet();
            final Iterator<Map.Entry<String, TxBackupLog>> iter = entries.iterator();
            while (iter.hasNext()) {
                final Map.Entry<String, TxBackupLog> entry = iter.next();
                final TxBackupLog log = entry.getValue();
                final String txnId = entry.getKey();
                if (log.state == State.NO_TXN && log.xid != null) {
                    final RecoveredTransaction rt = new RecoveredTransaction();
                    rt.setTxLogs(log.txLogs);
                    rt.setXid(log.xid);
                    rt.setCallerUuid(log.callerUuid);
                    rt.setStartTime(log.startTime);
                    rt.setTimeoutMillis(log.timeoutMillis);
                    rt.setTxnId(txnId);
                    recovered.add(rt);
                    iter.remove();
                }
            }
        }
        return recovered;
    }

    private static class TxBackupLog {
        private final List<TransactionLog> txLogs;
        private final String callerUuid;
        private final long timeoutMillis;
        private final long startTime;
        private final SerializableXID xid;


        private volatile State state;

        private TxBackupLog(List<TransactionLog> txLogs, String callerUuid, State state, long timeoutMillis, long startTime, SerializableXID xid) {
            this.txLogs = txLogs;
            this.callerUuid = callerUuid;
            this.state = state;
            this.timeoutMillis = timeoutMillis;
            this.startTime = startTime;
            this.xid = xid;
        }
    }
}
