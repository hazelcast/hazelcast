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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.transaction.impl.Transaction.State;

/**
 * @author mdogan 2/26/13
 */
public class TransactionManagerServiceImpl implements TransactionManagerService, ManagedService,
        MembershipAwareService, ClientAwareService {

    public static final String SERVICE_NAME = "hz:core:txManagerService";

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final ConcurrentMap<String, TxBackupLog> txBackupLogs = new ConcurrentHashMap<String, TxBackupLog>();

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

    public void memberRemoved(MembershipServiceEvent event) {
        final MemberImpl member = event.getMember();
        String uuid = member.getUuid();
        finalizeTransactionsOf(uuid);
    }

    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    private void finalizeTransactionsOf(String uuid) {
        if (!txBackupLogs.isEmpty()) {
            for (Map.Entry<String, TxBackupLog> entry : txBackupLogs.entrySet()) {
                TxBackupLog log = entry.getValue();
                if (uuid.equals(log.callerUuid)) {
                    String txnId = entry.getKey();
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

    void beginTxBackupLog(String callerUuid, String txnId) {
        TxBackupLog log = new TxBackupLog(Collections.<TransactionLog>emptyList(), callerUuid, State.ACTIVE, -1, -1);
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
        if (!txBackupLogs.replace(txnId, beginLog, new TxBackupLog(txLogs, callerUuid, State.COMMITTING, timeoutMillis, startTime))) {
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

    private static class TxBackupLog {
        private final List<TransactionLog> txLogs;
        private final String callerUuid;
        private final long timeoutMillis;
        private final long startTime;

        private volatile State state;

        private TxBackupLog(List<TransactionLog> txLogs, String callerUuid, State state, long timeoutMillis, long startTime) {
            this.txLogs = txLogs;
            this.callerUuid = callerUuid;
            this.state = state;
            this.timeoutMillis = timeoutMillis;
            this.startTime = startTime;
        }
    }
}
