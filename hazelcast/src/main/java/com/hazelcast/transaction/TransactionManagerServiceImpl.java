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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * @mdogan 2/26/13
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

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        final TransactionContextImpl context = new TransactionContextImpl(this, nodeEngine, options);
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
            if (e instanceof Error) {
                throw (Error) e;
            }
            throw new TransactionException(e);
        }
    }

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextImpl(this, nodeEngine, options);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        txBackupLogs.clear();
    }

    public void shutdown() {
        reset();
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void memberRemoved(MembershipServiceEvent event) {
        final MemberImpl member = event.getMember();
        String uuid = member.getUuid();
        if (!txBackupLogs.isEmpty()) {
            for (TxBackupLog log : txBackupLogs.values()) {
                if (uuid.equals(log.callerUuid)) {
                    TransactionImpl tx = new TransactionImpl(this, nodeEngine, log.txnId, log.txLogs, log.timeoutMillis, log.startTime);
                    if (log.state == Transaction.State.COMMITTING) {
                        try {
                            tx.commit();
                        } catch (Throwable e) {
                            logger.log(Level.WARNING, "Error during committing from tx backup!", e);
                        }
                    } else {
                        try {
                            tx.rollback();
                        } catch (Throwable e) {
                            logger.log(Level.WARNING, "Error during rolling-back from tx backup!", e);
                        }
                    }
                }
            }
        }
    }

    public void clientDisconnected(String clientUuid) {
        // TODO: !!!
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

    void putTxBackupLog(List<TransactionLog> txLogs, String callerUuid, String txnId, long timeoutMillis, long startTime) {
        if (txBackupLogs.putIfAbsent(txnId, new TxBackupLog(txLogs, callerUuid, txnId, timeoutMillis, startTime)) != null) {
            throw new TransactionException("TxLog already exists!");
        }
    }

    void rollbackTxBackupLog(String txnId) {
        final TxBackupLog log = txBackupLogs.get(txnId);
        if (log != null) {
            log.state = Transaction.State.ROLLING_BACK;
        } else {
            logger.log(Level.WARNING, "No tx backup log is found, tx -> " + txnId);
        }
    }

    void purgeTxBackupLog(String txnId) {
        txBackupLogs.remove(txnId);
    }

    private class TxBackupLog {
        private final List<TransactionLog> txLogs;
        private final String callerUuid;
        private final String txnId;
        private final long timeoutMillis;
        private final long startTime;

        private volatile Transaction.State state = Transaction.State.COMMITTING;

        private TxBackupLog(List<TransactionLog> txLogs, String callerUuid, String txnId, long timeoutMillis, long startTime) {
            this.txLogs = txLogs;
            this.callerUuid = callerUuid;
            this.txnId = txnId;
            this.timeoutMillis = timeoutMillis;
            this.startTime = startTime;
        }
    }
}
