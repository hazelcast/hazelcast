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

import com.hazelcast.core.Transaction;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TransactionImpl implements Transaction {

    private final HazelcastInstanceImpl instance;
    private final NodeEngine nodeEngine;
    private final Set<TxnParticipant> participants = new HashSet<TxnParticipant>(1);

    private int status = TXN_STATUS_NO_TXN;
    private final ILogger logger;
    private final String txnId = UUID.randomUUID().toString();

    public TransactionImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.getLoggingService().getLogger(this.getClass().getName());
        this.nodeEngine = instance.node.nodeEngine;
    }

    public String getTxnId() {
        return txnId;
    }

    public void attachParticipant(String serviceName, int partitionId) {
        participants.add(new TxnParticipant(serviceName, partitionId));
    }

    class TxnParticipant {
        final String serviceName;
        final int partitionId;

        TxnParticipant(String serviceName, int partitionId) {
            this.serviceName = serviceName;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TxnParticipant that = (TxnParticipant) o;
            if (partitionId != that.partitionId) return false;
            if (serviceName != null ? !serviceName.equals(that.serviceName) : that.serviceName != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = serviceName != null ? serviceName.hashCode() : 0;
            result = 31 * result + partitionId;
            return result;
        }
    }

    public void begin() throws IllegalStateException {
        if (status == TXN_STATUS_ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        status = TXN_STATUS_ACTIVE;
        ThreadContext.setTransaction(instance.getName(), this);
    }

    public void commit() throws IllegalStateException {
        if (status != TXN_STATUS_ACTIVE) {
            throw new IllegalStateException("Transaction is not active");
        }
        try {
            status = TXN_STATUS_PREPARING;
            List<Future> futures = new ArrayList<Future>(participants.size());
            for (TxnParticipant t : participants) {
                Operation op = new PrepareOperation(txnId);
                futures.add(nodeEngine.getOperationService().createInvocationBuilder(t.serviceName, op, t.partitionId).build()
                        .invoke());
            }
            for (Future future : futures) {
                future.get(300, TimeUnit.SECONDS);
            }
            status = TXN_STATUS_PREPARED;
            futures.clear();
            status = TXN_STATUS_COMMITTING;
            for (TxnParticipant t : participants) {
                Operation op = new CommitOperation(txnId);
                futures.add(nodeEngine.getOperationService().createInvocationBuilder(t.serviceName, op, t.partitionId).build()
                        .invoke());
            }
            for (Future future : futures) {
                future.get(300, TimeUnit.SECONDS);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            finalizeTxn();
            status = TXN_STATUS_COMMITTED;
        }
    }

    public void rollback() throws IllegalStateException {
        if (status == TXN_STATUS_NO_TXN) {
            throw new IllegalStateException("Transaction is not active");
        }
        status = TXN_STATUS_ROLLING_BACK;
        try {
            List<Future> futures = new ArrayList<Future>(participants.size());
            for (TxnParticipant t : participants) {
                Operation op = new RollbackOperation(txnId);
                futures.add(nodeEngine.getOperationService().createInvocationBuilder(t.serviceName, op, t.partitionId).build()
                        .invoke());
            }
            for (Future future : futures) {
                future.get(300, TimeUnit.SECONDS);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            finalizeTxn();
            status = TXN_STATUS_ROLLED_BACK;
        }
    }

    public int getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "TransactionImpl [" + txnId + "] status: " + status;
    }

    private void finalizeTxn() {
        status = TXN_STATUS_NO_TXN;
        ThreadContext.finalizeTransaction(instance.getName());
    }
}
