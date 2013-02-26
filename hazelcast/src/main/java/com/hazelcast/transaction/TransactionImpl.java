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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.Transaction.State.*;

public final class TransactionImpl implements Transaction {

    private final NodeEngine nodeEngine;
    private final Set<TxnParticipant> participants = new HashSet<TxnParticipant>(3);

    private final String txnId = UUID.randomUUID().toString();
    private State state = NO_TXN;
    private long timeoutSeconds = TimeUnit.MINUTES.toSeconds(5);

    public TransactionImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public String getTxnId() {
        return txnId;
    }

    public void setTransactionTimeout(int seconds)  {
        timeoutSeconds = seconds;
    }

    public void attachParticipant(String serviceName, int partitionId) {
        participants.add(new TxnParticipant(serviceName, partitionId));
    }

    private class TxnParticipant {
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
        if (state == ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        state = ACTIVE;
    }

    public void commit() throws TransactionException, IllegalStateException {
        if (state != ACTIVE) {
            throw new IllegalStateException("Transaction is not active");
        }
        try {
            state = PREPARING;
            final List<Future> futures = new ArrayList<Future>(participants.size());
            final OperationService operationService = nodeEngine.getOperationService();
            for (TxnParticipant t : participants) {
                Operation op = new PrepareOperation(txnId);
                futures.add(operationService.createInvocationBuilder(t.serviceName, op, t.partitionId).build()
                        .invoke());
            }
            for (Future future : futures) {
                future.get(timeoutSeconds, TimeUnit.SECONDS);
            }
            state = PREPARED;
            futures.clear();
            state = COMMITTING;
            for (TxnParticipant t : participants) {
                Operation op = new CommitOperation(txnId);
                futures.add(operationService.createInvocationBuilder(t.serviceName, op, t.partitionId).build()
                        .invoke());
            }
            for (Future future : futures) {
                future.get(timeoutSeconds, TimeUnit.SECONDS);
            }
            state = COMMITTED;
        } catch (Throwable e) {
            state = COMMIT_FAILED;
            if (e instanceof ExecutionException && e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            }
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void rollback() throws IllegalStateException {
        if (state == NO_TXN || state == ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        state = ROLLING_BACK;
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
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            state = ROLLED_BACK;
        }
    }

    public int getStatus() {
        return state.getValue();
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Transaction");
        sb.append("{txnId='").append(txnId).append('\'');
        sb.append(", state=").append(state);
        sb.append(", timeoutSeconds=").append(timeoutSeconds);
        sb.append('}');
        return sb.toString();
    }
}
