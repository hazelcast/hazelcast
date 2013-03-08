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

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.transaction.Transaction.State.*;
import static com.hazelcast.transaction.TransactionManagerServiceImpl.SERVICE_NAME;

final class TransactionImpl implements Transaction {

    private final NodeEngine nodeEngine;
    private final Set<Integer> partitions = new HashSet<Integer>(5);

    private final String txnId = UUID.randomUUID().toString();
    private State state = NO_TXN;
    private long timeoutMillis = TimeUnit.MINUTES.toMillis(2);
    private long startTime = 0L;

    public TransactionImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public String getTxnId() {
        return txnId;
    }

    public void addPartition(int partitionId) {
        partitions.add(partitionId);
    }

    void begin() throws IllegalStateException {
        if (state == ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        startTime = Clock.currentTimeMillis();
        state = ACTIVE;
    }

    void commit() throws TransactionException, IllegalStateException {
        if (state != ACTIVE) {
            throw new IllegalStateException("Transaction is not active");
        }
        checkTimeout();
        try {
            state = PREPARING;
            final List<Future> futures = new ArrayList<Future>(partitions.size());
            final OperationService operationService = nodeEngine.getOperationService();
            for (Integer partitionId : partitions) {
                PrepareOperation op = new PrepareOperation(txnId);
                futures.add(operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                        .build().invoke());
            }
            for (Future future : futures) {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            state = PREPARED;
            futures.clear();
            state = COMMITTING;
            for (Integer partitionId : partitions) {
                CommitOperation op = new CommitOperation(txnId);
                futures.add(operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                        .build().invoke());
            }
            for (Future future : futures) {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
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

    private void checkTimeout() throws TransactionException {
        if (startTime + timeoutMillis < Clock.currentTimeMillis()) {
            throw new TransactionException("Transaction is timed-out!");
        }
    }

    void rollback() throws IllegalStateException {
        if (state == NO_TXN || state == ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        state = ROLLING_BACK;
        try {
            List<Future> futures = new ArrayList<Future>(partitions.size());
            for (Integer partitionId : partitions) {
                RollbackOperation op = new RollbackOperation(txnId);
                futures.add(nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op, partitionId)
                        .build().invoke());
            }
            for (Future future : futures) {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            state = ROLLED_BACK;
        }
    }

    public State getState() {
        return state;
    }

    void setTimeout(int timeout, TimeUnit unit) {
        timeoutMillis = unit.toMillis(timeout);
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Transaction");
        sb.append("{txnId='").append(txnId).append('\'');
        sb.append(", state=").append(state);
        sb.append(", timeoutMillis=").append(timeoutMillis);
        sb.append('}');
        return sb.toString();
    }
}
