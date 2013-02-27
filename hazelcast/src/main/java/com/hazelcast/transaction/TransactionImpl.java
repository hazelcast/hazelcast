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
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.Transaction.State.*;
import static com.hazelcast.transaction.TransactionManagerService.SERVICE_NAME;

public final class TransactionImpl implements Transaction {

    private final NodeEngine nodeEngine;
    private final Map<Integer, Collection<String>> participants = new HashMap<Integer, Collection<String>>(3); // partitionId -> services

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

    public void attachParticipant(int partitionId, String serviceName) {
        Collection<String> services = participants.get(partitionId);
        if (services == null) {
            services = new HashSet<String>(3);
            participants.put(partitionId, services);
        }
        services.add(serviceName);
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
            for (Map.Entry<Integer, Collection<String>> entry : participants.entrySet()) {
                final int partitionId = entry.getKey();
                PrepareOperation op = new PrepareOperation(txnId, getServicesArray(entry.getValue()));
                futures.add(operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                        .build().invoke());
            }
            for (Future future : futures) {
                future.get(timeoutSeconds, TimeUnit.SECONDS);
            }
            state = PREPARED;
            futures.clear();
            state = COMMITTING;
            for (Map.Entry<Integer, Collection<String>> entry : participants.entrySet()) {
                final int partitionId = entry.getKey();
                CommitOperation op = new CommitOperation(txnId, getServicesArray(entry.getValue()));
                futures.add(operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                        .build().invoke());
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

    private static String[] getServicesArray(Collection<String> services) {
        return services.toArray(new String[services.size()]);
    }

    public void rollback() throws IllegalStateException {
        if (state == NO_TXN || state == ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        state = ROLLING_BACK;
        try {
            List<Future> futures = new ArrayList<Future>(participants.size());
            for (Map.Entry<Integer, Collection<String>> entry : participants.entrySet()) {
                final int partitionId = entry.getKey();
                RollbackOperation op = new RollbackOperation(txnId, getServicesArray(entry.getValue()));
                futures.add(nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op, partitionId)
                        .build().invoke());
            }
            for (Future future : futures) {
                future.get(timeoutSeconds, TimeUnit.SECONDS);
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
