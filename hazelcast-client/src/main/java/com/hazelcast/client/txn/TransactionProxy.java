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

package com.hazelcast.client.txn;

import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.SerializableXID;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

import static com.hazelcast.transaction.impl.Transaction.State;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;

/**
 * @author ali 6/6/13
 */
final class TransactionProxy {

    private static final ThreadLocal<Boolean> threadFlag = new ThreadLocal<Boolean>();

    private final TransactionOptions options;
    private final HazelcastClient client;
    private final long threadId = Thread.currentThread().getId();
    private final ClientConnection connection;

    private SerializableXID sXid;
    private String txnId;
    private State state = NO_TXN;
    private long startTime = 0L;

    TransactionProxy(HazelcastClient client, TransactionOptions options, ClientConnection connection) {
        this.options = options;
        this.client = client;
        this.connection = connection;
    }

    public String getTxnId() {
        return txnId;
    }

    public State getState() {
        return state;
    }

    public long getTimeoutMillis() {
        return options.getTimeoutMillis();
    }

    void begin() {
        try {
            if (state == ACTIVE) {
                throw new IllegalStateException("Transaction is already active");
            }
            checkThread();
            if (threadFlag.get() != null) {
                throw new IllegalStateException("Nested transactions are not allowed!");
            }
            threadFlag.set(Boolean.TRUE);
            startTime = Clock.currentTimeMillis();
            txnId = invoke(new CreateTransactionRequest(options, sXid));
            state = ACTIVE;
        } catch (Exception e) {
            closeConnection();
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void prepare() {
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            checkThread();
            checkTimeout();
            invoke(new PrepareTransactionRequest());
            state = PREPARED;
        } catch (Exception e) {
            state = ROLLING_BACK;
            closeConnection();
            throw ExceptionUtil.rethrow(e);
        }
    }

    void commit(boolean prepareAndCommit) {
        try {
            if (prepareAndCommit && state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            if (!prepareAndCommit && state != PREPARED) {
                throw new TransactionNotActiveException("Transaction is not prepared");
            }
            checkThread();
            checkTimeout();
            invoke(new CommitTransactionRequest(prepareAndCommit));
            state = COMMITTED;
        } catch (Exception e) {
            state = ROLLING_BACK;
            throw ExceptionUtil.rethrow(e);
        } finally {
            closeConnection();
        }
    }

    void rollback() {
        try {
            if (state == NO_TXN || state == ROLLED_BACK) {
                throw new IllegalStateException("Transaction is not active");
            }
            if (state == ROLLING_BACK) {
                state = ROLLED_BACK;
                return;
            }
            checkThread();
            try {
                invoke(new RollbackTransactionRequest());
            } catch (Exception ignored) {
            }
            state = ROLLED_BACK;
        } finally {
            closeConnection();
        }
    }

    SerializableXID getXid() {
        return sXid;
    }

    void setXid(SerializableXID xid) {
        this.sXid = xid;
    }

    private void closeConnection() {
        threadFlag.set(null);
//        try {
//            connection.release();
//        } catch (IOException e) {
//            IOUtil.closeResource(connection);
//        }
    }

    private void checkThread() {
        if (threadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("Transaction cannot span multiple threads!");
        }
    }

    private void checkTimeout() {
        if (startTime + options.getTimeoutMillis() < Clock.currentTimeMillis()) {
            throw new TransactionException("Transaction is timed-out!");
        }
    }

    private <T> T invoke(ClientRequest request) {
        if (request instanceof BaseTransactionRequest) {
            ((BaseTransactionRequest) request).setTxnId(txnId);
            ((BaseTransactionRequest) request).setClientThreadId(threadId);
        }
        final SerializationService ss = client.getSerializationService();
        final ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        try {
            final Future f = invocationService.send(request, connection);
            return ss.toObject(f.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
