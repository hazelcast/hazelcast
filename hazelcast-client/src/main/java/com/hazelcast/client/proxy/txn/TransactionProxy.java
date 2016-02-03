/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy.txn;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionCommitCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionRollbackCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.Future;

import static com.hazelcast.transaction.impl.Transaction.State;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;

final class TransactionProxy {

    private static final ThreadLocal<Boolean> TRANSACTION_EXISTS = new ThreadLocal<Boolean>();

    private final TransactionOptions options;
    private final HazelcastClientInstanceImpl client;
    private final long threadId = ThreadUtil.getThreadId();
    private final ClientConnection connection;

    private String txnId;
    private State state = NO_TXN;
    private long startTime;

    TransactionProxy(HazelcastClientInstanceImpl client, TransactionOptions options, ClientConnection connection) {
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

    void begin() {
        try {
            if (state == ACTIVE) {
                throw new IllegalStateException("Transaction is already active");
            }
            checkThread();
            if (TRANSACTION_EXISTS.get() != null) {
                throw new IllegalStateException("Nested transactions are not allowed!");
            }
            TRANSACTION_EXISTS.set(Boolean.TRUE);
            startTime = Clock.currentTimeMillis();
            ClientMessage request = TransactionCreateCodec.encodeRequest(options.getTimeoutMillis(),
                    options.getDurability(), options.getTransactionType().id(), threadId);
            ClientMessage response = invoke(request);
            TransactionCreateCodec.ResponseParameters result = TransactionCreateCodec.decodeResponse(response);
            txnId = result.response;
            state = ACTIVE;
        } catch (Exception e) {
            TRANSACTION_EXISTS.set(null);
            throw ExceptionUtil.rethrow(e);
        }
    }

    void commit() {
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            checkThread();
            checkTimeout();
            ClientMessage request = TransactionCommitCodec.encodeRequest(txnId, threadId);
            invoke(request);
            state = COMMITTED;
        } catch (Exception e) {
            state = ROLLING_BACK;
            throw ExceptionUtil.rethrow(e);
        } finally {
            TRANSACTION_EXISTS.set(null);
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
                ClientMessage request = TransactionRollbackCodec.encodeRequest(txnId, threadId);
                invoke(request);
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
            state = ROLLED_BACK;
        } finally {
            TRANSACTION_EXISTS.set(null);
        }
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

    private ClientMessage invoke(ClientMessage request) {
        try {
            final ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
            final Future<ClientMessage> future = clientInvocation.invoke();
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
