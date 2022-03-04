/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy.txn;

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionCommitCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec;
import com.hazelcast.client.impl.protocol.codec.TransactionRollbackCodec;
import com.hazelcast.logging.ILogger;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.transaction.TransactionTimedOutException;

import java.util.UUID;

import javax.annotation.Nonnull;

import static com.hazelcast.transaction.impl.Transaction.State;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.COMMIT_FAILED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

final class TransactionProxy {

    private static final ThreadLocal<Boolean> TRANSACTION_EXISTS = new ThreadLocal<Boolean>();

    private final TransactionOptions options;
    private final HazelcastClientInstanceImpl client;
    private final long threadId = ThreadUtil.getThreadId();
    private final ClientConnection connection;
    private final ILogger logger;

    private UUID txnId;
    private State state = NO_TXN;
    private long startTime;

    TransactionProxy(HazelcastClientInstanceImpl client,
                     @Nonnull TransactionOptions options, ClientConnection connection) {
        this.options = options;
        this.client = client;
        this.connection = connection;
        this.logger = client.getLoggingService().getLogger(TransactionProxy.class);
    }

    public UUID getTxnId() {
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
            ClientMessage response = ClientTransactionUtil.invoke(request, getTxnId(), client, connection);
            txnId = TransactionCreateCodec.decodeResponse(response);
            state = ACTIVE;
        } catch (Exception e) {
            TRANSACTION_EXISTS.set(null);
            throw rethrow(e);
        }
    }

    void commit() {
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            state = COMMITTING;
            checkThread();
            checkTimeout();
            ClientMessage request = TransactionCommitCodec.encodeRequest(txnId, threadId);
            ClientTransactionUtil.invoke(request, getTxnId(), client, connection);
            state = COMMITTED;
        } catch (Exception e) {
            state = COMMIT_FAILED;
            throw rethrow(e);
        } finally {
            TRANSACTION_EXISTS.set(null);
        }
    }

    void rollback() {
        try {
            if (state == NO_TXN || state == ROLLED_BACK) {
                throw new IllegalStateException("Transaction is not active");
            }
            state = ROLLING_BACK;
            checkThread();
            try {
                ClientMessage request = TransactionRollbackCodec.encodeRequest(txnId, threadId);
                ClientTransactionUtil.invoke(request, getTxnId(), client, connection);
            } catch (Exception exception) {
                logger.warning("Exception while rolling back the transaction", exception);
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
            throw new TransactionTimedOutException("Transaction is timed-out!");
        }
    }

}
