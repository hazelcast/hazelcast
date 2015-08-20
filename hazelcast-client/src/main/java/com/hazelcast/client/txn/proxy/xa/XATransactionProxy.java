/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.txn.proxy.xa;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.client.CommitXATransactionRequest;
import com.hazelcast.transaction.client.CreateXATransactionRequest;
import com.hazelcast.transaction.client.PrepareXATransactionRequest;
import com.hazelcast.transaction.client.RollbackXATransactionRequest;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;

/**
 * This class does not need to be thread-safe, it is only used via XAResource
 * All visibility guarantees handled by XAResource
 */
public class XATransactionProxy {

    private final HazelcastClientInstanceImpl client;
    private final ClientConnection connection;
    private final SerializableXID xid;
    private final int timeout;

    private Transaction.State state = NO_TXN;
    private volatile String txnId;
    private long startTime;

    public XATransactionProxy(HazelcastClientInstanceImpl client, ClientConnection connection, Xid xid, int timeout) {
        this.client = client;
        this.connection = connection;
        this.timeout = timeout;
        this.xid = new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
    }

    void begin() {
        try {
            startTime = Clock.currentTimeMillis();
            txnId = invoke(new CreateXATransactionRequest(xid, timeout));
            state = ACTIVE;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    void prepare() {
        checkTimeout();
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            PrepareXATransactionRequest request = new PrepareXATransactionRequest(txnId);
            invoke(request);
            state = PREPARED;
        } catch (Exception e) {
            state = ROLLING_BACK;
            throw ExceptionUtil.rethrow(e);
        }
    }

    void commit(boolean onePhase) {
        checkTimeout();
        try {
            if (onePhase && state != ACTIVE) {
                throw new TransactionException("Transaction is not active");
            }
            if (!onePhase && state != PREPARED) {
                throw new TransactionException("Transaction is not prepared");
            }
            invoke(new CommitXATransactionRequest(txnId, onePhase));
            state = COMMITTED;
        } catch (Exception e) {
            state = ROLLING_BACK;
            throw ExceptionUtil.rethrow(e);
        }
    }

    void rollback() {
        try {
            RollbackXATransactionRequest request = new RollbackXATransactionRequest(txnId);
            invoke(request);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
        state = ROLLED_BACK;
    }

    public String getTxnId() {
        return txnId;
    }

    public Transaction.State getState() {
        return state;
    }

    private void checkTimeout() {
        long timeoutMillis = TimeUnit.SECONDS.toMillis(timeout);
        if (startTime + timeoutMillis < Clock.currentTimeMillis()) {
            ExceptionUtil.sneakyThrow(new XAException(XAException.XA_RBTIMEOUT));
        }
    }

    private <T> T invoke(ClientRequest request) {
        SerializationService ss = client.getSerializationService();
        try {
            ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
            Future<SerializableList> future = clientInvocation.invoke();
            return ss.toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
