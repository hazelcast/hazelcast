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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.impl.SerializableXid;
import com.hazelcast.util.ExceptionUtil;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ali 14/02/14
 */
public class ClientTransactionManager {

    static final int CONNECTION_TRY_COUNT = 5;

    final HazelcastClient client;

    final ConcurrentMap<SerializableXid, TransactionProxy> managedTransactions = new ConcurrentHashMap<SerializableXid, TransactionProxy>();
    final ConcurrentMap<SerializableXid, Connection> recoveredTransactions = new ConcurrentHashMap<SerializableXid, Connection>();

    public ClientTransactionManager(HazelcastClient client) {
        this.client = client;
    }

    public HazelcastClient getClient() {
        return client;
    }

    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextProxy(this, options);
    }

    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        final TransactionContext context = newTransactionContext(options);
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

    public void addManagedTransaction(Xid xid, TransactionProxy transaction) {
        final SerializableXid sXid = new SerializableXid(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        transaction.setXid(sXid);
        managedTransactions.put(sXid, transaction);
    }

    public TransactionProxy getManagedTransaction(Xid xid) {
        final SerializableXid sXid = new SerializableXid(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        return managedTransactions.get(sXid);
    }

    public void removeManagedTransaction(Xid xid) {
        final SerializableXid sXid = new SerializableXid(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        managedTransactions.remove(sXid);
    }

    Connection connect() {
        Connection conn = null;
        for (int i = 0; i < CONNECTION_TRY_COUNT; i++) {
            try {
                conn = client.getConnectionManager().getRandomConnection();
            } catch (IOException e) {
                continue;
            }
            if (conn != null) {
                break;
            }
        }
        return conn;
    }

    public Xid[] recover() {
        final SerializationService serializationService = client.getSerializationService();
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        final Xid[] empty = new Xid[0];
        try {
            final Connection connection = connect();
            if (connection == null) {
                return empty;
            }
            final RecoverAllTransactionsRequest request = new RecoverAllTransactionsRequest();
            final SerializableCollection collectionWrapper = clusterService.sendAndReceiveFixedConnection(connection, request);
            final ConnectionWrapper connectionWrapper = new ConnectionWrapper(connection, collectionWrapper.size());
            for (Data data : collectionWrapper) {
                final SerializableXid xid = (SerializableXid) serializationService.toObject(data);
                recoveredTransactions.put(xid, connectionWrapper);
            }

            final Set<SerializableXid> xidSet = recoveredTransactions.keySet();
            return xidSet.toArray(new Xid[xidSet.size()]);
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return empty;
    }

    public boolean recover(Xid xid, boolean commit) {
        final SerializableXid sXid = new SerializableXid(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        final Connection connection = recoveredTransactions.remove(sXid);
        if (connection == null) {
            return false;
        }
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        final RecoverTransactionRequest request = new RecoverTransactionRequest(sXid, commit);
        try {
            clusterService.sendAndReceiveFixedConnection(connection, request);
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        } finally {
            try {
                connection.release();
            } catch (IOException e) {
                Logger.getLogger(ClientTransactionManager.class).severe("Error during connection release", e);
            }
        }
        return true;
    }

    class ConnectionWrapper implements Connection {

        final Connection inner;
        final AtomicInteger counter;

        ConnectionWrapper(Connection inner, int size) {
            this.inner = inner;
            this.counter = new AtomicInteger(size);
        }

        @Override
        public Address getEndpoint() {
            return inner.getEndpoint();
        }

        @Override
        public boolean write(Data data) throws IOException {
            return inner.write(data);
        }

        @Override
        public Data read() throws IOException {
            return inner.read();
        }

        @Override
        public int getId() {
            return inner.getId();
        }

        @Override
        public long getLastReadTime() {
            return inner.getLastReadTime();
        }

        @Override
        public void release() throws IOException {
            if (counter.decrementAndGet() == 0) {
                inner.release();
            }
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }

        @Override
        public void setEndpoint(Address address) {
            inner.setEndpoint(address);
        }
    }
}
