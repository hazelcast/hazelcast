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

package com.hazelcast.client.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.client.RecoverAllTransactionsRequest;
import com.hazelcast.transaction.client.RecoverTransactionRequest;
import com.hazelcast.transaction.impl.SerializableXID;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import javax.transaction.xa.Xid;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author ali 14/02/14
 */
public class ClientTransactionManager {

    final HazelcastClient client;

    final ConcurrentMap<SerializableXID, TransactionProxy> managedTransactions =
            new ConcurrentHashMap<SerializableXID, TransactionProxy>();
    final ConcurrentMap<SerializableXID, ClientConnection> recoveredTransactions =
            new ConcurrentHashMap<SerializableXID, ClientConnection>();

    public ClientTransactionManager(HazelcastClient client) {
        this.client = client;
    }

    public HazelcastClient getClient() {
        return client;
    }

    public String getGroupName() {
        final GroupConfig groupConfig = client.getClientConfig().getGroupConfig();
        if (groupConfig == null) {
            throw new RuntimeException("GroupConfig cannot be null client is participate in XA Transaction");
        }
        return groupConfig.getName();
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
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        transaction.setXid(sXid);
        managedTransactions.put(sXid, transaction);
    }

    public TransactionProxy getManagedTransaction(Xid xid) {
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        return managedTransactions.get(sXid);
    }

    public void removeManagedTransaction(Xid xid) {
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        managedTransactions.remove(sXid);
    }

    ClientConnection connect() {
        try {
            return client.getConnectionManager().tryToConnect(null);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
        return null;
    }

    public Xid[] recover() {
        final SerializationService serializationService = client.getSerializationService();
        final ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        final Xid[] empty = new Xid[0];
        try {
            final ClientConnection connection = connect();
            if (connection == null) {
                return empty;
            }
            final RecoverAllTransactionsRequest request = new RecoverAllTransactionsRequest();
            final ICompletableFuture<SerializableCollection> future = invocationService.send(request, connection);
            final SerializableCollection collectionWrapper = serializationService.toObject(future.get());

            for (Data data : collectionWrapper) {
                final SerializableXID xid = serializationService.toObject(data);
                recoveredTransactions.put(xid, connection);
            }

            final Set<SerializableXID> xidSet = recoveredTransactions.keySet();
            return xidSet.toArray(new Xid[xidSet.size()]);
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return empty;
    }

    public boolean recover(Xid xid, boolean commit) {
        final SerializableXID sXid = new SerializableXID(xid.getFormatId(),
                xid.getGlobalTransactionId(), xid.getBranchQualifier());
        final ClientConnection connection = recoveredTransactions.remove(sXid);
        if (connection == null) {
            return false;
        }
        final ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        final RecoverTransactionRequest request = new RecoverTransactionRequest(sXid, commit);
        try {
            final ICompletableFuture future = invocationService.send(request, connection);
            future.get();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return true;
    }

    public void shutdown() {
        managedTransactions.clear();
        recoveredTransactions.clear();
    }

}
