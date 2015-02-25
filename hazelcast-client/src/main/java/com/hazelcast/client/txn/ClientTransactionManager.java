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

package com.hazelcast.client.txn;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.ClusterAuthenticator;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
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
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public class ClientTransactionManager {

    private static final int RETRY_COUNT = 20;
    final HazelcastClientInstanceImpl client;

    final ConcurrentMap<SerializableXID, TransactionProxy> managedTransactions =
            new ConcurrentHashMap<SerializableXID, TransactionProxy>();
    final ConcurrentMap<SerializableXID, ClientConnection> recoveredTransactions =
            new ConcurrentHashMap<SerializableXID, ClientConnection>();
    private final LoadBalancer loadBalancer;
    private final Credentials credentials;
    private final ClusterAuthenticator authenticator;

    public ClientTransactionManager(HazelcastClientInstanceImpl client, LoadBalancer loadBalancer) {
        this.client = client;
        this.loadBalancer = loadBalancer;
        credentials = client.getCredentials();
        authenticator = new ClusterAuthenticator(client, credentials);
    }

    public HazelcastClientInstanceImpl getClient() {
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

    ClientConnection connect() throws Exception {
        Exception lastError = null;
        int count = 0;
        while (count < RETRY_COUNT) {
            try {
                final Address randomAddress = getRandomAddress();
                return (ClientConnection) client.getConnectionManager().getOrConnect(randomAddress, authenticator);
            } catch (IOException e) {
                lastError = e;
            } catch (HazelcastInstanceNotActiveException e) {
                lastError = e;
            }
            count++;
        }
        throw lastError;
    }

    public Xid[] recover() {
        final SerializationService serializationService = client.getSerializationService();
        final Xid[] empty = new Xid[0];
        try {
            final ClientConnection connection;
            try {
                connection = connect();
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
                return empty;
            }
            final RecoverAllTransactionsRequest request = new RecoverAllTransactionsRequest();
            final ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
            final Future<SerializableCollection> future = clientInvocation.invoke();
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
        final RecoverTransactionRequest request = new RecoverTransactionRequest(sXid, commit);
        try {
            final ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
            final Future<SerializableCollection> future = clientInvocation.invoke();
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

    private Address getRandomAddress() {

        MemberImpl member = (MemberImpl) loadBalancer.next();
        if (member == null) {
            Set<Member> members = client.getCluster().getMembers();
            String msg;
            if (members.isEmpty()) {
                msg = "No address was return by the LoadBalancer since there are no members in the cluster";
            } else {
                msg = "No address was return by the LoadBalancer. "
                        + "But the cluster contains the following members:" + members;
            }
            throw new IllegalStateException(msg);
        }

        return member.getAddress();
    }


}
