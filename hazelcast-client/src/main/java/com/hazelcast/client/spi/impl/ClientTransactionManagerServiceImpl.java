/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.proxy.txn.TransactionContextProxy;
import com.hazelcast.client.proxy.txn.xa.XATransactionContextProxy;
import com.hazelcast.client.spi.ClientTransactionManagerService;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import javax.transaction.xa.Xid;
import java.util.Set;

public class ClientTransactionManagerServiceImpl implements ClientTransactionManagerService {

    private static final int RETRY_COUNT = 20;

    final HazelcastClientInstanceImpl client;

    private final LoadBalancer loadBalancer;

    public ClientTransactionManagerServiceImpl(HazelcastClientInstanceImpl client, LoadBalancer loadBalancer) {
        this.client = client;
        this.loadBalancer = loadBalancer;
    }

    public HazelcastClientInstanceImpl getClient() {
        return client;
    }

    @Override
    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    @Override
    public TransactionContext newTransactionContext(TransactionOptions options) {
        return new TransactionContextProxy(this, options);
    }

    @Override
    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
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

    @Override
    public TransactionContext newXATransactionContext(Xid xid, int timeoutInSeconds) {
        return new XATransactionContextProxy(this, xid, timeoutInSeconds);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public String getGroupName() {
        final GroupConfig groupConfig = client.getClientConfig().getGroupConfig();
        if (groupConfig == null) {
            throw new RuntimeException("GroupConfig cannot be null client is participate in XA Transaction");
        }
        return groupConfig.getName();
    }

    public ClientConnection connect() throws Exception {
        Exception lastError = null;
        int count = 0;
        while (count < RETRY_COUNT) {
            try {
                final Address randomAddress = getRandomAddress();
                return (ClientConnection) client.getConnectionManager().getOrConnect(randomAddress);
            } catch (Exception e) {
                lastError = e;
            }
            count++;
        }
        throw lastError;
    }

    private Address getRandomAddress() {
        Member member = loadBalancer.next();
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
