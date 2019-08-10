/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.proxy.txn.TransactionContextProxy;
import com.hazelcast.client.impl.proxy.txn.xa.XATransactionContextProxy;
import com.hazelcast.client.impl.spi.ClientTransactionManagerService;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cluster.Address;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.timeToString;

public class ClientTransactionManagerServiceImpl implements ClientTransactionManagerService {

    private final HazelcastClientInstanceImpl client;
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
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return new TransactionContextProxy(this, options);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        checkNotNull(options, "TransactionOptions must not be null!");
        checkNotNull(task, "TransactionalTask is required!");
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

    public void shutdown() {
    }

    @Override
    public String getClusterName() {
        return client.getClientConfig().getClusterName();
    }

    public ClientConnection connect() throws Exception {
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) client.getInvocationService();
        long startTimeMillis = System.currentTimeMillis();
        long invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();
        ClientConfig clientConfig = client.getClientConfig();
        boolean smartRouting = clientConfig.getNetworkConfig().isSmartRouting();

        while (client.getLifecycleService().isRunning()) {
            try {
                if (smartRouting) {
                    return tryConnectSmart();
                } else {
                    return tryConnectUnisocket();
                }
            } catch (Exception e) {
                if (e instanceof HazelcastClientOfflineException) {
                    throw e;
                }
                if (System.currentTimeMillis() - startTimeMillis > invocationTimeoutMillis) {
                    throw newOperationTimeoutException(e, invocationTimeoutMillis, startTimeMillis);
                }
            }
            Thread.sleep(invocationService.getInvocationRetryPauseMillis());
        }
        throw new HazelcastClientNotActiveException("Client is shutdown");
    }

    private Exception newOperationTimeoutException(Throwable e, long invocationTimeoutMillis, long startTimeMillis) {
        StringBuilder sb = new StringBuilder();
        sb.append("Creating transaction context timed out because exception occurred after client invocation timeout ");
        sb.append(invocationTimeoutMillis).append(" ms. ");
        sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
        sb.append("Start time: ").append(timeToString(startTimeMillis)).append(". ");
        sb.append("Total elapsed time: ").append(currentTimeMillis() - startTimeMillis).append(" ms. ");
        String msg = sb.toString();
        return new OperationTimeoutException(msg, e);
    }

    private ClientConnection tryConnectUnisocket() {
        Iterator<ClientConnection> iterator = client.getConnectionManager().getActiveConnections().iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return throwException(false);
    }

    private ClientConnection throwException(boolean smartRouting) {
        ClientConfig clientConfig = client.getClientConfig();
        ClientConnectionStrategyConfig connectionStrategyConfig = clientConfig.getConnectionStrategyConfig();
        ClientConnectionStrategyConfig.ReconnectMode reconnectMode = connectionStrategyConfig.getReconnectMode();
        if (reconnectMode.equals(ClientConnectionStrategyConfig.ReconnectMode.ASYNC)) {
            throw new HazelcastClientOfflineException("Hazelcast client is offline");
        }
        if (smartRouting) {
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
        throw new IllegalStateException("No active connection is found");
    }

    private ClientConnection tryConnectSmart() throws IOException {
        Address address = getRandomAddress();
        if (address == null) {
            throwException(true);
        }
        return (ClientConnection) client.getConnectionManager().getOrConnect(address);
    }

    private Address getRandomAddress() {
        Member member = loadBalancer.next();
        if (member == null) {
            return null;
        }
        return member.getAddress();
    }

}
