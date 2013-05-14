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

package com.hazelcast.deprecated.client;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.nio.Connection;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClientEndpoint implements ConnectionListener, Client {
    final TcpIpConnection conn;
    final Set<ClientRequestHandler> currentRequests = Collections.newSetFromMap(new ConcurrentHashMap<ClientRequestHandler, Boolean>());
    final Node node;
    final String uuid;
    volatile boolean authenticated = false;
    LoginContext loginContext = null;

    ClientEndpoint(Node node, TcpIpConnection conn, String uuid) {
        this.node = node;
        this.conn = conn;
        this.uuid = uuid;
    }

//    public CallContext getCallContext(int threadId) {
//        CallContext context = callContexts.get(threadId);
//        if (context == null) {
//            int locallyMappedThreadId = ThreadContext.createNewThreadId();
//            context = new CallContext(locallyMappedThreadId, true);
//            callContexts.put(threadId, context);
//        }
//        return context;
//    }

    @Override
    public int hashCode() {
        return this.conn.hashCode();
    }

    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(Connection connection) {
//        LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) node.factory.getLifecycleService();
//        if (connection.equals(this.conn) && !lifecycleService.paused.get()) {
//            destroyEndpointThreads();
//            rollbackTransactions();
//            removeEntryListeners();
//            removeEntryListenersWithKey();
//            removeMessageListeners();
//            cancelRunningOperations();
//            releaseAttachedSemaphorePermits();
//            node.clusterImpl.sendProcessableToAll(new ClientHandlerService.CountDownLatchLeave(conn.getEndPoint()), true);
//            node.clientService.remove(this);
//        }
    }

    private void cancelRunningOperations() {
        for (ClientRequestHandler clientRequestHandler : currentRequests) {
            clientRequestHandler.cancel();
        }
        currentRequests.clear();
    }
//
//    private void rollbackTransactions() {
//        for (CallContext callContext : callContexts.values()) {
//            ThreadContext.get().setCallContext(callContext);
//            if (callContext.getTransaction() != null && callContext.getTransaction().getStatus() == Transaction.TXN_STATUS_ACTIVE) {
//                callContext.getTransaction().rollback();
//            }
//        }
//    }


    public void addRequest(ClientRequestHandler clientRequestHandler) {
        this.currentRequests.add(clientRequestHandler);
    }

    public void removeRequest(ClientRequestHandler clientRequestHandler) {
        this.currentRequests.remove(clientRequestHandler);
    }

    public void setLoginContext(LoginContext loginContext) {
        this.loginContext = loginContext;
    }

    public LoginContext getLoginContext() {
        return loginContext;
    }

    public Subject getSubject() {
        return loginContext != null ? loginContext.getSubject() : null;
    }

    public void authenticated() {
        this.authenticated = true;
//        node.clientService.add(this);
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public SocketAddress getSocketAddress() {
        return conn.getSocketChannelWrapper().socket().getRemoteSocketAddress();
    }

    public ClientType getClientType() {
        return ClientType.Native;
    }

    static class Entry implements Map.Entry {
        Object key;
        Object value;

        Entry(Object k, Object v) {
            key = k;
            value = v;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Object setValue(Object value) {
            Object r = key;
            key = value;
            return r;
        }
    }

    public String getUuid() {
        return null;
    }
}
