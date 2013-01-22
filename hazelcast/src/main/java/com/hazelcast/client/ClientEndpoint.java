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

package com.hazelcast.client;

import com.hazelcast.core.*;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.TcpIpConnection;
import com.hazelcast.spi.Connection;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientEndpoint implements ConnectionListener, Client {
    final TcpIpConnection conn;
//    final Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>(100);
    final Map<ITopic, MessageListener<Object>> messageListeners = new HashMap<ITopic, MessageListener<Object>>();
    final List<IMap> listeningMaps = new ArrayList<IMap>();
    final List<MultiMap> listeningMultiMaps = new ArrayList<MultiMap>();
    final List<Map.Entry<IMap, Object>> listeningKeysOfMaps = new ArrayList<Map.Entry<IMap, Object>>();
    final List<Map.Entry<MultiMap, Object>> listeningKeysOfMultiMaps = new ArrayList<Map.Entry<MultiMap, Object>>();
    final Map<IQueue, ItemListener<Object>> queueItemListeners = new ConcurrentHashMap<IQueue, ItemListener<Object>>();
//    final Map<Long, DistributedTask> runningExecutorTasks = new ConcurrentHashMap<Long, DistributedTask>();
    final Set<ClientRequestHandler> currentRequests = Collections.newSetFromMap(new ConcurrentHashMap<ClientRequestHandler, Boolean>());
    final Node node;
    final Map<String, AtomicInteger> attachedSemaphorePermits = new ConcurrentHashMap<String, AtomicInteger>();
    volatile boolean authenticated = false;

    LoginContext loginContext = null;

    ClientEndpoint(Node node, TcpIpConnection conn) {
        this.node = node;
        this.conn = conn;
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

    void sendPacket(Packet packet) {
        if (conn != null && conn.live()) {
            conn.getWriteHandler().enqueueSocketWritable(packet);
        }
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
}
