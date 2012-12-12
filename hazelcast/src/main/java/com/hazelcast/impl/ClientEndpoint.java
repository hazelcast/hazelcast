/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.*;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.ConcurrentHashSet;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ClientEndpoint implements EntryListener, InstanceListener, MembershipListener, ConnectionListener, ClientHandlerService.ClientListener, Client {
    final Connection conn;
    final Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>(100);
    final Map<ITopic, MessageListener<Object>> messageListeners = new HashMap<ITopic, MessageListener<Object>>();
    final List<IMap> listeningMaps = new ArrayList<IMap>();
    final List<MultiMap> listeningMultiMaps = new ArrayList<MultiMap>();
    final List<Map.Entry<IMap, Object>> listeningKeysOfMaps = new ArrayList<Map.Entry<IMap, Object>>();
    final List<Map.Entry<MultiMap, Object>> listeningKeysOfMultiMaps = new ArrayList<Map.Entry<MultiMap, Object>>();
    final Map<IQueue, ItemListener<Object>> queueItemListeners = new ConcurrentHashMap<IQueue, ItemListener<Object>>();
    final Map<Long, DistributedTask> runningExecutorTasks = new ConcurrentHashMap<Long, DistributedTask>();
    final ConcurrentHashSet<ClientRequestHandler> currentRequests = new ConcurrentHashSet<ClientRequestHandler>();
    final Node node;
    final Map<String, AtomicInteger> attachedSemaphorePermits = new ConcurrentHashMap<String, AtomicInteger>();
    volatile boolean authenticated = false;

    LoginContext loginContext = null;

    ClientEndpoint(Node node, Connection conn) {
        this.node = node;
        this.conn = conn;
    }

    public CallContext getCallContext(int threadId) {
        CallContext context = callContexts.get(threadId);
        if (context == null) {
            int locallyMappedThreadId = ThreadContext.get().createNewThreadId();
            context = new CallContext(locallyMappedThreadId, true);
            callContexts.put(threadId, context);
        }
        return context;
    }

    public synchronized void addThisAsListener(IMap map, Data key, boolean includeValue) {
        if (!listeningMaps.contains(map) && !(listeningKeyExist(map, key))) {
            if (key == null){
                map.addEntryListener(this, includeValue);
            } else {
                map.addEntryListener(this, toObject(key), includeValue);
            }
        }
        if (key == null) {
            listeningMaps.add(map);
        } else {
            listeningKeysOfMaps.add(new Entry(map, key));
        }
    }

    public synchronized void removeThisListener(IMap map, Data key) {
        List<Map.Entry<IMap, Object>> entriesToRemove = new ArrayList<Map.Entry<IMap, Object>>();
        if (key == null) {
            listeningMaps.remove(map);
        } else {
            for (Map.Entry<IMap, Object> entry : listeningKeysOfMaps) {
                if (entry.getKey().equals(map) && entry.getValue().equals(key)) {
                    entriesToRemove.add(entry);
                    break;
                }
            }
        }
        listeningKeysOfMaps.removeAll(entriesToRemove);
        if (!listeningMaps.contains(map) && !(listeningKeyExist(map, key))) {
            map.removeEntryListener(this);
        }
    }

    public synchronized void addThisAsListener(MultiMap<Object, Object> multiMap, Data key, boolean includeValue) {
        if (!listeningMultiMaps.contains(multiMap) && !(listeningKeyExist(multiMap, key))) {
            multiMap.addEntryListener(this, includeValue);
        }
        if (key == null) {
            listeningMultiMaps.add(multiMap);
        } else {
            listeningKeysOfMultiMaps.add(new Entry(multiMap, key));
        }
    }

    public synchronized void removeThisListener(MultiMap multiMap, Data key) {
        List<Map.Entry<MultiMap, Object>> entriesToRemove = new ArrayList<Map.Entry<MultiMap, Object>>();
        if (key == null) {
            listeningMultiMaps.remove(multiMap);
        } else {
            for (Map.Entry<MultiMap, Object> entry : listeningKeysOfMultiMaps) {
                if (entry.getKey().equals(multiMap) && entry.getValue().equals(key)) {
                    entriesToRemove.add(entry);
                    break;
                }
            }
        }
        listeningKeysOfMultiMaps.removeAll(entriesToRemove);
        if (!listeningMultiMaps.contains(multiMap) && !(listeningKeyExist(multiMap, key))) {
            multiMap.removeEntryListener(this);
        }
    }

    private boolean listeningKeyExist(IMap map, Object key) {
        for (Map.Entry<IMap, Object> entry : listeningKeysOfMaps) {
            if (entry.getKey().equals(map) && (key == null || entry.getValue().equals(key))) {
                return true;
            }
        }
        return false;
    }

    private boolean listeningKeyExist(MultiMap map, Object key) {
        for (Map.Entry<MultiMap, Object> entry : listeningKeysOfMultiMaps) {
            if (entry.getKey().equals(map) && (key == null || entry.getValue().equals(key))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.conn.hashCode();
    }

    public void entryAdded(EntryEvent event) {
        processEvent(event);
    }

    public void entryEvicted(EntryEvent event) {
        processEvent(event);
    }

    public void entryRemoved(EntryEvent event) {
        processEvent(event);
    }

    public void entryUpdated(EntryEvent event) {
        processEvent(event);
    }

    public void instanceCreated(InstanceEvent event) {
        processEvent(event);
    }

    public void instanceDestroyed(InstanceEvent event) {
        processEvent(event);
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        processEvent(membershipEvent);
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        processEvent(membershipEvent);
    }

    private void processEvent(MembershipEvent membershipEvent) {
        Packet packet = createMembershipEventPacket(membershipEvent);
        sendPacket(packet);
    }

    private void processEvent(InstanceEvent event) {
        Packet packet = createInstanceEventPacket(event);
        sendPacket(packet);
    }

    /**
     * if a client is listening for both key and the entire
     * map, then we should make sure that we don't send
     * two separate events. One is enough. so check
     * if we already sent one.
     * <p/>
     * called by executor service threads
     *
     * @param event
     */
    private void processEvent(EntryEvent event) {
        Packet packet = createEntryEventPacket(event);
        sendPacket(packet);
    }

    void sendPacket(Packet packet) {
        if (conn != null && conn.live()) {
            conn.getWriteHandler().enqueueSocketWritable(packet);
        }
    }

    Packet createEntryEventPacket(EntryEvent event) {
        Packet packet = new Packet();
        final DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
        Data key = dataAwareEntryEvent.getKeyData();
        Data value = null;
        if (dataAwareEntryEvent.getNewValueData() != null) {
            Keys keys = new Keys();
            keys.add(dataAwareEntryEvent.getNewValueData());
            keys.add(dataAwareEntryEvent.getOldValueData());
            value = toData(keys);
        }
        String name = dataAwareEntryEvent.getLongName();
        if (name.startsWith(Prefix.MAP_OF_LIST)) {
            name = name.substring(Prefix.MAP_FOR_QUEUE.length());
            value = ((DataAwareEntryEvent) event).getNewValueData();
        } else if (name.startsWith(Prefix.SET)) {
            value = ((DataAwareEntryEvent) event).getKeyData();
            key = null;
        }
        packet.set(name, ClusterOperation.EVENT, key, value);
        packet.longValue = event.getEventType().getType();
        return packet;
    }

    Packet createInstanceEventPacket(InstanceEvent event) {
        Packet packet = new Packet();
        packet.set(null, ClusterOperation.EVENT, toData(event.getInstance().getId()), toData(event.getEventType().getId()));
        return packet;
    }

    Packet createMembershipEventPacket(MembershipEvent membershipEvent) {
        Packet packet = new Packet();
        packet.set(null, ClusterOperation.EVENT, toData(membershipEvent.getMember()), toData(membershipEvent.getEventType()));
        return packet;
    }

    public void connectionAdded(Connection connection) {
    }

    public void connectionRemoved(Connection connection) {
        LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) node.factory.getLifecycleService();
        if (connection.equals(this.conn) && !lifecycleService.paused.get()) {
            destroyEndpointThreads();
            rollbackTransactions();
            removeEntryListeners();
            removeEntryListenersWithKey();
            removeMessageListeners();
            cancelRunningOperations();
            releaseAttachedSemaphorePermits();
//            node.clusterManager.sendProcessableToAll(new ClientHandlerService.CountDownLatchLeave(conn.getEndPoint()), true);
            node.clusterManager.sendProcessableToAll(new ClientHandlerService.ClientDisconnect(node.address), true);
            node.clientService.remove(this);
        }
    }

    private void destroyEndpointThreads() {
        Set<Integer> threadIds = new HashSet<Integer>(callContexts.size());
        for (CallContext callContext : callContexts.values()) {
            threadIds.add(callContext.getThreadId());
        }
        Set<Member> allMembers = node.getClusterImpl().getMembers();
        MultiTask task = new MultiTask(new DestroyEndpointThreadsCallable(node.getThisAddress(), threadIds), allMembers);
        node.factory.getExecutorService().execute(task);
    }

    private void cancelRunningOperations() {
        for (ClientRequestHandler clientRequestHandler : currentRequests) {
            clientRequestHandler.cancel();
        }
        currentRequests.clear();
    }

    private void rollbackTransactions() {
        for (CallContext callContext : callContexts.values()) {
            ThreadContext.get().setCallContext(callContext);
            if (callContext.getTransaction() != null && callContext.getTransaction().getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                callContext.getTransaction().rollback();
            }
        }
    }

    private void removeMessageListeners() {
        for (ITopic topic : messageListeners.keySet()) {
            topic.removeMessageListener(messageListeners.get(topic));
        }
    }

    private void removeEntryListenersWithKey() {
        for (Map.Entry e : listeningKeysOfMaps) {
            IMap m = (IMap) e.getKey();
            m.removeEntryListener(this, e.getValue());
        }
    }

    private void removeEntryListeners() {
        for (IMap map : listeningMaps) {
            map.removeEntryListener(this);
        }
    }

    private void releaseAttachedSemaphorePermits() {
        for (Map.Entry<String, AtomicInteger> entry : attachedSemaphorePermits.entrySet()) {
            final ISemaphore semaphore = node.factory.getSemaphore(entry.getKey());
            final int permits = entry.getValue().get();
            if (permits > 0) {
                semaphore.releaseDetach(permits);
            } else {
                semaphore.reducePermits(permits);
                semaphore.attach(permits);
            }
        }
    }

    public void attachDetachPermits(String name, int permits) {
        if (attachedSemaphorePermits.containsKey(name)) {
            attachedSemaphorePermits.get(name).addAndGet(permits);
        } else {
            attachedSemaphorePermits.put(name, new AtomicInteger(permits));
        }
    }

    public void storeTask(long callId, DistributedTask task) {
        this.runningExecutorTasks.put(callId, task);
    }

    public void removeTask(long callId) {
        this.runningExecutorTasks.remove(callId);
    }

    public DistributedTask getTask(long taskId) {
        return this.runningExecutorTasks.get(taskId);
    }

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
        node.clientService.add(this);
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
