/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.core.*;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.ConcurrentHashSet;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.toData;

public class ClientEndpoint implements EntryListener, InstanceListener, MembershipListener, ConnectionListener, ClientService.ClientListener {
    final Connection conn;
    final Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>(100);
    final Map<ITopic, MessageListener<Object>> messageListeners = new HashMap<ITopic, MessageListener<Object>>();
    final List<IMap> listeningMaps = new ArrayList<IMap>();
    final List<Map.Entry<IMap, Object>> listeningKeysOfMaps = new ArrayList<Map.Entry<IMap, Object>>();
    final Map<IQueue, ItemListener<Object>> queueItemListeners = new ConcurrentHashMap<IQueue, ItemListener<Object>>();
    final Map<Long, DistributedTask> runningExecutorTasks = new ConcurrentHashMap<Long, DistributedTask>();
    final ConcurrentHashSet<ClientRequestHandler> currentRequests = new ConcurrentHashSet<ClientRequestHandler>();
    final Node node;
    final Map<String, AtomicInteger> attachedSemaphorePermits = new ConcurrentHashMap<String, AtomicInteger>();

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
            map.addEntryListener(this, includeValue);
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

    private boolean listeningKeyExist(IMap map, Object key) {
        for (Map.Entry<IMap, Object> entry : listeningKeysOfMaps) {
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
        DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
        Data valueEvent = null;
        if (dataAwareEntryEvent.getNewValueData() != null) {
            Keys keys = new Keys();
            keys.add(dataAwareEntryEvent.getNewValueData());
            keys.add(dataAwareEntryEvent.getOldValueData());
            valueEvent = toData(keys);
        }
        String name = event.getName();
        if (name.startsWith(Prefix.MAP_OF_LIST)) {
            name = name.substring(Prefix.MAP_FOR_QUEUE.length());
            valueEvent = ((DataAwareEntryEvent) event).getNewValueData();
        }
        packet.set(name, ClusterOperation.EVENT, dataAwareEntryEvent.getKeyData(), valueEvent);
        packet.longValue = event.getEventType().getType();
        return packet;
    }

    Packet createInstanceEventPacket(InstanceEvent event) {
        Packet packet = new Packet();
        packet.set(null, ClusterOperation.EVENT, toData(event.getInstance().getId()), toData(event.getEventType()));
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
            node.clusterManager.sendProcessableToAll(new ClientService.CountDownLatchLeave(conn.getEndPoint()), true);
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
        for (Map.Entry<String, AtomicInteger> entry : attachedSemaphorePermits.entrySet()){
            final ISemaphore semaphore = node.factory.getSemaphore(entry.getKey());
            final int permits = entry.getValue().get();
            if(permits>0){
                semaphore.releaseDetach(permits);
            } else {
                semaphore.reducePermits(permits);
                semaphore.attach(permits);
            }
        }
    }

    public void attachDetachPermits(String name, int permits) {
        if (attachedSemaphorePermits.containsKey(name)){
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
