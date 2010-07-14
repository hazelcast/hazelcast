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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ClientEndpoint implements EntryListener, InstanceListener, MembershipListener, ConnectionListener {
    final Connection conn;
    final private Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>();
    final ConcurrentMap<String, ConcurrentMap<Object, EntryEvent>> listeneds = new ConcurrentHashMap<String, ConcurrentMap<Object, EntryEvent>>();
    final Map<String, MessageListener<Object>> messageListeners = new HashMap<String, MessageListener<Object>>();
    final Map<Integer, Map<IMap, List<Data>>> locks = new ConcurrentHashMap<Integer, Map<IMap, List<Data>>>();

    ClientEndpoint(Connection conn) {
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

    public void addThisAsListener(IMap map, Data key, boolean includeValue) {
        if (key == null) {
            map.addEntryListener(this, includeValue);
        } else {
            map.addEntryListener(this, toObject(key), includeValue);
        }
    }

    private ConcurrentMap<Object, EntryEvent> getEventProcessedLog(String name) {
        ConcurrentMap<Object, EntryEvent> eventProcessedLog = listeneds.get(name);
        if (eventProcessedLog == null) {
            eventProcessedLog = new ConcurrentHashMap<Object, EntryEvent>();
            listeneds.putIfAbsent(name, eventProcessedLog);
        }
        return eventProcessedLog;
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
        final Object key = event.getKey();
        Map<Object, EntryEvent> eventProcessedLog = getEventProcessedLog(event.getName());
        if (eventProcessedLog.get(key) != null && eventProcessedLog.get(key) == event) {
            return;
        }
        eventProcessedLog.put(key, event);
        Packet packet = createEntryEventPacket(event);
        sendPacket(packet);
    }

    void sendPacket(Packet packet) {
        if (conn != null && conn.live()) {
            conn.getWriteHandler().enqueueSocketWritable(packet);
        }
    }

    private Packet createEntryEventPacket(EntryEvent event) {
        Packet packet = new Packet();
        BaseManager.EventTask eventTask = (BaseManager.EventTask) event;
        packet.set(event.getName(), ClusterOperation.EVENT, eventTask.getDataKey(), eventTask.getDataValue());
        packet.longValue = event.getEventType().getType();
        return packet;
    }

    private Packet createInstanceEventPacket(InstanceEvent event) {
        Packet packet = new Packet();
        packet.set(null, ClusterOperation.EVENT, toData(event.getInstance().getId()), toData(event.getEventType()));
        return packet;
    }

    private Packet createMembershipEventPacket(MembershipEvent membershipEvent) {
        Packet packet = new Packet();
        packet.set(null, ClusterOperation.EVENT, toData(membershipEvent.getMember()), toData(membershipEvent.getEventType()));
        return packet;
    }

    public void connectionAdded(Connection connection) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void connectionRemoved(Connection connection) {
        if (connection.equals(this.conn)) {
            for (Integer threadId : locks.keySet()) {
                ThreadContext.get().setCallContext(getCallContext(threadId));
                Map<IMap, List<Data>> mapOfLocks = locks.get(threadId);
                for (IMap map : mapOfLocks.keySet()) {
                    List<Data> list = mapOfLocks.get(map);
                    for (Data key : list) {

                        map.unlock(key);
                    }
                }
            }
        }
    }

    public void locked(IMap<Object, Object> map, Data keyData, int threadId) {
        Map<IMap, List<Data>> mapOfLocks;
        if (!locks.containsKey(threadId)) {
            mapOfLocks = new ConcurrentHashMap<IMap, List<Data>>();
            locks.put(threadId, mapOfLocks);
        } else {
            mapOfLocks = locks.get(threadId);
        }
        if (!mapOfLocks.containsKey(map)) {
            List<Data> list = new CopyOnWriteArrayList<Data>();
            mapOfLocks.put(map, list);
        }
        mapOfLocks.get(map).add(keyData);
    }

    public void unlocked(IMap<Object, Object> map, Data keyData, int threadId) {
        if (locks.containsKey(threadId)) {
            Map<IMap, List<Data>> mapOfLocks = locks.get(threadId);
            if (mapOfLocks.containsKey(map)) {
                List list = mapOfLocks.get(map);
                list.remove(keyData);
                if (list.size() == 0) {
                    mapOfLocks.remove(map);
                }
                if(locks.keySet().size() == 0){
                    locks.remove(threadId);
                }
            }
        }
    }
}
