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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.nio.IOUtil.toData;

public class ClientEndpoint implements EntryListener, InstanceListener, MembershipListener, ConnectionListener {
    final Connection conn;
    final private Map<Integer, CallContext> callContexts = new HashMap<Integer, CallContext>();
    final Map<ITopic, MessageListener<Object>> messageListeners = new HashMap<ITopic, MessageListener<Object>>();
    final Map<Integer, Map<IMap, List<Data>>> locks = new ConcurrentHashMap<Integer, Map<IMap, List<Data>>>();
    final List<IMap> listeningMaps = new ArrayList<IMap>();
    final List<Map.Entry<IMap, Object>> listeningKeysOfMaps = new ArrayList<Map.Entry<IMap, Object>>();
    public Map<IQueue, ItemListener<Object>> queueItemListeners = new HashMap<IQueue, ItemListener<Object>>();

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
        BaseManager.EventTask eventTask = (BaseManager.EventTask) event;
        packet.set(event.getName(), ClusterOperation.EVENT, eventTask.getDataKey(), eventTask.getDataValue());
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
            for (IMap map : listeningMaps) {
                map.removeEntryListener(this);
            }
            for (Map.Entry e : listeningKeysOfMaps) {
                IMap m = (IMap) e.getKey();
                m.removeEntryListener(this, e.getValue());
            }
            for (ITopic topic : messageListeners.keySet()) {
                topic.removeMessageListener(messageListeners.get(topic));
            }
        }
    }

    public void locked(IMap<Object, Object> map, Data keyData, int threadId) {
        if (!locks.containsKey(threadId)) {
            synchronized (locks) {
                if (!locks.containsKey(threadId)) {
                    Map<IMap, List<Data>> mapOfLocks = new ConcurrentHashMap<IMap, List<Data>>();
                    locks.put(threadId, mapOfLocks);
                }
            }
        }
        Map<IMap, List<Data>> mapOfLocks = locks.get(threadId);
        if (!mapOfLocks.containsKey(map)) {
            synchronized (locks.get(threadId)) {
                if (!mapOfLocks.containsKey(map)) {
                    List<Data> list = new CopyOnWriteArrayList<Data>();
                    mapOfLocks.put(map, list);
                }
            }
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
                if (locks.keySet().size() == 0) {
                    locks.remove(threadId);
                }
            }
        }
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
