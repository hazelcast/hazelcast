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

package com.hazelcast.client.impl;

import com.hazelcast.client.Call;
import com.hazelcast.client.ClientRunnable;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.core.*;
import com.hazelcast.core.InstanceEvent.InstanceEventType;

import java.util.*;
import java.util.concurrent.*;

import static com.hazelcast.client.Serializer.toObject;
import static com.hazelcast.impl.BaseManager.getInstanceType;

public class ListenerManager extends ClientRunnable {
    final private Map<String, Map<Object, List<EntryListener<?, ?>>>> entryListeners = new ConcurrentHashMap<String, Map<Object, List<EntryListener<?, ?>>>>();
    final private Map<String, List<MessageListener<Object>>> messageListeners = new ConcurrentHashMap<String, List<MessageListener<Object>>>();
    final private List<InstanceListener> instanceListeners = new CopyOnWriteArrayList<InstanceListener>();
    final private List<MembershipListener> memberShipListeners = new CopyOnWriteArrayList<MembershipListener>();
    final private BlockingQueue<Call> listenerCalls = new LinkedBlockingQueue<Call>();
    final Map<ItemListener, EntryListener> itemListener2EntryListener = new ConcurrentHashMap<ItemListener, EntryListener>();
    final BlockingQueue<Packet> queue = new LinkedBlockingQueue<Packet>();
    final private HazelcastClient client;

    public ListenerManager(HazelcastClient hazelcastClient) {
        this.client = hazelcastClient;
    }

    public synchronized void registerEntryListener(String name, Object key, EntryListener<?, ?> entryListener) {
        if (!entryListeners.containsKey(name)) {
            entryListeners.put(name, new HashMap<Object, List<EntryListener<?, ?>>>());
        }
        if (!entryListeners.get(name).containsKey(key)) {
            entryListeners.get(name).put(key, new ArrayList<EntryListener<?, ?>>());
        }
        entryListeners.get(name).get(key).add(entryListener);
    }

    public synchronized <E, V> void registerItemListener(String name, final ItemListener<E> itemListener) {
        EntryListener<E, V> e = new EntryListener<E, V>() {
            public void entryAdded(EntryEvent<E, V> event) {
                itemListener.itemAdded((E) event.getKey());
            }

            public void entryEvicted(EntryEvent<E, V> event) {
                // TODO Auto-generated method stub
            }

            public void entryRemoved(EntryEvent<E, V> event) {
                itemListener.itemRemoved((E) event.getKey());
            }

            public void entryUpdated(EntryEvent<E, V> event) {
                // TODO Auto-generated method stub
            }
        };
        registerEntryListener(name, null, e);
        itemListener2EntryListener.put(itemListener, e);
    }

    public synchronized void registerMessageListener(String name, MessageListener messageListener) {
        if (!messageListeners.containsKey(name)) {
            messageListeners.put(name, new ArrayList<MessageListener<Object>>());
        }
        messageListeners.get(name).add(messageListener);
    }

    public synchronized void removeMessageListener(String name, MessageListener messageListener) {
        if (!messageListeners.containsKey(name)) {
            return;
        }
        messageListeners.get(name).remove(messageListener);
        if (messageListeners.get(name).size() == 0) {
            messageListeners.remove(name);
        }
    }

    public synchronized boolean noMessageListenerRegistered(String name) {
        if (!messageListeners.containsKey(name)) {
            return true;
        }
        return messageListeners.get(name).size() <= 0;
    }

    public synchronized boolean noEntryListenerRegistered(Object key, String name) {
        return !(entryListeners.get(name) != null &&
                entryListeners.get(name).get(key) != null &&
                entryListeners.get(name).get(key).size() > 0);
    }

    public synchronized boolean noInstanceListenerRegistered() {
        return instanceListeners.isEmpty();
    }

    public synchronized boolean noMembershipListenerRegistered() {
        return memberShipListeners.isEmpty();
    }

    public synchronized void removeEntryListener(String name, Object key, EntryListener<?, ?> entryListener) {
        Map<Object, List<EntryListener<?, ?>>> m = entryListeners.get(name);
        if (m != null) {
            List<EntryListener<?, ?>> list = m.get(key);
            if (list != null) {
                list.remove(entryListener);
                if (m.get(key).size() == 0) {
                    m.remove(key);
                }
            }
            if (m.size() == 0) {
                entryListeners.remove(name);
            }
        }
    }

    public synchronized void removeItemListener(String name, ItemListener itemListener) {
        EntryListener entryListener = itemListener2EntryListener.remove(itemListener);
        removeEntryListener(name, null, entryListener);
    }

    private void fireEntryEvent(EntryEvent event) {
        String name = event.getName();
        Object key = event.getKey();
        if (entryListeners.get(name) != null) {
            notifyEntryListeners(event, entryListeners.get(name).get(null));
            notifyEntryListeners(event, entryListeners.get(name).get(key));
        }
    }

    private void notifyEntryListeners(EntryEvent event, Collection<EntryListener<?, ?>> collection) {
        if (collection == null) {
            return;
        }
        for (Iterator<EntryListener<?, ?>> iterator = collection.iterator(); iterator.hasNext();) {
            EntryListener<?, ?> entryListener = iterator.next();
            if (event.getEventType().equals(EntryEventType.ADDED)) {
                entryListener.entryAdded(event);
            } else if (event.getEventType().equals(EntryEventType.REMOVED)) {
                entryListener.entryRemoved(event);
            } else if (event.getEventType().equals(EntryEventType.UPDATED)) {
                entryListener.entryUpdated(event);
            } else if (event.getEventType().equals(EntryEventType.EVICTED)) {
                entryListener.entryEvicted(event);
            }
        }
    }

    public void enqueue(Packet packet) {
        try {
            queue.put(packet);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void addListenerCall(Call call) {
        listenerCalls.add(call);
    }

    public BlockingQueue<Call> getListenerCalls() {
        return listenerCalls;
    }

    protected void customRun() throws InterruptedException {
        Packet packet = queue.poll(100, TimeUnit.MILLISECONDS);
        if (packet == null) {
            return;
        }
        if (packet.getName() == null) {
            Object eventType = toObject(packet.getValue());
            if (eventType instanceof InstanceEventType) {
                notifyInstanceListeners(packet);
            } else {
                notifyMembershipListeners(packet);
            }
        } else if (getInstanceType(packet.getName()).equals(Instance.InstanceType.TOPIC)) {
            notifyMessageListeners(packet);
        } else {
            notifyEventListeners(packet);
        }
    }

    private void notifyMembershipListeners(Packet packet) {
        Member member = (Member) toObject(packet.getKey());
        Integer type = (Integer) toObject(packet.getValue());
        MembershipEvent event = new MembershipEvent(client.getCluster(), member, type);
        if (type.equals(MembershipEvent.MEMBER_ADDED)) {
            for (MembershipListener membershipListener : memberShipListeners) {
                membershipListener.memberAdded(event);
            }
        } else {
            for (MembershipListener membershipListener : memberShipListeners) {
                membershipListener.memberRemoved(event);
            }
        }
    }

    private void notifyInstanceListeners(Packet packet) {
        String id = (String) toObject(packet.getKey());
        InstanceEventType instanceEventType = (InstanceEventType) toObject(packet.getValue());
        InstanceEvent event = new InstanceEvent(instanceEventType, (Instance) client.getClientProxy(id));
        for (Iterator<InstanceListener> it = instanceListeners.iterator(); it.hasNext();) {
            InstanceListener listener = it.next();
            if (InstanceEventType.CREATED.equals(event.getEventType())) {
                listener.instanceCreated(event);
            } else if (InstanceEventType.DESTROYED.equals(event.getEventType())) {
                listener.instanceDestroyed(event);
            }
        }
    }

    private void notifyEventListeners(Packet packet) {
        EntryEvent event = new EntryEvent(packet.getName(), (int) packet.getLongValue(), toObject(packet.getKey()), toObject(packet.getValue()));
        fireEntryEvent(event);
    }

    private void notifyMessageListeners(Packet packet) {
        List<MessageListener<Object>> list = messageListeners.get(packet.getName());
        if (list != null) {
            for (Iterator<MessageListener<Object>> it = list.iterator(); it.hasNext();) {
                MessageListener messageListener = it.next();
                messageListener.onMessage(toObject(packet.getKey()));
            }
        }
    }

    public void registerInstanceListener(InstanceListener listener) {
        this.instanceListeners.add(listener);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        this.instanceListeners.remove(instanceListener);
    }

    public void registerMembershipListener(MembershipListener listener) {
        this.memberShipListeners.add(listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        this.memberShipListeners.remove(listener);
    }
}
