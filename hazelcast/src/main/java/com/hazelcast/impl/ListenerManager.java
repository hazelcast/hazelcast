/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.core.*;
import static com.hazelcast.impl.ClusterOperation.ADD_LISTENER;
import com.hazelcast.nio.*;
import static com.hazelcast.nio.IOUtil.toData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

public class ListenerManager extends BaseManager {
    private List<ListenerItem> listeners = new CopyOnWriteArrayList<ListenerItem>();

    ListenerManager(Node node) {
        super(node);
        registerPacketProcessor(ClusterOperation.EVENT, new PacketProcessor() {
            public void process(Packet packet) {
                handleEvent(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.ADD_LISTENER, new PacketProcessor() {
            public void process(Packet packet) {
                handleAddRemoveListener(true, packet);
            }
        });
        registerPacketProcessor(ClusterOperation.REMOVE_LISTENER, new PacketProcessor() {
            public void process(Packet packet) {
                handleAddRemoveListener(false, packet);
            }
        });
    }

    private void handleEvent(Packet packet) {
        int eventType = (int) packet.longValue;
        Data key = IOUtil.doTake(packet.key);
        Data value = IOUtil.doTake(packet.value);
        String name = packet.name;
        Address from = packet.conn.getEndPoint();
        packet.returnToContainer();
        enqueueEvent(eventType, name, key, value, from);
    }

    private void handleAddRemoveListener(boolean add, Packet packet) {
        Data key = (packet.key != null) ? IOUtil.doTake(packet.key) : null;
        boolean returnValue = (packet.longValue == 1);
        String name = packet.name;
        Address address = packet.conn.getEndPoint();
        packet.returnToContainer();
        handleListenerRegisterations(add, name, key, address, returnValue);
    }

    public void syncForDead(Address deadAddress) {
        syncForAdd();
    }

    public void syncForAdd() {
        for (ListenerItem listenerItem : listeners) {
            registerListener(listenerItem.name, listenerItem.key, true, listenerItem.includeValue);
        }
    }

    public void syncForAdd(Address newAddress) {
        for (ListenerItem listenerItem : listeners) {
            Data dataKey = null;
            if (listenerItem.key != null) {
                dataKey = ThreadContext.get().toData(listenerItem.key);
            }
            sendAddRemoveListener(newAddress, true, listenerItem.name, dataKey, listenerItem.includeValue);
        }
    }

    class AddRemoveListenerOperationHandler extends TargetAwareOperationHandler {
        boolean isRightRemoteTarget(Packet packet) {
            return (null == packet.key) || thisAddress.equals(getKeyOwner(packet.key));
        }

        void doOperation(Request request) {
            Address from = request.caller;
            logger.log(Level.FINEST, "AddListnerOperation from " + from + ", local=" + request.local);
            if (from == null) throw new RuntimeException("Listener origin is not known!");
            boolean add = (request.operation == ADD_LISTENER);
            boolean includeValue = (request.longValue == 1);
            handleListenerRegisterations(add, request.name, request.key, request.caller, includeValue);
            request.response = Boolean.TRUE;
        }
    }

    public class AddRemoveListener extends MultiCall {
        final String name;
        final boolean includeValue;

        public AddRemoveListener(String name, boolean includeValue) {
            this.name = name;
            this.includeValue = includeValue;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new AddListenerAtTarget(target);
        }

        boolean onResponse(Object response) {
            return true;
        }

        Object returnResult() {
            return Boolean.TRUE;
        }

        class AddListenerAtTarget extends MigrationAwareTargettedCall {
            public AddListenerAtTarget(Address target) {
                this.target = target;
                request.reset();
                setLocal(ADD_LISTENER, name);
                request.setBooleanRequest();
                request.longValue = (includeValue) ? 1 : 0;
            }
        }
    }

    private void registerListener(String name, Object key, boolean add, boolean includeValue) {
        Data dataKey = null;
        if (key != null) {
            dataKey = ThreadContext.get().toData(key);
        }
        enqueueAndReturn(new ListenerRegistrationProcess(name, dataKey, add, includeValue));
    }

    class ListenerRegistrationProcess implements Processable {
        final String name;
        final Data key;
        boolean add = true;
        ClusterOperation packetProcess = ClusterOperation.ADD_LISTENER;
        boolean includeValue = true;

        public ListenerRegistrationProcess(String name, Data key, boolean add, boolean includeValue) {
            super();
            this.key = key;
            this.name = name;
            this.add = add;
            this.includeValue = includeValue;
            if (!add)
                packetProcess = ClusterOperation.REMOVE_LISTENER;
        }

        public void process() {
            if (key != null) {
                Address owner = node.concurrentMapManager.getKeyOwner(key);
                if (owner.equals(thisAddress)) {
                    handleListenerRegisterations(add, name, key, thisAddress, includeValue);
                } else {
                    Packet packet = obtainPacket();
                    packet.set(name, packetProcess, key, null);
                    packet.longValue = (includeValue) ? 1 : 0;
                    boolean sent = send(packet, owner);
                    if (!sent) {
                        packet.returnToContainer();
                    }
                }
            } else {
                for (MemberImpl member : lsMembers) {
                    if (member.localMember()) {
                        handleListenerRegisterations(add, name, key, thisAddress, includeValue);
                    } else {
                        sendAddRemoveListener(member.getAddress(), add, name, key, includeValue);
                    }
                }
            }
        }
    }

    public void collectInitialProcess(List<AbstractRemotelyProcessable> lsProcessables) {
        for (ListenerItem listenerItem : listeners) {
            lsProcessables.add(listenerItem);
        }
    }

    void sendAddRemoveListener(Address toAddress, boolean add, String name, Data key,
                               boolean includeValue) {
        Packet packet = obtainPacket();
        packet.set(name, (add) ? ClusterOperation.ADD_LISTENER : ClusterOperation.REMOVE_LISTENER, key, null);
        packet.longValue = (includeValue) ? 1 : 0;
        boolean sent = send(packet, toAddress);
        if (!sent) {
            packet.returnToContainer();
        }
    }

    public void addListener(String name, Object listener, Object key, boolean includeValue,
                            Instance.InstanceType instanceType) {
        addListener(name, listener, key, includeValue, instanceType, true);
    }

    synchronized void addListener(String name, Object listener, Object key, boolean includeValue,
                                  Instance.InstanceType instanceType, boolean shouldRemotelyRegister) {
        /**
         * check if already registered send this address to the key owner as a
         * listener add this listener to the local listeners map
         */
        if (shouldRemotelyRegister) {
            boolean remotelyRegister = true;
            for (ListenerItem listenerItem : listeners) {
                if (remotelyRegister) {
                    if (listenerItem.listener == listener) {
                        if (listenerItem.name.equals(name)) {
                            if (key == null) {
                                if (listenerItem.key == null) {
                                    if (!includeValue || listenerItem.includeValue == includeValue) {
                                        remotelyRegister = false;
                                    }
                                }
                            } else {
                                if (listenerItem.key != null) {
                                    if (listenerItem.key.equals(key)) {
                                        if (!includeValue
                                                || listenerItem.includeValue == includeValue) {
                                            remotelyRegister = false;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (remotelyRegister) {
                registerListener(name, key, true, includeValue);
            }
        }
        ListenerItem listenerItem = new ListenerItem(name, key, listener, includeValue,
                instanceType);
        listeners.add(listenerItem);
    }

    public synchronized void removeListener(String name, Object listener, Object key) {
        /**
         * send this address to the key owner as a listener add this listener to
         * the local listeners map
         */
        Iterator<ListenerItem> it = listeners.iterator();
        for (; it.hasNext();) {
            ListenerItem listenerItem = it.next();
            if (listener == listenerItem.listener) {
                if (key == null) {
                    if (listenerItem.key == null) {
                        registerListener(name, null, false, false);
                        listeners.remove(listenerItem);
                    }
                } else if (key.equals(listenerItem.key)) {
                    registerListener(name, key, false, false);
                    listeners.remove(listenerItem);
                }
            }
        }
    }

    void callListeners(EventTask event) {
        for (ListenerItem listenerItem : listeners) {
            if (listenerItem.listens(event)) {
                callListener(listenerItem, event);
            }
        }
    }

    private void callListener(ListenerItem listenerItem, EntryEvent event) {
        Object listener = listenerItem.listener;
        if (listenerItem.instanceType == Instance.InstanceType.MAP || listenerItem.instanceType == Instance.InstanceType.MULTIMAP) {
            EntryListener l = (EntryListener) listener;
            if (event.getEventType() == EntryEvent.EntryEventType.ADDED) {
                l.entryAdded(event);
            } else if (event.getEventType() == EntryEvent.EntryEventType.REMOVED) {
                l.entryRemoved(event);
            } else if (event.getEventType() == EntryEvent.EntryEventType.UPDATED) {
                l.entryUpdated(event);
            } else if (event.getEventType() == EntryEvent.EntryEventType.EVICTED) {
                l.entryEvicted(event);
            }
        } else if (listenerItem.instanceType == Instance.InstanceType.SET || listenerItem.instanceType == Instance.InstanceType.LIST) {
            ItemListener l = (ItemListener) listener;
            if (event.getEventType() == EntryEvent.EntryEventType.ADDED)
                l.itemAdded(event.getKey());
            else if (event.getEventType() == EntryEvent.EntryEventType.REMOVED)
                l.itemRemoved(event.getKey());
        } else if (listenerItem.instanceType == Instance.InstanceType.TOPIC) {
            MessageListener l = (MessageListener) listener;
            l.onMessage(event.getValue());
        } else if (listenerItem.instanceType == Instance.InstanceType.QUEUE) {
            ItemListener l = (ItemListener) listener;
            if (event.getEventType() == EntryEvent.EntryEventType.ADDED)
                l.itemAdded(event.getValue());
            else if (event.getEventType() == EntryEvent.EntryEventType.REMOVED)
                l.itemRemoved(event.getValue());
        }
    }

    public static class ListenerItem extends AbstractRemotelyProcessable implements DataSerializable {
        public String name;
        public Object key;
        public Object listener;
        public boolean includeValue;
        public Instance.InstanceType instanceType;

        public ListenerItem() {
        }

        public ListenerItem(String name, Object key, Object listener, boolean includeValue,
                            Instance.InstanceType instanceType) {
            super();
            this.key = key;
            this.listener = listener;
            this.name = name;
            this.includeValue = includeValue;
            this.instanceType = instanceType;
        }

        public boolean listens(EventTask event) {
            String name = event.getName();
            return this.name.equals(name) && (this.key == null || event.getKey().equals(this.key));
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
            writeObject(out, key);
            out.writeBoolean(includeValue);
        }

        public void readData(DataInput in) throws IOException {
            name = in.readUTF();
            key = readObject(in);
            includeValue = in.readBoolean();
        }

        public void process() {
            getNode().listenerManager.handleListenerRegisterations(true, name, toData(key), getConnection().getEndPoint(), includeValue);
        }
    }
}
