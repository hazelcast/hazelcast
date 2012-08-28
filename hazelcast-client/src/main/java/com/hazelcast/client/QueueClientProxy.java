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

package com.hazelcast.client;

import com.hazelcast.client.impl.QueueItemListenerManager;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.IOUtil.toObject;
import static com.hazelcast.client.PacketProxyHelper.check;
import static com.hazelcast.client.PacketProxyHelper.checkTime;
import static com.hazelcast.nio.IOUtil.toData;

public class QueueClientProxy<E> extends AbstractQueue<E> implements IQueue<E> {
    final protected ProtocolProxyHelper protocolProxyHelper;
    final protected String name;

    final Object lock = new Object();

    public QueueClientProxy(HazelcastClient client, String name) {
        super();
        this.name = name;
        protocolProxyHelper = new ProtocolProxyHelper(name, client);
    }

    public String getName() {
        return name.substring(Prefix.QUEUE.length());
    }

    public InstanceType getInstanceType() {
        return InstanceType.QUEUE;
    }

    public void destroy() {
        protocolProxyHelper.doCommand(Command.DESTROY, new String[]{InstanceType.QUEUE.name(), getName()});
    }

    public Object getId() {
        return name;
    }

    @Override
    public String toString() {
        return "Queue{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public LocalQueueStats getLocalQueueStats() {
        throw new UnsupportedOperationException();
    }

    public boolean offer(E e) {
        check(e);
        return protocolProxyHelper.doCommandAssBoolean(Command.QOFFER, new String[]{getName()}, toData(e));
    }

    public E poll() {
        return (E) protocolProxyHelper.doCommandAsObject(Command.QPOLL, getName(), null);
    }

    public E peek() {
        return (E) protocolProxyHelper.doCommandAsObject(Command.QPEEK, getName(), null);
    }

    public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
        check(e);
        checkTime(l, timeUnit);
        l = (l < 0) ? 0 : l;
        if (e == null) {
            throw new NullPointerException();
        }
        return protocolProxyHelper.doCommandAssBoolean(Command.QOFFER, new String[]{getName(), String.valueOf(timeUnit.toMillis(l))}, toData(e));
    }

    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        checkTime(l, timeUnit);
        l = (l < 0) ? 0 : l;
        return (E) protocolProxyHelper.doCommandAsObject(Command.QPOLL, new String[]{getName(), String.valueOf(timeUnit.toMillis(l))}, null);
    }

    public E take() throws InterruptedException {
        return (E) protocolProxyHelper.doCommandAsObject(Command.QTAKE, getName(), null);
    }

    public void put(E e) throws InterruptedException {
        check(e);
        protocolProxyHelper.doCommand(Command.QPUT, getName(), toData(e));
    }

    public int remainingCapacity() {
        return protocolProxyHelper.doCommandAsInt(Command.QREMCAPACITY, new String[]{getName()}, null);
    }

    public int drainTo(Collection<? super E> objects) {
        return drainTo(objects, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<? super E> objects, int i) {
        if (objects == null) throw new NullPointerException("drainTo null!");
        if (i < 0) throw new IllegalArgumentException("Negative maxElements:" + i);
        if (i == 0) return 0;
        if (objects instanceof IQueue) {
            if (((IQueue) objects).getName().equals(getName())) {
                throw new IllegalArgumentException("Cannot drainTo self!");
            }
        }
        E e;
        int counter = 0;
        while (counter < i && (e = poll()) != null) {
            objects.add(e);
            counter++;
        }
        return counter;
    }

    @Override
    public int size() {
        return protocolProxyHelper.doCommandAsInt(Command.QSIZE, new String[]{getName()}, null);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IQueue && o != null) {
            return getName().equals(((IQueue) o).getName());
        } else {
            return false;
        }
    }

    @Override
    public boolean remove(Object o) {
        return protocolProxyHelper.doCommandAssBoolean(Command.QREMOVE, new String[]{getName()}, toData(o));
    }

    @Override
    public java.util.Iterator<E> iterator() {
        Protocol protocol = protocolProxyHelper.doCommand(Command.QENTRIES, new String[]{getName()}, null);
        List<E> list = new ArrayList<E>();
        if (protocol.hasBuffer()) {
            for (ByteBuffer bb : protocol.buffers) {
                list.add((E) toObject(new Data(bb.array())));
            }
        }
        return new QueueItemIterator(list.toArray(), this);
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
        synchronized (lock) {
            boolean shouldCall = listenerManager().noListenerRegistered(getName());
            listenerManager().registerListener(getName(), listener, includeValue);
            if (shouldCall) {
                protocolProxyHelper.doCommand(Command.QADDLISTENER, new String[]{getName(), String.valueOf(includeValue)}, null);
            }
        }
    }

    public void removeItemListener(ItemListener<E> listener) {
        check(listener);
        synchronized (lock) {
            listenerManager().removeListener(getName(), listener);
            protocolProxyHelper.doCommand(Command.QREMOVELISTENER, getName(), null);
        }
    }

    private QueueItemListenerManager listenerManager() {
        return protocolProxyHelper.client.getListenerManager().getQueueItemListenerManager();
    }
}
