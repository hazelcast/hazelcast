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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.listener.ItemEventLRH;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.client.util.QueueItemIterator;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.proxy.ProxyHelper.check;
import static com.hazelcast.client.proxy.ProxyHelper.checkTime;

public class QueueClientProxy<E> extends AbstractQueue<E> implements IQueue<E> {
    final protected ProxyHelper proxyHelper;
    final protected String name;
    final private HazelcastClient client;
    private Map<ItemListener, ListenerThread> listenerMap = new ConcurrentHashMap<ItemListener, ListenerThread>();

    public QueueClientProxy(HazelcastClient client, String name) {
        super();
        this.name = name;
        this.client = client;
        proxyHelper = new ProxyHelper(client);
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{QueueService.SERVICE_NAME, getName()});
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
        return proxyHelper.doCommandAsBoolean(Command.QOFFER, new String[]{getName()},
                proxyHelper.toData(e));
    }

    public E poll() {
        return (E) proxyHelper.doCommandAsObject(Command.QPOLL, new String[]{getName()});
    }

    public E peek() {
        return (E) proxyHelper.doCommandAsObject(Command.QPEEK, new String[]{getName()});
    }

    public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
        check(e);
        checkTime(l, timeUnit);
        l = (l < 0) ? 0 : l;
        if (e == null) {
            throw new NullPointerException();
        }
        String[] args = new String[]{getName(), String.valueOf(timeUnit.toMillis(l))};
        return proxyHelper.doCommandAsBoolean(Command.QOFFER, args, proxyHelper.toData(e));
    }

    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        checkTime(l, timeUnit);
        l = (l < 0) ? 0 : l;
        String[] args = new String[]{getName(), String.valueOf(timeUnit.toMillis(l))};
        return (E) proxyHelper.doCommandAsObject(Command.QPOLL, args);
    }

    public E take() throws InterruptedException {
        return (E) proxyHelper.doCommandAsObject(Command.QTAKE, new String[]{getName()});
    }

    public void put(E e) throws InterruptedException {
        check(e);
        proxyHelper.doCommand(Command.QPUT, new String[]{getName()}, proxyHelper.toData(e));
    }

    public int remainingCapacity() {
        return proxyHelper.doCommandAsInt(Command.QREMCAPACITY, new String[]{getName()});
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
        return proxyHelper.doCommandAsInt(Command.QSIZE, new String[]{getName()});
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
        return proxyHelper.doCommandAsBoolean(Command.QREMOVE, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public java.util.Iterator<E> iterator() {
        Protocol protocol = proxyHelper.doCommand(Command.QENTRIES, new String[]{getName()});
        List<E> list = new ArrayList<E>();
        if (protocol.hasBuffer()) {
            for (Data bb : protocol.buffers) {
                list.add((E) proxyHelper.toObject(bb));
            }
        }
        return new QueueItemIterator(list.toArray(), this);
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
        Protocol request = proxyHelper.createProtocol(Command.QLISTEN, new String[]{getName(), String.valueOf(includeValue)}, null);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.qListener.",
                client, request, new ItemEventLRH<E>(listener, includeValue, this));
        listenerMap.put(listener, thread);
        thread.start();
    }

    public void removeItemListener(ItemListener<E> listener) {
        ListenerThread thread = listenerMap.remove(listener);
        if (thread != null)
            thread.interrupt();
    }
}
