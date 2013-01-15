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

import com.hazelcast.client.impl.QueueItemListenerManager;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.Keys;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.Data;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.IOUtil.toObject;
import static com.hazelcast.client.ProxyHelper.check;

public class QueueClientProxy<E> extends AbstractQueue<E> implements IQueue<E> {
    final protected ProxyHelper proxyHelper;
    final protected String name;

    final Object lock = new Object();

    public QueueClientProxy(HazelcastClient hazelcastClient, String name) {
        super();
        this.name = name;
        proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public String getName() {
        return name.substring(Prefix.QUEUE.length());
    }

    public InstanceType getInstanceType() {
        return InstanceType.QUEUE;
    }

    public void destroy() {
        proxyHelper.destroy();
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
        return innerOffer(e, 0);
    }

    public E poll() {
        return innerPoll(0);
    }

    public E peek() {
        return (E) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_PEEK, null, null);
    }

    public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
        check(e);
        ProxyHelper.checkTime(l, timeUnit);
        l = (l < 0) ? 0 : l;
        if (e == null) {
            throw new NullPointerException();
        }
        return innerOffer(e, timeUnit.toMillis(l));
    }

    private boolean innerOffer(E e, long millis) {
        return (Boolean) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_OFFER, e, millis);
    }

    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        ProxyHelper.checkTime(l, timeUnit);
        l = (l < 0) ? 0 : l;
        return innerPoll(timeUnit.toMillis(l));
    }

    private E innerPoll(long millis) {
        return (E) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_POLL, null, millis);
    }

    public E take() throws InterruptedException {
        return innerPoll(-1);
    }

    public void put(E e) throws InterruptedException {
        check(e);
        innerOffer(e, -1);
    }

    public int remainingCapacity() {
        return (Integer) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_REMAINING_CAPACITY, null, null);
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
        return (Integer) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_SIZE, null, null);
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
        return (Boolean) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_REMOVE, null, o);
    }

    @Override
    public java.util.Iterator<E> iterator() {
        Keys keys = (Keys) proxyHelper.doOp(ClusterOperation.BLOCKING_QUEUE_ENTRIES, null, null);
        List<E> list = new ArrayList<E>();
        for (Data d : keys) {
            list.add((E) toObject(d.buffer));
        }
        return new QueueItemIterator(list.toArray(), this);
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
        synchronized (lock) {
            boolean shouldCall = listenerManager().noListenerRegistered(name);
            listenerManager().registerListener(name, listener, includeValue);
            if (shouldCall) {
                Call c = listenerManager().createNewAddItemListenerCall(proxyHelper, includeValue);
                proxyHelper.doCall(c);
            }
        }
    }

    public void removeItemListener(ItemListener<E> listener) {
        check(listener);
        synchronized (lock) {
            listenerManager().removeListener(name, listener);
            Packet request = proxyHelper.createRequestPacket(ClusterOperation.REMOVE_LISTENER, null, null);
            Call c = proxyHelper.createCall(request);
            proxyHelper.doCall(c);
        }
    }

    private QueueItemListenerManager listenerManager() {
        return proxyHelper.getHazelcastClient().getListenerManager().getQueueItemListenerManager();
    }
}
