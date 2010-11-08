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

package com.hazelcast.client;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.monitor.LocalQueueStats;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.ProxyHelper.check;

public class QueueClientProxy<E> extends AbstractQueue<E> implements IQueue<E> {
    final protected ProxyHelper proxyHelper;
    final protected String name;

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
        throw new UnsupportedOperationException();
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
        while ((e = poll()) != null && counter < i) {
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
    public java.util.Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        throw new UnsupportedOperationException();
    }

    public void removeItemListener(ItemListener<E> eItemListener) {
        throw new UnsupportedOperationException();
    }
}
