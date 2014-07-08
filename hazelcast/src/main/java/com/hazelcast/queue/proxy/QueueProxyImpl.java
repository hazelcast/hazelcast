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

package com.hazelcast.queue.proxy;

import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 13:23 AM
 */
public class QueueProxyImpl<E> extends QueueProxySupport implements IQueue<E>, InitializingObject {

    public QueueProxyImpl(String name, QueueService queueService, NodeEngine nodeEngine) {
        super(name, queueService, nodeEngine);
    }

    public LocalQueueStats getLocalQueueStats() {
        return getService().createLocalQueueStats(name, partitionId);
    }

    public boolean add(E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    public void put(E e) throws InterruptedException {
        offer(e, -1, TimeUnit.MILLISECONDS);
    }

    public boolean offer(E e, long timeout, TimeUnit timeUnit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(e);
        return offerInternal(data, timeUnit.toMillis(timeout));
    }

    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Object data = pollInternal(timeUnit.toMillis(timeout));
        return nodeEngine.toObject(data);
    }

    public int remainingCapacity() {
        return config.getMaxSize() - size();
    }

    public boolean remove(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        return removeInternal(data);
    }

    public boolean contains(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        List<Data> dataSet = new ArrayList<Data>(1);
        dataSet.add(data);
        return containsInternal(dataSet);
    }

    public int drainTo(Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    public int drainTo(Collection<? super E> objects, int i) {
        final NodeEngine nodeEngine = getNodeEngine();
        if (this.equals(objects)) {
            throw new IllegalArgumentException("Can not drain to same Queue");
        }
        Collection<Data> dataList = drainInternal(i);
        for (Data data : dataList) {
            E e = nodeEngine.toObject(data);
            objects.add(e);
        }
        return dataList.size();
    }

    public E remove() {
        final E res = poll();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }


    public E poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public E element() {
        final E res = peek();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public E peek() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Object data = peekInternal();
        return nodeEngine.toObject(data);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Iterator<E> iterator() {
        final NodeEngine nodeEngine = getNodeEngine();
        return new QueueIterator<E>(listInternal().iterator(), nodeEngine, partitionId, name, false);
    }

    public Object[] toArray() {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> list = listInternal();
        int size = list.size();
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = nodeEngine.toObject(list.get(i));
        }
        return array;
    }

    public <T> T[] toArray(T[] ts) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> list = listInternal();
        int size = list.size();
        if (ts.length < size) {
            ts = (T[]) java.lang.reflect.Array.newInstance(ts.getClass().getComponentType(), size);
        }
        for (int i = 0; i < size; i++) {
            ts[i] = nodeEngine.toObject(list.get(i));
        }
        return ts;
    }

    public boolean containsAll(Collection<?> objects) {
        return containsInternal(getDataList(objects));
    }

    public boolean addAll(Collection<? extends E> es) {
        return addAllInternal(getDataList(es));
    }

    public boolean removeAll(Collection<?> objects) {
        return compareAndRemove(getDataList(objects), false);
    }

    public boolean retainAll(Collection<?> objects) {
        return compareAndRemove(getDataList(objects), true);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("IQueue");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }

    private List<Data> getDataList(Collection<?> objects) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            dataList.add(nodeEngine.toData(o));
        }
        return dataList;
    }

}
