/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;

/**
 * Proxy implementation for the Queue.
 *
 * @param <E>
 */
public class QueueProxyImpl<E> extends QueueProxySupport<E> implements IQueue<E>, InitializingObject {

    public QueueProxyImpl(String name, QueueService queueService, NodeEngine nodeEngine, QueueConfig config) {
        super(name, queueService, nodeEngine, config);
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        return getService().createLocalQueueStats(name, partitionId);
    }

    @Override
    public boolean add(@Nonnull E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

    @Override
    public boolean offer(@Nonnull E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void put(@Nonnull E e) throws InterruptedException {
        offer(e, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean offer(@Nonnull E e,
                         long timeout, @Nonnull TimeUnit timeUnit) throws InterruptedException {
        checkNotNull(e, "Null item is not allowed!");
        checkNotNull(timeUnit, "Null timeUnit is not allowed!");

        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(e);
        return offerInternal(data, timeUnit.toMillis(timeout));
    }

    @Nonnull
    @Override
    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public E poll(long timeout, @Nonnull TimeUnit timeUnit) throws InterruptedException {
        checkNotNull(timeUnit, "Null timeUnit is not allowed!");

        final NodeEngine nodeEngine = getNodeEngine();
        final Object data = pollInternal(timeUnit.toMillis(timeout));
        return nodeEngine.toObject(data);
    }

    @Override
    public boolean remove(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed!");
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        return removeInternal(data);
    }

    @Override
    public boolean contains(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed!");
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        List<Data> dataSet = new ArrayList<>(1);
        dataSet.add(data);
        return containsInternal(dataSet);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> objects, int i) {
        checkNotNull(objects, "Null objects parameter is not allowed!");
        checkFalse(this.equals(objects), "Can not drain to same Queue");

        final NodeEngine nodeEngine = getNodeEngine();
        Collection<Data> dataList = drainInternal(i);
        for (Data data : dataList) {
            E e = nodeEngine.toObject(data);
            objects.add(e);
        }
        return dataList.size();
    }

    @Override
    public E remove() {
        final E res = poll();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    @Override
    public E poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
    }

    @Override
    public E element() {
        final E res = peek();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    @Override
    public E peek() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Object data = peekInternal();
        return nodeEngine.toObject(data);
    }

    @Override
    public Iterator<E> iterator() {
        final NodeEngine nodeEngine = getNodeEngine();
        return new QueueIterator<E>(listInternal().iterator(), nodeEngine.getSerializationService(), false);
    }

    @Override
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

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts) {
        checkNotNull(ts, "Null array parameter is not allowed!");

        T[] tsParam = ts;
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> list = listInternal();
        int size = list.size();
        if (tsParam.length < size) {
            tsParam = (T[]) java.lang.reflect.Array.newInstance(tsParam.getClass().getComponentType(), size);
        }
        for (int i = 0; i < size; i++) {
            tsParam[i] = nodeEngine.toObject(list.get(i));
        }
        return tsParam;
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> objects) {
        checkNotNull(objects, "Null collection is not allowed!");

        return containsInternal(getDataList(objects));
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> es) {
        checkNotNull(es, "Null collection is not allowed!");

        return addAllInternal(toDataList(es));
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> objects) {
        checkNotNull(objects, "Null collection is not allowed!");

        return compareAndRemove(getDataList(objects), false);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> objects) {
        checkNotNull(objects, "Null collection is not allowed!");

        return compareAndRemove(getDataList(objects), true);
    }

    private List<Data> getDataList(Collection<?> objects) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            dataList.add(nodeEngine.toData(o));
        }
        return dataList;
    }

    private List<Data> toDataList(Collection<?> objects) {
        final NodeEngine nodeEngine = getNodeEngine();
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            checkNotNull(o, "Object is null");
            dataList.add(nodeEngine.toData(o));
        }
        return dataList;
    }

    @Override
    public String toString() {
        return "IQueue{name='" + name + '\'' + '}';
    }
}
