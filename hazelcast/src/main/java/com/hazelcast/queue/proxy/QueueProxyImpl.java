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

import com.hazelcast.core.AsyncQueue;
import com.hazelcast.core.CompletionFuture;
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
public class QueueProxyImpl<E> extends QueueProxySupport implements AsyncQueue<E>, InitializingObject {

    public QueueProxyImpl(String name, QueueService queueService, NodeEngine nodeEngine) {
        super(name, queueService, nodeEngine);
    }

    @Override
    public CompletionFuture<Integer> asyncSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionFuture<Void> asyncClear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        return getService().createLocalQueueStats(name, partitionId);
    }

    @Override
    public boolean add(E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

    @Override
    public CompletionFuture<Boolean> asyncAdd(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    @Override
    public CompletionFuture<Boolean> asyncOffer(E e) {
        throw new UnsupportedOperationException();
    }

     @Override
    public void put(E e) throws InterruptedException {
        offer(e, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletionFuture<Void> asyncPut(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit timeUnit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(e);
        return offerInternal(data, timeUnit.toMillis(timeout));
    }

    @Override
    public CompletionFuture<Boolean> asyncOffer(E e, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletionFuture<E> asyncTake() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        final Object data = pollInternal(timeUnit.toMillis(timeout));
        return nodeEngine.toObject(data);
    }

    @Override
    public CompletionFuture<E> asyncPoll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        return config.getMaxSize() - size();
    }

    @Override
    public CompletionFuture<Integer> asyncRemainingCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        return removeInternal(data);
    }

    @Override
    public CompletionFuture<E> asyncRemove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data data = nodeEngine.toData(o);
        List<Data> dataSet = new ArrayList<Data>(1);
        dataSet.add(data);
        return containsInternal(dataSet);
    }

    @Override
    public CompletionFuture<Boolean> asyncContains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    @Override
    public CompletionFuture<Integer> asyncDrainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
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

    @Override
    public CompletionFuture<Integer> asyncDrainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
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
    public CompletionFuture<Boolean> asyncRemove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public CompletionFuture<E> asyncPoll() {
        throw new UnsupportedOperationException();
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
    public CompletionFuture<E> asyncElement() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E peek() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Object data = peekInternal();
        return nodeEngine.toObject(data);
    }

    @Override
    public CompletionFuture<E> asyncPeek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public CompletionFuture<Boolean> asyncIsEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        final NodeEngine nodeEngine = getNodeEngine();
        return new QueueIterator<E>(listInternal().iterator(), nodeEngine.getSerializationService(), false);
    }

    @Override
    public CompletionFuture<Iterator<E>> asyncIterator() {
        throw new UnsupportedOperationException();
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

    @Override
    public CompletionFuture<Object[]> asyncToArray() {
        throw new UnsupportedOperationException();
    }

    @Override
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

    @Override
    public <T> CompletionFuture<T[]> asyncToArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> objects) {
        return containsInternal(getDataList(objects));
    }

    @Override
    public CompletionFuture<Boolean> asyncContainsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> es) {
        return addAllInternal(getDataList(es));
    }

    @Override
    public CompletionFuture<Boolean> asyncAddAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> objects) {
        return compareAndRemove(getDataList(objects), false);
    }

    @Override
    public CompletionFuture<Boolean> asyncRemoveAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> objects) {
        return compareAndRemove(getDataList(objects), true);
    }

    @Override
    public CompletionFuture<Boolean> asyncRetainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
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
