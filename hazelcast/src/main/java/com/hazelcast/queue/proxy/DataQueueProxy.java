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

import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:54 AM
 */
public class DataQueueProxy extends QueueProxySupport implements QueueProxy<Data> {

    public DataQueueProxy(String name, QueueService queueService, NodeEngine nodeEngine) {
        super(name, queueService, nodeEngine);
    }

    public LocalQueueStats getLocalQueueStats() {
        return getService().createLocalQueueStats(name, partitionId);
    }

    public boolean add(Data data) {
        final boolean res = offer(data);
        if (!res) {
            throw new IllegalStateException("Queue is full!");
        }
        return res;
    }

    public boolean offer(Data data) {
        try {
            return offer(data, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    public void put(Data data) throws InterruptedException {
        offer(data, -1, TimeUnit.MILLISECONDS);
    }

    public boolean offer(Data data, long timeout, TimeUnit timeUnit) throws InterruptedException {
        return offerInternal(data, timeUnit.toMillis(timeout));
    }

    public Data take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public Data poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return (Data) pollInternal(timeUnit.toMillis(timeout));
    }

    public int remainingCapacity() {
        return config.getMaxSize() - size();
    }

    public boolean remove(Object o) {
        return removeInternal((Data) o);
    }

    public boolean contains(Object o) {
        List<Data> dataSet = new ArrayList<Data>(1);
        dataSet.add((Data) o);
        return containsInternal(dataSet);
    }

    public int drainTo(Collection<? super Data> objects) {
        return drainTo(objects, -1);
    }

    public int drainTo(Collection<? super Data> objects, int i) {
        if (this.equals(objects)) {
            throw new IllegalArgumentException("Can not drain to same Queue");
        }
        Collection<Data> dataList = drainInternal(i);
        objects.addAll(dataList);
        return dataList.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Iterator<Data> iterator() {
        return new QueueIterator<Data>(listInternal().iterator(), null, true);
    }

    public Object[] toArray() {
        List<Data> list = listInternal();
        return list.toArray();
    }

    public boolean containsAll(Collection<?> objects) {
        return containsInternal((Collection<Data>) objects);
    }

    public boolean addAll(Collection<? extends Data> datas) {
        return addAllInternal((Collection<Data>) datas);
    }

    public boolean removeAll(Collection<?> objects) {
        return compareAndRemove((Collection<Data>) objects, false);
    }

    public Data remove() {
        final Data res = poll();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public Data poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public Data element() {
        final Data res = peek();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public Data peek() {
        return (Data) peekInternal();
    }

    public <T> T[] toArray(T[] ts) {
        List<Data> list = listInternal();
        return list.toArray(ts);
    }

    public boolean retainAll(Collection<?> objects) {
        return compareAndRemove((Collection<Data>) objects, true);
    }
}
