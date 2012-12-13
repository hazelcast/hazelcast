/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.NodeService;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 13:23 AM
 */
public class ObjectQueueProxy<E> extends QueueProxySupport implements QueueProxy<E> {

    public ObjectQueueProxy(String name, QueueService queueService, NodeService nodeService) {
        super(name, queueService, nodeService);
    }

    public LocalQueueStats getLocalQueueStats() {
        //TODO what to do
        System.out.println(queueService.getContainer("ali").size());
        return null;
    }

    public boolean add(E e) {
        final boolean res = offer(e);
        if (!res) {
            throw new IllegalStateException();
        }
        return res;
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
        final Data data = nodeService.toData(e);
        return offerInternal(data, timeUnit.toMillis(timeout));
    }

    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public int remainingCapacity() {
        return config.getMaxSize() - size();
    }

    public boolean remove(Object o) {
        final Data data = nodeService.toData(o);
        return removeInternal(data);
    }

    public boolean contains(Object o) {
        final Data data = nodeService.toData(o);
        Set<Data> dataSet = new HashSet<Data>(1);
        dataSet.add(data);
        return containsInternal(dataSet);
    }

    public int drainTo(Collection<? super E> objects) {
        return 0;
    }

    public int drainTo(Collection<? super E> objects, int i) {
        return 0;
    }

    public E remove() {
        final E res = poll();
        if (res == null) {
            throw new NoSuchElementException();
        }
        return res;
    }

    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        final Data data = pollInternal(timeUnit.toMillis(timeout));
        return (E) nodeService.toObject(data);
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
            throw new NoSuchElementException();
        }
        return res;
    }

    public E peek() {
        final Data data = peekInternal();
        return (E) nodeService.toObject(data);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Iterator<E> iterator() {
        return null;
    }

    public Object[] toArray() {
        return new Object[0];
    }

    public <T> T[] toArray(T[] ts) {
        return null;
    }

    public boolean containsAll(Collection<?> objects) {
        Iterator iter = objects.iterator();
        Set<Data> dataSet = new HashSet<Data>(objects.size());
        while (iter.hasNext()){
            Object o = iter.next();
            final Data data = nodeService.toData(o);
            dataSet.add(data);
        }
        return containsInternal(dataSet);
    }

    public boolean addAll(Collection<? extends E> es) {
        return false;
    }

    public boolean removeAll(Collection<?> objects) {
        return false;
    }

    public boolean retainAll(Collection<?> objects) {
        return false;
    }

    public String getName() {
        return name;
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
    }

    public void removeItemListener(ItemListener<E> listener) {
    }

    public InstanceType getInstanceType() {
        return InstanceType.QUEUE;
    }

    public void destroy() {
    }

    public Object getId() {
        return name;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("IQueue");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
