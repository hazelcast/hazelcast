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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/14/12
 * Time: 12:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class DataQueueProxy extends QueueProxySupport implements QueueProxy<Data> {

    public DataQueueProxy(String name, QueueService queueService, NodeService nodeService) {
        super(name, queueService, nodeService);
    }

    public LocalQueueStats getLocalQueueStats() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean add(Data data) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean offer(Data data) {
        try {
            return offer(data,-1,null);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void put(Data data) throws InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean offer(Data data, long ttl, TimeUnit timeUnit) throws InterruptedException {
        return false;
    }

    public Data take() throws InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Data poll(long l, TimeUnit timeUnit) throws InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int remainingCapacity() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean remove(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean contains(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int drainTo(Collection<? super Data> objects) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int drainTo(Collection<? super Data> objects, int i) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isEmpty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Iterator<Data> iterator() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object[] toArray() {
        return new Object[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean containsAll(Collection<?> objects) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean addAll(Collection<? extends Data> datas) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean removeAll(Collection<?> objects) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getName() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void addItemListener(ItemListener<Data> listener, boolean includeValue) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeItemListener(ItemListener<Data> listener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public InstanceType getInstanceType() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void destroy() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object getId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Data remove() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Data poll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Data element() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Data peek() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public <T> T[] toArray(T[] ts) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean retainAll(Collection<?> objects) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
