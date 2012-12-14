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

package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.monitor.LocalQueueStatsImpl;
import com.hazelcast.impl.monitor.QueueOperationsCounter;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.DataSerializable;

import java.io.*;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class QProxyImpl extends AbstractQueue implements QProxy, HazelcastInstanceAwareInstance, DataSerializable {
    private transient QProxy qproxyReal = null;
    private transient FactoryImpl factory = null;
    private String name = null;
    private BlockingQueueManager blockingQueueManager = null;
    private ListenerManager listenerManager = null;

    public QProxyImpl() {
    }

    QProxyImpl(String name, FactoryImpl factory) {
        this.name = name;
        qproxyReal = new QProxyReal();
        setHazelcastInstance(factory);
    }

    public FactoryImpl getFactory() {
        return factory;
    }

    public String getLongName() {
        ensure();
        return qproxyReal.getLongName();
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
        this.blockingQueueManager = factory.node.blockingQueueManager;
        this.listenerManager = factory.node.listenerManager;
    }

    private void ensure() {
        factory.initialChecks();
        if (qproxyReal == null) {
            qproxyReal = (QProxy) factory.getOrCreateProxyByName(name);
        }
    }

    public Object getId() {
        ensure();
        return qproxyReal.getId();
    }

    @Override
    public String toString() {
        return "Queue [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QProxyImpl qProxy = (QProxyImpl) o;
        return !(name != null ? !name.equals(qProxy.name) : qProxy.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeData(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        readData(in);
    }

    public LocalQueueStats getLocalQueueStats() {
        ensure();
        return qproxyReal.getLocalQueueStats();
    }

    public Iterator iterator() {
        ensure();
        return qproxyReal.iterator();
    }

    public int size() {
        ensure();
        return qproxyReal.size();
    }

    public void addItemListener(ItemListener listener, boolean includeValue) {
        ensure();
        qproxyReal.addItemListener(listener, includeValue);
    }

    public void removeItemListener(ItemListener listener) {
        ensure();
        qproxyReal.removeItemListener(listener);
    }

    public String getName() {
        return name.substring(Prefix.QUEUE.length());
    }

    public int drainTo(Collection c) {
        ensure();
        return qproxyReal.drainTo(c);
    }

    public int drainTo(Collection c, int maxElements) {
        ensure();
        return qproxyReal.drainTo(c, maxElements);
    }

    public void destroy() {
        ensure();
        qproxyReal.destroy();
    }

    public InstanceType getInstanceType() {
        ensure();
        return qproxyReal.getInstanceType();
    }

    public boolean offer(Object o) {
        ensure();
        return qproxyReal.offer(o);
    }

    public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
        ensure();
        return qproxyReal.offer(obj, timeout, unit);
    }

    public void put(Object obj) throws InterruptedException {
        ensure();
        qproxyReal.put(obj);
    }

    public Object poll() {
        ensure();
        return qproxyReal.poll();
    }

    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        ensure();
        return qproxyReal.poll(timeout, unit);
    }

    public Object take() throws InterruptedException {
        ensure();
        return qproxyReal.take();
    }

    public int remainingCapacity() {
        ensure();
        return qproxyReal.remainingCapacity();
    }

    public Object peek() {
        ensure();
        return qproxyReal.peek();
    }

    public QueueOperationsCounter getQueueOperationCounter() {
        return qproxyReal.getQueueOperationCounter();
    }

    private static void check(Object obj) {
        Util.checkSerializable(obj);
    }

    private class QProxyReal extends AbstractQueue implements QProxy {
        private final QueueOperationsCounter operationsCounter = new QueueOperationsCounter();

        public QProxyReal() {
        }

        public LocalQueueStats getLocalQueueStats() {
            operationsCounter.incrementOtherOperations();
            LocalQueueStatsImpl localQueueStats = blockingQueueManager.getOrCreateBQ(name).getQueueStats();
            localQueueStats.setOperationStats(operationsCounter.getPublishedStats());
            return localQueueStats;
        }

        public String getLongName() {
            return name;
        }

        public boolean offer(Object obj) {
            check(obj);
            try {
                return offer(obj, 0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                return false;
            }
        }

        public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
            check(obj);
            if (timeout < 0) {
                timeout = 0;
            }
            boolean result = blockingQueueManager.offer(name, obj, unit.toMillis(timeout));
            if (!result) {
                operationsCounter.incrementRejectedOffers();
            }
            operationsCounter.incrementOffers();
            return result;
        }

        public void put(Object obj) throws InterruptedException {
            check(obj);
            blockingQueueManager.offer(name, obj, -1);
            operationsCounter.incrementOffers();
        }

        public Object peek() {
            operationsCounter.incrementOtherOperations();
            return blockingQueueManager.peek(name);
        }

        public Object poll() {
            try {
                Object result = blockingQueueManager.poll(name, 0);
                if (result == null) {
                    operationsCounter.incrementEmptyPolls();
                }
                operationsCounter.incrementPolls();
                return result;
            } catch (InterruptedException e) {
                return null;
            }
        }

        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            if (timeout < 0) {
                timeout = 0;
            }
            Object result = blockingQueueManager.poll(name, unit.toMillis(timeout));
            if (result == null) {
                operationsCounter.incrementEmptyPolls();
            }
            operationsCounter.incrementPolls();
            return result;
        }

        public Object take() throws InterruptedException {
            Object result = blockingQueueManager.poll(name, -1);
            if (result == null) {
                operationsCounter.incrementEmptyPolls();
            }
            operationsCounter.incrementPolls();
            return result;
        }

        public int remainingCapacity() {
            operationsCounter.incrementOtherOperations();
            BlockingQueueManager.BQ q = blockingQueueManager.getOrCreateBQ(name);
            int maxSizePerJVM = q.getMaxSizePerJVM();
            if (maxSizePerJVM <= 0) {
                return Integer.MAX_VALUE;
            } else {
                int size = size();
                int numberOfMembers = factory.node.getClusterImpl().getMembers().size();
                int totalCapacity = numberOfMembers * maxSizePerJVM;
                return totalCapacity - size;
            }
        }

        @Override
        public Iterator iterator() {
            operationsCounter.incrementOtherOperations();
            return blockingQueueManager.iterate(name);
        }

        @Override
        public int size() {
            operationsCounter.incrementOtherOperations();
            return blockingQueueManager.size(name);
        }

        public void addItemListener(ItemListener listener, boolean includeValue) {
            blockingQueueManager.addItemListener(name, listener, includeValue);
        }

        public void removeItemListener(ItemListener listener) {
            blockingQueueManager.removeItemListener(name, listener);
        }

        public String getName() {
            return QProxyImpl.this.getName();
        }

        @Override
        public boolean remove(Object obj) {
            throw new UnsupportedOperationException();
        }

        public int drainTo(Collection c) {
            return drainTo(c, Integer.MAX_VALUE);
        }

        public int drainTo(Collection c, int maxElements) {
            if (c == null) throw new NullPointerException("drainTo null!");
            if (maxElements < 0) throw new IllegalArgumentException("Negative maxElements:" + maxElements);
            if (maxElements == 0) return 0;
            if (c instanceof QProxy) {
                QProxy q = (QProxy) c;
                if (q.getName().equals(getName())) {
                    throw new IllegalArgumentException("Cannot drainTo self!");
                }
            }
            operationsCounter.incrementOtherOperations();
            int added = 0;
            Object value = null;
            do {
                value = poll();
                if (value != null) {
                    if (!c.add(value)) {
                        throw new RuntimeException("drainTo is not able to add!");
                    }
                    added++;
                }
            } while (added < maxElements && value != null);
            return added;
        }

        public void destroy() {
            operationsCounter.incrementOtherOperations();
            factory.destroyInstanceClusterWide(name, null);
            factory.destroyInstanceClusterWide(Prefix.MAP + name, null);
        }

        public InstanceType getInstanceType() {
            return InstanceType.QUEUE;
        }

        public Object getId() {
            return name;
        }

        public QueueOperationsCounter getQueueOperationCounter() {
            return operationsCounter;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}
