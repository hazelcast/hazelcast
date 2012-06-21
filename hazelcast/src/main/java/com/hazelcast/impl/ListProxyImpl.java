/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.core.Instance;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.nio.IOUtil.toData;

public class ListProxyImpl extends AbstractList implements ListProxy, DataSerializable {
    String name;
    String queueName;
    String mapName;
    FactoryImpl factory;

    public ListProxyImpl() {
    }

    public ListProxyImpl(String name, FactoryImpl factory) {
        this.factory = factory;
        setName(name);
    }

    public Instance.InstanceType getInstanceType() {
        return Instance.InstanceType.LIST;
    }

    public void destroy() {
        factory.destroyInstanceClusterWide(name, null);
        factory.destroyInstanceClusterWide(queueName, null);
        factory.destroyInstanceClusterWide(mapName, null);
    }

    public Object getId() {
        return queueName;
    }

    public int size() {
        factory.initialChecks();
        return factory.node.blockingQueueManager.size(queueName);
    }

    public boolean contains(Object o) {
        factory.initialChecks();
        Set keys = factory.node.blockingQueueManager.getValueKeys(queueName, toData(o));
        return keys != null && keys.size() > 0;
    }

    public Iterator iterator() {
        factory.initialChecks();
        return factory.node.blockingQueueManager.iterate(queueName);
    }

    public boolean add(Object o) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.add(queueName, o, Integer.MAX_VALUE);
    }

    public boolean remove(Object o) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.remove(queueName, o);
    }

    public boolean addAll(Collection c) {
        for (Object o : c) {
            add(o);
        }
        return true;
    }

    public Object get(int index) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.getItemByIndex(queueName, index);
    }

    public Object set(int index, Object element) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.set(queueName, element, index);
    }

    public void add(int index, Object element) {
        factory.initialChecks();
        try {
            factory.node.blockingQueueManager.offer(queueName, element, index, 0);
        } catch (InterruptedException e) {
        }
    }

    public Object remove(int index) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.remove(queueName, index);
    }

    public int indexOf(Object o) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.getIndexOf(queueName, o, true);
    }

    public int lastIndexOf(Object o) {
        factory.initialChecks();
        return factory.node.blockingQueueManager.getIndexOf(queueName, o, false);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        setName(name);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }

    public String getName() {
        return name.substring(Prefix.AS_LIST.length());
    }

    private void setName(String n) {
        name = n;
        queueName = Prefix.QUEUE + name;
        mapName = Prefix.MAP + queueName;
    }

    public void addItemListener(ItemListener itemListener, boolean includeValue) {
        factory.initialChecks();
        factory.node.blockingQueueManager.addItemListener(queueName, itemListener, includeValue);
    }

    public void removeItemListener(ItemListener itemListener) {
        factory.initialChecks();
        factory.node.blockingQueueManager.removeItemListener(queueName, itemListener);
    }

    @Override
    public String toString() {
        return "List [" + getName() + "] ";
    }
}

