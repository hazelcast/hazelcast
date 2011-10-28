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

package com.hazelcast.impl;

import com.hazelcast.core.*;
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
	String actualName;
    String name;
    FactoryImpl factory;

    public ListProxyImpl() {
    }

    public ListProxyImpl(String name, FactoryImpl factory) {
    	this.actualName = name;
        this.name = Prefix.QUEUE + actualName;
        this.factory = factory;
    }

    public Instance.InstanceType getInstanceType() {
        return Instance.InstanceType.LIST;
    }

    public void destroy() {
    	factory.destroyInstanceClusterWide(actualName, null);
        factory.destroyInstanceClusterWide(Prefix.MAP + name, null);
    }

    public Object getId() {
        return name;
    }

    public int size() {
        return factory.node.blockingQueueManager.size(name);
    }

    public boolean contains(Object o) {
        Set keys = factory.node.blockingQueueManager.getValueKeys(name, toData(o));
        return keys != null && keys.size() > 0;
    }

    public Iterator iterator() {
        return factory.node.blockingQueueManager.iterate(name);
    }

    public boolean add(Object o) {
        return factory.node.blockingQueueManager.add(name, o, Integer.MAX_VALUE);
    }

    public boolean remove(Object o) {
        return factory.node.blockingQueueManager.remove(name, o);
    }

    public boolean addAll(Collection c) {
        for (Object o : c) {
            add(o);
        }
        return true;
    }

    public Object get(int index) {
        return factory.node.blockingQueueManager.getItemByIndex(name, index);
    }

    public Object set(int index, Object element) {
        return factory.node.blockingQueueManager.set(name, element, index);
    }

    public void add(int index, Object element) {
        try {
            factory.node.blockingQueueManager.offer(name, element, index, 0);
        } catch (InterruptedException e) {
        }
    }

    public Object remove(int index) {
        return factory.node.blockingQueueManager.remove(name, index);
    }

    public int indexOf(Object o) {
        return factory.node.blockingQueueManager.getIndexOf(name, o, true);
    }

    public int lastIndexOf(Object o) {
        return factory.node.blockingQueueManager.getIndexOf(name, o, false);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }

    public String getName() {
        return name.substring(4);
    }

    public void addItemListener(ItemListener itemListener, boolean includeValue) {
        factory.node.blockingQueueManager.addItemListener(name, itemListener, includeValue);
    }

    public void removeItemListener(ItemListener itemListener) {
        factory.node.blockingQueueManager.removeItemListener(name, itemListener);
    }

    @Override
    public String toString() {
        return "List [" + getName() + "] ";
    }
}

