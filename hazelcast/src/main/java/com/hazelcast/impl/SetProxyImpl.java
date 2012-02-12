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
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Prefix;
import com.hazelcast.nio.DataSerializable;

import java.io.*;
import java.util.AbstractCollection;
import java.util.Iterator;

public class SetProxyImpl extends AbstractCollection implements SetProxy, DataSerializable, HazelcastInstanceAwareInstance {
    String name = null;
    private transient SetProxy base = null;
    private transient FactoryImpl factory = null;

    public SetProxyImpl() {
    }

    SetProxyImpl(String name, FactoryImpl factory) {
        this.name = name;
        this.factory = factory;
        this.base = new SetProxyReal();
    }

    public SetProxy getBase() {
        return base;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (SetProxy) factory.getOrCreateProxyByName(name);
        }
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    @Override
    public String toString() {
        ensure();
        return "Set [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SetProxyImpl that = (SetProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public int size() {
        ensure();
        return base.size();
    }

    public boolean contains(Object o) {
        ensure();
        return base.contains(o);
    }

    public Iterator iterator() {
        ensure();
        return base.iterator();
    }

    public boolean add(Object o) {
        ensure();
        return base.add(o);
    }

    public boolean remove(Object o) {
        ensure();
        return base.remove(o);
    }

    public void clear() {
        ensure();
        base.clear();
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public void destroy() {
        factory.destroyInstanceClusterWide(name, null);
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

    public String getName() {
        ensure();
        return base.getName();
    }

    public void addItemListener(ItemListener itemListener, boolean includeValue) {
        ensure();
        base.addItemListener(itemListener, includeValue);
    }

    public void removeItemListener(ItemListener itemListener) {
        ensure();
        base.removeItemListener(itemListener);
    }

    public MProxy getMProxy() {
        ensure();
        return base.getMProxy();
    }

    class SetProxyReal extends AbstractCollection implements SetProxy {

        final MProxy mapProxy;

        public SetProxyReal() {
            mapProxy = new MProxyImpl(name, factory);
        }

        public Object getId() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            return SetProxyImpl.this.equals(o);
        }

        @Override
        public int hashCode() {
            return SetProxyImpl.this.hashCode();
        }

        public InstanceType getInstanceType() {
            return BaseManager.getInstanceType(name);
        }

        public void addItemListener(ItemListener listener, boolean includeValue) {
            mapProxy.addGenericListener(listener, null, includeValue,
                    getInstanceType());
        }

        public void removeItemListener(ItemListener listener) {
            mapProxy.removeGenericListener(listener, null);
        }

        public String getName() {
            return name.substring(Prefix.SET.length());
        }

        @Override
        public boolean add(Object obj) {
            return mapProxy.add(obj);
        }

        @Override
        public boolean remove(Object obj) {
            return mapProxy.removeKey(obj);
        }

        @Override
        public boolean contains(Object obj) {
            return mapProxy.containsKey(obj);
        }

        @Override
        public Iterator iterator() {
            return mapProxy.keySet().iterator();
        }

        @Override
        public int size() {
            return mapProxy.size();
        }

        @Override
        public void clear() {
            mapProxy.clear();
        }

        public void destroy() {
            factory.destroyInstanceClusterWide(name, null);
        }

        public MProxy getMProxy() {
            return mapProxy;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}
