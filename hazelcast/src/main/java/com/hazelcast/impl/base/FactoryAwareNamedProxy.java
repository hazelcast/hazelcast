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

package com.hazelcast.impl.base;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.HazelcastInstanceAwareInstance;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.nio.DataSerializable;

import java.io.*;

public abstract class FactoryAwareNamedProxy implements HazelcastInstanceAwareInstance, DataSerializable {
    transient protected FactoryImpl factory = null;
    protected String name = null;

    protected FactoryAwareNamedProxy() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FactoryImpl getFactory() {
        return factory;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
    }

    public void readData(DataInput in) throws IOException {
        setName(in.readUTF());
        setHazelcastInstance(ThreadContext.get().getCurrentFactory());
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeData(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        readData(in);
    }
}
