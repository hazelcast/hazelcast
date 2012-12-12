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
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Prefix;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

public class EvictLocalMapEntriesCallable implements Callable<Boolean>, HazelcastInstanceAware, DataSerializable {

    private static final long serialVersionUID = -8809741591882405286L;

    private transient HazelcastInstance hazelcastInstance;
    private String mapName;
    private int percentage;

    public EvictLocalMapEntriesCallable() {
    }

    public EvictLocalMapEntriesCallable(String mapName, int percentage) {
        this.mapName = mapName;
        this.percentage = percentage;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public Boolean call() throws Exception {
        CMap cmap = getCMap(hazelcastInstance, mapName);
        cmap.evict(percentage);
        return true;
    }

    private ConcurrentMapManager getConcurrentMapManager(HazelcastInstance h) {
        FactoryImpl factory = (FactoryImpl) h;
        return factory.node.concurrentMapManager;
    }

    private CMap getCMap(HazelcastInstance h, String name) {
        ConcurrentMapManager concurrentMapManager = getConcurrentMapManager(h);
        String fullName = Prefix.MAP + name;
        return concurrentMapManager.getMap(fullName);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(percentage);
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
        percentage = in.readInt();
    }
}
