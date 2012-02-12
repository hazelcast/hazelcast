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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Processable;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

public class PutAllCallable implements Callable<Boolean>, HazelcastInstanceAware, DataSerializable {

    private String mapName;
    private Pairs pairs;
    private FactoryImpl factory = null;

    public PutAllCallable() {
    }

    public PutAllCallable(String mapName, Pairs pairs) {
        this.mapName = mapName;
        this.pairs = pairs;
    }

    public Boolean call() throws Exception {
        final ConcurrentMapManager c = factory.node.concurrentMapManager;
        CMap cmap = c.getMap(mapName);
        if (cmap == null) {
            c.enqueueAndWait(new Processable() {
                public void process() {
                    c.getOrCreateMap(mapName);
                }
            }, 100);
            cmap = c.getMap(mapName);
        }
        if (cmap != null) {
            for (KeyValue keyValue : pairs.getKeyValues()) {
                Object value = (cmap.getMapIndexService().hasIndexedAttributes()) ?
                        keyValue.getValue() : keyValue.getValueData();
                IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(cmap.getName());
                map.put(keyValue.getKeyData(), value);
            }
        }
        return Boolean.TRUE;
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
        pairs = new Pairs();
        pairs.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
        pairs.writeData(out);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }
}