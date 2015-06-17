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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MapLoader;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.CallContext;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Keys;
import com.hazelcast.impl.MProxy;
import com.hazelcast.impl.Processable;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.base.KeyValue;
import com.hazelcast.impl.base.Pairs;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class GetAllCallable implements Callable<Pairs>, HazelcastInstanceAware, DataSerializable {

    private String mapName;
    private Keys keys;
    private FactoryImpl factory = null;

    public GetAllCallable() {
    }

    public GetAllCallable(String mapName, Keys keys) {
        this.mapName = mapName;
        this.keys = keys;
    }

    public Pairs call() throws Exception {
        final ConcurrentMapManager c = factory.node.concurrentMapManager;
        Pairs pairs = new Pairs();
        CMap cmap = c.getMap(mapName);
        if (cmap == null) {
            c.enqueueAndWait(new Processable() {
                public void process() {
                    c.getOrCreateMap(mapName);
                }
            });
            cmap = c.getMap(mapName);
        }

        if (cmap != null) {
            MapLoader loader = cmap.getMapLoader();
            Collection<Object> keysToLoad = (loader != null) ? new HashSet<Object>() : null;
            Set<Data> missingKeys = new HashSet<Data>(1);
            for (Data key : keys.getKeys()) {
                boolean missing = true;
                Record record = cmap.getRecord(key);

                if (record != null) {
                    if (record.isActive() && record.isValid()) {
                        Data value = record.getValueData();
                        if (value != null) {
                            pairs.addKeyValue(new KeyValue(key, value));
                            record.setLastAccessed();
                            missing = false;
                        }
                    }
                    if (!record.isActive() && record.getRemoveTime() > 0) {
                        missing = false;
                    }
                }

                if (missing) {
                    missingKeys.add(key);
                    if (keysToLoad != null) {
                        keysToLoad.add(toObject(key));
                    }
                }
            }

            if (keysToLoad != null && keysToLoad.size() > 0 && loader != null) {
                final Map<Object, Object> mapLoadedEntries = loader.loadAll(keysToLoad);
                if (mapLoadedEntries != null) {
                    for (Object key : mapLoadedEntries.keySet()) {
                        Data dKey = toData(key);
                        Object value = mapLoadedEntries.get(key);
                        Data dValue = toData(value);
                        if (dKey != null && dValue != null) {
                            pairs.addKeyValue(new KeyValue(dKey, dValue));
                            c.putTransient(mapName, key, value, -1);
                        } else {
                            missingKeys.add(dKey);
                        }
                    }
                }
            }
            if (loader == null && !missingKeys.isEmpty()) {
                ThreadContext threadContext = ThreadContext.get();
                CallContext realCallContext = threadContext.getCallContext();
                try {
                    threadContext.setCallContext(CallContext.DUMMY_CLIENT);
                    MProxy mproxy = (MProxy) factory.getOrCreateProxyByName(mapName);
                    for (Data key : missingKeys) {
                        Data value = (Data) mproxy.get(key);
                        if (value != null) {
                            pairs.addKeyValue(new KeyValue(key, value));
                        }
                    }
                } finally {
                    threadContext.setCallContext(realCallContext);
                }
            }
        }
        return pairs;
    }

    public void readData(DataInput in) throws IOException {
        mapName = in.readUTF();
        keys = new Keys();
        keys.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(mapName);
        keys.writeData(out);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.factory = (FactoryImpl) hazelcastInstance;
    }
}
