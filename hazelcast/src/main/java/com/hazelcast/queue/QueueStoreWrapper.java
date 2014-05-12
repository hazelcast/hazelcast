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

package com.hazelcast.queue;

import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class QueueStoreWrapper implements QueueStore<Data> {

    private static final int DEFAULT_MEMORY_LIMIT = 1000;

    private static final int DEFAULT_BULK_LOAD = 250;

    private static final int OUTPUT_SIZE = 1024;

    private QueueStore store;

    private QueueStoreConfig storeConfig;

    private boolean enabled;

    private int memoryLimit = DEFAULT_MEMORY_LIMIT;

    private int bulkLoad = DEFAULT_BULK_LOAD;

    private boolean binary;

    private final SerializationService serializationService;

    public QueueStoreWrapper(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public void setConfig(QueueStoreConfig storeConfig, String name) {
        if (storeConfig == null) {
            return;
        }
        store = storeConfig.getStoreImplementation();
        if (store == null) {
            try {
                store = ClassLoaderUtil.newInstance(serializationService.getClassLoader(), storeConfig.getClassName());
            } catch (Exception ignored) {
            }
        }

        if (store == null) {
            QueueStoreFactory factory = storeConfig.getFactoryImplementation();
            if (factory == null) {
                try {
                    factory = ClassLoaderUtil.newInstance(serializationService.getClassLoader(),
                            storeConfig.getFactoryClassName());
                } catch (Exception ignored) {
                }
            }
            if (factory == null) {
                return;
            }
            store = factory.newQueueStore(name, storeConfig.getProperties());
        }
        this.storeConfig = storeConfig;
        enabled = storeConfig.isEnabled();
        binary = Boolean.parseBoolean(storeConfig.getProperty("binary"));
        memoryLimit = parseInt("memory-limit", DEFAULT_MEMORY_LIMIT);
        bulkLoad = parseInt("bulk-load", DEFAULT_BULK_LOAD);
        if (bulkLoad < 1) {
            bulkLoad = 1;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isBinary() {
        return binary;
    }

    public int getMemoryLimit() {
        return memoryLimit;
    }

    public int getBulkLoad() {
        return bulkLoad;
    }

    @Override
    public void store(Long key, Data value) {
        if (!enabled) {
            return;
        }
        final Object actualValue;
        if (binary) {
            // WARNING: we can't pass original Data to the user
            BufferObjectDataOutput out = serializationService.createObjectDataOutput(value.totalSize());
            try {
                value.writeData(out);
                // buffer size is exactly equal to binary size, no need to copy array.
                actualValue = out.getBuffer();
            } catch (IOException e) {
                throw new HazelcastException(e);
            } finally {
                IOUtil.closeResource(out);
            }
        } else {
            actualValue = serializationService.toObject(value);
        }
        store.store(key, actualValue);
    }

    @Override
    public void storeAll(Map<Long, Data> map) {
        if (!enabled) {
            return;
        }

        final Map<Long, Object> objectMap = new HashMap<Long, Object>(map.size());
        if (binary) {
            // WARNING: we can't pass original Data to the user
            // TODO: @mm - is there really an advantage of using binary storeAll?
            // since we need to do array copy for each item.
            BufferObjectDataOutput out = serializationService.createObjectDataOutput(OUTPUT_SIZE);
            try {
                for (Map.Entry<Long, Data> entry : map.entrySet()) {
                    entry.getValue().writeData(out);
                    objectMap.put(entry.getKey(), out.toByteArray());
                    out.clear();
                }
            } catch (IOException e) {
                throw new HazelcastException(e);
            } finally {
                IOUtil.closeResource(out);
            }
        } else {
            for (Map.Entry<Long, Data> entry : map.entrySet()) {
                objectMap.put(entry.getKey(), serializationService.toObject(entry.getValue()));
            }
        }
        store.storeAll(objectMap);
    }

    @Override
    public void delete(Long key) {
        if (enabled) {
            store.delete(key);
        }
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
        if (enabled) {
            store.deleteAll(keys);
        }
    }

    @Override
    public Data load(Long key) {
        if (!enabled) {
            return null;
        }

        final Object val = store.load(key);
        if (binary) {
            byte[] dataBuffer = (byte[]) val;
            ObjectDataInput in = serializationService.createObjectDataInput(dataBuffer);
            Data data = new Data();
            try {
                data.readData(in);
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
            return data;
        }
        return serializationService.toData(val);
    }

    @Override
    public Map<Long, Data> loadAll(Collection<Long> keys) {
        if (enabled) {
            final Map<Long, ?> map = store.loadAll(keys);
            if (map == null) {
                return Collections.emptyMap();
            }
            final Map<Long, Data> dataMap = new HashMap<Long, Data>(map.size());
            if (binary) {
                for (Map.Entry<Long, ?> entry : map.entrySet()) {
                    byte[] dataBuffer = (byte[]) entry.getValue();
                    ObjectDataInput in = serializationService.createObjectDataInput(dataBuffer);
                    Data data = new Data();
                    try {
                        data.readData(in);
                    } catch (IOException e) {
                        throw new HazelcastException(e);
                    }
                    dataMap.put(entry.getKey(), data);
                }
                return (Map<Long, Data>) map;
            } else {
                for (Map.Entry<Long, ?> entry : map.entrySet()) {
                    dataMap.put(entry.getKey(), serializationService.toData(entry.getValue()));
                }
            }
            return dataMap;
        }
        return null;
    }

    @Override
    public Set<Long> loadAllKeys() {
        if (enabled) {
            return store.loadAllKeys();
        }
        return null;
    }

    private int parseInt(String name, int defaultValue) {
        String val = storeConfig.getProperty(name);
        if (val == null || val.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
