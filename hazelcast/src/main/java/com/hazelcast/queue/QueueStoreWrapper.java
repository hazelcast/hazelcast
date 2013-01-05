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

package com.hazelcast.queue;

import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.QueueStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @ali 12/14/12
 */
public class QueueStoreWrapper {

    private static final int DEFAULT_MEMORY_LIMIT = 1000;

    private static final int DEFAULT_BULK_LOAD = 250;

    private QueueStore store;

    private QueueStoreConfig storeConfig;

    private boolean enabled = false;

    private int memoryLimit = DEFAULT_MEMORY_LIMIT;

    private int bulkLoad = DEFAULT_BULK_LOAD;

    private boolean binary = false;

    private final SerializationService serializationService;

    public QueueStoreWrapper(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public void setConfig(QueueStoreConfig storeConfig) {
        if (storeConfig == null) {
            return;
        }
        this.storeConfig = storeConfig;
        try {
            Class<?> storeClass = Class.forName(storeConfig.getClassName());
            store = (QueueStore) storeClass.newInstance();
            enabled = storeConfig.isEnabled();
            binary = Boolean.parseBoolean(storeConfig.getProperty("binary"));
            memoryLimit = parseInt("memory-limit", DEFAULT_MEMORY_LIMIT);
            bulkLoad = parseInt("bulk-load", DEFAULT_BULK_LOAD);
            if (bulkLoad < 1) {
                bulkLoad = 1;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
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

    public void store(Long key, Data value) throws Exception {
        if (enabled) {
            store.store(key, binary ? value.buffer : serializationService.toObject(value));
        }
    }

    public void storeAll(Map<Long, Data> map) throws Exception {
        if (enabled) {
            if (binary) {
                store.storeAll(map);
            } else {
                Map<Long, Object> objectMap = new HashMap<Long, Object>(map.size());
                for (Map.Entry<Long, Data> entry : map.entrySet()) {
                    objectMap.put(entry.getKey(), serializationService.toObject(entry.getValue()));
                }
                store.storeAll(objectMap);
            }
        }
    }

    public void delete(Long key) throws Exception {
        if (enabled) {
            store.delete(key);
        }
    }

    public void deleteAll(Collection<Long> keys) throws Exception {
        if (enabled) {
            store.deleteAll(keys);
        }
    }

    public Data load(Long key) throws Exception {
        if (enabled) {
            Object val = store.load(key);
            if (binary) {
                return (Data) val;
            }
            return serializationService.toData(val);
        }
        return null;
    }

    public Map<Long, Data> loadAll(Collection<Long> keys) throws Exception {
        if (enabled) {
            Map<Long, ?> map = store.loadAll(keys);
            if (binary) {
                return (Map<Long, Data>) map;
            }
            Map<Long, Data> dataMap = new HashMap<Long, Data>(map.size());
            for (Map.Entry<Long, ?> entry : map.entrySet()) {
                dataMap.put(entry.getKey(), serializationService.toData(entry.getValue()));
            }
            return dataMap;
        }
        return null;
    }

    public Set<Long> loadAllKeys() throws Exception {
        if (enabled) {
            return store.loadAllKeys();
        }
        return null;
    }

    private int parseInt(String name, int defaultValue) {
        String val = storeConfig.getProperty(name);
        if (val == null){
            return defaultValue;
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
