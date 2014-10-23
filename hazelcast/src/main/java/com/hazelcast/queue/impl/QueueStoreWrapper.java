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

package com.hazelcast.queue.impl;

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
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.ValidationUtil.checkNotNull;

/**
 * Wrapper for the Queue Store.
 */
@SuppressWarnings("unchecked")
public final class QueueStoreWrapper implements QueueStore<Data> {

    private static final int DEFAULT_MEMORY_LIMIT = 1000;

    private static final int DEFAULT_BULK_LOAD = 250;

    private static final int OUTPUT_SIZE = 1024;

    private static final String STORE_BINARY = "binary";

    private static final String STORE_MEMORY_LIMIT = "memory-limit";

    private static final String STORE_BULK_LOAD = "bulk-load";

    private int memoryLimit = DEFAULT_MEMORY_LIMIT;

    private int bulkLoad = DEFAULT_BULK_LOAD;

    private boolean enabled;

    private boolean binary;

    private QueueStore store;

    private SerializationService serializationService;

    private QueueStoreWrapper() {
    }

    /**
     * Factory method that creates a {@link QueueStoreWrapper}
     *
     * @param name                 queue name
     * @param storeConfig          store config of queue
     * @param serializationService serialization service.
     * @return returns a new instance of {@link QueueStoreWrapper}
     */
    public static QueueStoreWrapper create(String name, QueueStoreConfig storeConfig, SerializationService serializationService) {
        checkNotNull(name, "name should not be null");
        checkNotNull(serializationService, "serializationService should not be null");

        final QueueStoreWrapper storeWrapper = new QueueStoreWrapper();
        storeWrapper.setSerializationService(serializationService);
        if (storeConfig == null) {
            return storeWrapper;
        }
        // create queue store.
        final ClassLoader classLoader = serializationService.getClassLoader();
        final QueueStore queueStore = createQueueStore(name, storeConfig, classLoader);
        if (queueStore != null) {
            storeWrapper.setEnabled(storeConfig.isEnabled());
            storeWrapper.setBinary(Boolean.parseBoolean(storeConfig.getProperty(STORE_BINARY)));
            storeWrapper.setMemoryLimit(parseInt(STORE_MEMORY_LIMIT, DEFAULT_MEMORY_LIMIT, storeConfig));
            storeWrapper.setBulkLoad(parseInt(STORE_BULK_LOAD, DEFAULT_BULK_LOAD, storeConfig));
            storeWrapper.setStore(queueStore);
        }
        return storeWrapper;
    }

    private static QueueStore createQueueStore(String name, QueueStoreConfig storeConfig, ClassLoader classLoader) {
        // 1. Try to create store from `store impl.` class.
        QueueStore store = getQueueStore(storeConfig, classLoader);
        // 2. Try to create store from `store factory impl.` class.
        if (store == null) {
            store = getQueueStoreFactory(name, storeConfig, classLoader);
        }
        return store;
    }

    private static QueueStore getQueueStore(QueueStoreConfig storeConfig, ClassLoader classLoader) {
        if (storeConfig == null) {
            return null;
        }
        QueueStore store = storeConfig.getStoreImplementation();
        if (store != null) {
            return store;
        }
        try {
            store = ClassLoaderUtil.newInstance(classLoader, storeConfig.getClassName());
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
        return store;

    }

    private static QueueStore getQueueStoreFactory(String name, QueueStoreConfig storeConfig, ClassLoader classLoader) {
        if (storeConfig == null) {
            return null;
        }
        QueueStoreFactory factory = storeConfig.getFactoryImplementation();
        if (factory == null) {
            try {
                factory = ClassLoaderUtil.newInstance(classLoader,
                        storeConfig.getFactoryClassName());
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        return factory == null ? null : factory.newQueueStore(name, storeConfig.getProperties());
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

    private static int parseInt(String name, int defaultValue, QueueStoreConfig storeConfig) {
        final String val = storeConfig.getProperty(name);
        if (val == null || val.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
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

    void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    void setStore(QueueStore store) {
        this.store = store;
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    void setMemoryLimit(int memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    void setBulkLoad(int bulkLoad) {
        if (bulkLoad < 1) {
            bulkLoad = 1;
        }
        this.bulkLoad = bulkLoad;
    }

    void setBinary(boolean binary) {
        this.binary = binary;
    }
}
