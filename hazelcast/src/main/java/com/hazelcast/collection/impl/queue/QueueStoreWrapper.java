/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.config.QueueStoreConfig.DEFAULT_BULK_LOAD;
import static com.hazelcast.config.QueueStoreConfig.DEFAULT_MEMORY_LIMIT;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Wrapper for the Queue Store.
 */
public final class QueueStoreWrapper implements QueueStore<Data> {
    private final String name;
    private int memoryLimit = DEFAULT_MEMORY_LIMIT;
    private int bulkLoad = DEFAULT_BULK_LOAD;
    private boolean enabled;
    private boolean binary;
    private QueueStore store;
    private SerializationService serializationService;

    private QueueStoreWrapper(String name) {
        this.name = name;
    }

    /**
     * Factory method that creates a {@link QueueStoreWrapper}
     *
     * @param name                 queue name
     * @param storeConfig          store config of queue
     * @param serializationService serialization service.
     * @return returns a new instance of {@link QueueStoreWrapper}
     */
    public static QueueStoreWrapper create(@Nonnull String name,
                                           @Nullable QueueStoreConfig storeConfig,
                                           @Nonnull SerializationService serializationService,
                                           @Nullable ClassLoader classLoader) {
        checkNotNull(name, "name should not be null");
        checkNotNull(serializationService, "serializationService should not be null");

        QueueStoreWrapper storeWrapper = new QueueStoreWrapper(name);
        storeWrapper.setSerializationService(serializationService);
        if (storeConfig == null || !storeConfig.isEnabled()) {
            return storeWrapper;
        }
        // create queue store.
        QueueStore queueStore = createQueueStore(name, storeConfig, classLoader);
        if (queueStore != null) {
            boolean isBinary = Boolean.parseBoolean(storeConfig.getProperty(QueueStoreConfig.STORE_BINARY));
            int memoryLimit = parseInt(QueueStoreConfig.STORE_MEMORY_LIMIT, DEFAULT_MEMORY_LIMIT, storeConfig);
            int bulkLoad = parseInt(QueueStoreConfig.STORE_BULK_LOAD, DEFAULT_BULK_LOAD, storeConfig);
            storeWrapper.setEnabled(storeConfig.isEnabled())
                        .setBinary(isBinary)
                        .setMemoryLimit(memoryLimit)
                        .setBulkLoad(bulkLoad)
                        .setStore(queueStore);
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
            ignore(ignored);
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
                factory = ClassLoaderUtil.newInstance(classLoader, storeConfig.getFactoryClassName());
            } catch (Exception ignored) {
                ignore(ignored);
            }
        }
        return factory == null ? null : factory.newQueueStore(name, storeConfig.getProperties());
    }

    void instrument(NodeEngine nodeEngine) {
        Diagnostics diagnostics = ((NodeEngineImpl) nodeEngine).getDiagnostics();
        StoreLatencyPlugin storeLatencyPlugin = diagnostics.getPlugin(StoreLatencyPlugin.class);
        if (!enabled || storeLatencyPlugin == null) {
            return;
        }

        this.store = new LatencyTrackingQueueStore(store, storeLatencyPlugin, name);
    }

    @Override
    public void store(Long key, Data value) {
        if (!enabled) {
            return;
        }
        Object actualValue;
        if (binary) {
            // WARNING: we can't pass original Data to the user
            actualValue = Arrays.copyOf(value.toByteArray(), value.totalSize());
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

        Map<Long, Object> objectMap = createHashMap(map.size());
        if (binary) {
            // WARNING: we can't pass original Data to the user
            // TODO: @mm - is there really an advantage of using binary storeAll?
            // since we need to do array copy for each item.
            for (Map.Entry<Long, Data> entry : map.entrySet()) {
                Data value = entry.getValue();
                byte[] copy = Arrays.copyOf(value.toByteArray(), value.totalSize());
                objectMap.put(entry.getKey(), copy);
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

        Object val = store.load(key);
        if (binary) {
            byte[] dataBuffer = (byte[]) val;
            return new HeapData(Arrays.copyOf(dataBuffer, dataBuffer.length));
        }
        return serializationService.toData(val);
    }

    @Override
    public Map<Long, Data> loadAll(Collection<Long> keys) {
        if (!enabled) {
            return null;
        }

        Map<Long, ?> map = store.loadAll(keys);
        if (map == null) {
            return Collections.emptyMap();
        }
        Map<Long, Data> dataMap = createHashMap(map.size());
        if (binary) {
            for (Map.Entry<Long, ?> entry : map.entrySet()) {
                byte[] dataBuffer = (byte[]) entry.getValue();
                Data data = new HeapData(Arrays.copyOf(dataBuffer, dataBuffer.length));
                dataMap.put(entry.getKey(), data);
            }
        } else {
            for (Map.Entry<Long, ?> entry : map.entrySet()) {
                dataMap.put(entry.getKey(), serializationService.toData(entry.getValue()));
            }
        }
        return dataMap;
    }

    @Override
    public Set<Long> loadAllKeys() {
        if (enabled) {
            return store.loadAllKeys();
        }
        return null;
    }

    private static int parseInt(String name, int defaultValue, QueueStoreConfig storeConfig) {
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

    QueueStoreWrapper setStore(QueueStore store) {
        this.store = store;
        return this;
    }

    QueueStoreWrapper setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    QueueStoreWrapper setMemoryLimit(int memoryLimit) {
        this.memoryLimit = memoryLimit;
        return this;
    }

    QueueStoreWrapper setBulkLoad(int bulkLoad) {
        if (bulkLoad < 1) {
            bulkLoad = 1;
        }
        this.bulkLoad = bulkLoad;
        return this;
    }

    QueueStoreWrapper setBinary(boolean binary) {
        this.binary = binary;
        return this;
    }
}
