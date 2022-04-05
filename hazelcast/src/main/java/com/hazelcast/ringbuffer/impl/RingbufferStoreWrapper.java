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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.Arrays;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Wrapper for the ring buffer store. In charge of creation of a ring buffer store from
 * configuration and enforcing rules defined by the ring buffer configuration (e.g.
 * store format) before forwarding the calls to the underlying ring buffer store.
 */
public final class RingbufferStoreWrapper implements RingbufferStore<Data> {

    private final ObjectNamespace namespace;
    private boolean enabled;

    /**
     * Is the data being stored in serialized (BINARY/NATIVE) or deserialized (OBJECT) format.
     */
    private InMemoryFormat inMemoryFormat;
    private RingbufferStore store;
    private SerializationService serializationService;

    private RingbufferStoreWrapper(ObjectNamespace namespace) {
        this.namespace = namespace;
    }

    /**
     * Factory method that creates a {@link RingbufferStoreWrapper}. It attempts to create the ring buffer store from several
     * sources :
     * <ul>
     * <li>checks if the config contains the store implementation</li>
     * <li>tries to create the store from the ring buffer store class</li>
     * <li>tries to create one from the ring buffer store factory</li>
     * <li>tries to instantiate the factory from the factory class and create the store from the factory</li>
     * </ul>
     *
     * @param namespace            ring buffer namespace
     * @param storeConfig          store config of ring buffer
     * @param inMemoryFormat       the format of the stored items (BINARY, OBJECT or NATIVE). NATIVE format translates into
     *                             binary values on the store method calls.
     * @param serializationService serialization service.
     * @return returns a new instance of {@link RingbufferStoreWrapper}
     */
    public static RingbufferStoreWrapper create(ObjectNamespace namespace,
                                                RingbufferStoreConfig storeConfig,
                                                InMemoryFormat inMemoryFormat, SerializationService serializationService,
                                                ClassLoader classLoader) {
        checkNotNull(namespace, "namespace should not be null");
        checkNotNull(serializationService, "serializationService should not be null");

        final RingbufferStoreWrapper storeWrapper = new RingbufferStoreWrapper(namespace);
        storeWrapper.serializationService = serializationService;
        if (storeConfig == null || !storeConfig.isEnabled()) {
            return storeWrapper;
        }
        // create ring buffer store.
        final RingbufferStore ringbufferStore = createRingbufferStore(namespace, storeConfig, classLoader);
        if (ringbufferStore != null) {
            storeWrapper.enabled = storeConfig.isEnabled();
            storeWrapper.inMemoryFormat = inMemoryFormat;
            storeWrapper.store = ringbufferStore;
        }
        return storeWrapper;
    }

    private static RingbufferStore createRingbufferStore(ObjectNamespace namespace,
                                                         RingbufferStoreConfig storeConfig,
                                                         ClassLoader classLoader) {
        // 1. Try to create store from `store impl.` class.
        RingbufferStore store = getRingbufferStore(storeConfig, classLoader);
        // 2. Try to create store from `store factory impl.` class.
        if (store == null) {
            store = getRingbufferStoreFactory(namespace, storeConfig, classLoader);
        }
        return store;
    }

    private static RingbufferStore getRingbufferStore(RingbufferStoreConfig storeConfig, ClassLoader classLoader) {
        if (storeConfig == null) {
            return null;
        }
        return getOrInstantiate(storeConfig.getStoreImplementation(), classLoader, storeConfig.getClassName());
    }

    private static RingbufferStore getRingbufferStoreFactory(ObjectNamespace namespace,
                                                             RingbufferStoreConfig storeConfig, ClassLoader classLoader) {
        if (storeConfig == null) {
            return null;
        }
        final RingbufferStoreFactory implementation = storeConfig.getFactoryImplementation();
        final String className = storeConfig.getFactoryClassName();

        final RingbufferStoreFactory factory = getOrInstantiate(implementation, classLoader, className);
        return factory == null ? null : factory.newRingbufferStore(namespace.getObjectName(), storeConfig.getProperties());
    }

    private static <T> T getOrInstantiate(T instance, ClassLoader classLoader, String className) {
        if (instance != null) {
            return instance;
        }
        try {
            return ClassLoaderUtil.newInstance(classLoader, className);
        } catch (Exception ignored) {
            ignore(ignored);
        }
        return null;
    }

    /**
     * Is the ring buffer store enabled.
     *
     * @return if the store is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    void instrument(NodeEngine nodeEngine) {
        Diagnostics diagnostics = ((NodeEngineImpl) nodeEngine).getDiagnostics();
        StoreLatencyPlugin storeLatencyPlugin = diagnostics.getPlugin(StoreLatencyPlugin.class);
        if (!enabled || storeLatencyPlugin == null) {
            return;
        }

        this.store = new LatencyTrackingRingbufferStore(store, storeLatencyPlugin, namespace);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void store(long sequence, Data value) {
        final Object actualValue;
        if (isBinaryFormat()) {
            // WARNING: we can't pass original byte array to the user
            actualValue = Arrays.copyOf(value.toByteArray(), value.totalSize());
        } else {
            // here we deserialize the object again (the first time is in the actual ring buffer).
            // if we are certain that the user cannot abuse the object, we can provide him with a reference to the
            // stored value
            actualValue = serializationService.toObject(value);
        }
        store.store(sequence, actualValue);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void storeAll(long firstItemSequence, Data[] items) {
        final Object[] storedItems = new Object[items.length];
        for (int i = 0; i < items.length; i++) {
            final Data value = items[i];
            if (isBinaryFormat()) {
                // more defensive copying in the case of binary format
                storedItems[i] = Arrays.copyOf(value.toByteArray(), value.totalSize());
            } else {
                // in the case of object format we have again deserialization. We could possibly pass the user the reference
                // to the item in the RB store if it is safe
                storedItems[i] = serializationService.toObject(value);
            }

        }
        store.storeAll(firstItemSequence, storedItems);
    }

    private boolean isBinaryFormat() {
        return inMemoryFormat.equals(BINARY) || inMemoryFormat.equals(NATIVE);
    }

    @Override
    public Data load(long sequence) {
        final Object val = store.load(sequence);
        if (val == null) {
            return null;
        }

        if (isBinaryFormat()) {
            byte[] dataBuffer = (byte[]) val;
            return new HeapData(Arrays.copyOf(dataBuffer, dataBuffer.length));
        }
        return serializationService.toData(val);
    }

    @Override
    public long getLargestSequence() {
        return store.getLargestSequence();
    }
}
