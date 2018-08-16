/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.SerializationConstants;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.callbackOf;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.tryIncrement;
import static java.util.stream.Collectors.toList;

/**
 * This is private API. Check out the {@link SinkProcessors} class for
 * public factory methods.
 */
public final class HazelcastWriters {

    private HazelcastWriters() {
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier mergeMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
            @Nonnull DistributedFunction<? super T, ? extends V> toValueFn,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toValueFn, "toValueFn");
        checkSerializable(mergeFn, "mergeFn");

        return updateMapSupplier(name, clientConfig, toKeyFn, (V oldValue, T item) -> {
            V newValue = toValueFn.apply(item);
            if (oldValue == null) {
                return newValue;
            }
            return mergeFn.apply(oldValue, newValue);
        });
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
            @Nonnull DistributedBiFunction<? super V, ? super T, ? extends V> updateFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(updateFn, "updateFn");

        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayList<>(),
                ArrayList::add,
                instance -> {
                    IMap map = instance.getMap(name);
                    Map<K, T> tmpMap = new HashMap<>();
                    ApplyFnEntryProcessor<K, V, T> entryProcessor = new ApplyFnEntryProcessor<>(tmpMap, updateFn);

                    return buffer -> {
                        try {
                            if (buffer.isEmpty()) {
                                return;
                            }
                            for (Object object : buffer) {
                                T item = (T) object;
                                K key = toKeyFn.apply(item);
                                // on duplicate key, we'll flush immediately
                                if (tmpMap.containsKey(key)) {
                                    map.executeOnKeys(tmpMap.keySet(), entryProcessor);
                                    tmpMap.clear();
                                }
                                tmpMap.put(key, item);
                            }
                            map.executeOnKeys(tmpMap.keySet(), entryProcessor);
                            tmpMap.clear();
                        } catch (HazelcastInstanceNotActiveException e) {
                            throw handleInstanceNotActive(e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                noopConsumer()
        ));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
            @Nonnull DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toEntryProcessorFn, "toEntryProcessorFn");

        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new EntryProcessorWriterSupplier<>(
                        name,
                        serializableConfig(clientConfig),
                        toKeyFn,
                        toEntryProcessorFn,
                        isLocal
                )
        );
    }


    @Nonnull
    @SuppressWarnings("unchecked")
    public static ProcessorMetaSupplier writeMapSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayMap(),
                ArrayMap::add,
                instance -> {
                    IMap map = instance.getMap(name);
                    return buffer -> {
                        try {
                            map.putAll(buffer);
                        } catch (HazelcastInstanceNotActiveException e) {
                            throw handleInstanceNotActive(e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                noopConsumer()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeCacheSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayMap(),
                ArrayMap::add,
                CacheFlush.flushToCache(name, isLocal),
                noopConsumer()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                index -> new ArrayList<>(),
                ArrayList::add,
                instance -> {
                    IList<Object> list = instance.getList(name);
                    return buffer -> {
                        try {
                            list.addAll(buffer);
                        } catch (HazelcastInstanceNotActiveException e) {
                            throw handleInstanceNotActive(e, isLocal);
                        }
                        buffer.clear();
                    };
                },
                noopConsumer()
        ));
    }

    private static RuntimeException handleInstanceNotActive(HazelcastInstanceNotActiveException e, boolean isLocal) {
        // if we are writing to a local instance, restarting the job should resolve the error
        return isLocal ? new RestartableException(e) : e;
    }

    private static SerializableClientConfig serializableConfig(ClientConfig clientConfig) {
        return clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
    }

    /**
     * Wrapper class needed to conceal the JCache API while
     * serializing/deserializing other lambdas
     */
    private static class CacheFlush {

        static DistributedFunction<HazelcastInstance, DistributedConsumer<ArrayMap>> flushToCache(
                String name, boolean isLocal
        ) {
            return instance -> {
                ICache cache = instance.getCacheManager().getCache(name);
                return buffer -> {
                    try {
                        cache.putAll(buffer);
                    } catch (HazelcastInstanceNotActiveException e) {
                        throw handleInstanceNotActive(e, isLocal);
                    }
                    buffer.clear();
                };
            };
        }
    }

    private static final class ArrayMap extends AbstractMap<Object, Object> {

        private final List<Entry<Object, Object>> entries;
        private final ArraySet set = new ArraySet();

        ArrayMap() {
            entries = new ArrayList<>();
        }

        @Override @Nonnull
        public Set<Entry<Object, Object>> entrySet() {
            return set;
        }

        public void add(Map.Entry entry) {
            entries.add(entry);
        }

        private class ArraySet extends AbstractSet<Entry<Object, Object>> {
            @Override @Nonnull
            public Iterator<Entry<Object, Object>> iterator() {
                return entries.iterator();
            }

            @Override
            public int size() {
                return entries.size();
            }
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }

    private static final class EntryProcessorWriter<T, K, V> extends AbstractProcessor {

        private static final int MAX_PARALLEL_ASYNC_OPS = 1000;
        private final AtomicInteger numConcurrentOps = new AtomicInteger();

        private final boolean isLocal;
        private final IMap<? super K, ? extends V> map;
        private final DistributedFunction<? super T, ? extends K> toKeyFn;
        private final DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;
        private final AtomicReference<Throwable> lastError = new AtomicReference<>();
        private final ExecutionCallback callback = callbackOf(
                response -> numConcurrentOps.decrementAndGet(),
                exception -> {
                    numConcurrentOps.decrementAndGet();
                    if (exception != null) {
                        lastError.compareAndSet(null, exception);
                    }
                });

        private EntryProcessorWriter(
                @Nonnull HazelcastInstance instance,
                @Nonnull String name,
                @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
                @Nonnull DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
                boolean isLocal
        ) {
            setCooperative(false);
            this.map = instance.getMap(name);
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
            this.isLocal = isLocal;
        }

        @Override
        public boolean tryProcess() {
            checkError();
            return true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object object) {
            checkError();
            if (!tryIncrement(numConcurrentOps, 1, MAX_PARALLEL_ASYNC_OPS)) {
                return false;
            }
            try {
                @SuppressWarnings("unchecked")
                T item = (T) object;
                EntryProcessor<K, V> entryProcessor = toEntryProcessorFn.apply(item);
                K key = toKeyFn.apply(item);
                map.submitToKey(key, entryProcessor, callback);
                return true;
            } catch (HazelcastInstanceNotActiveException e) {
                throw handleInstanceNotActive(e, isLocal);
            }
        }

        @Override
        public boolean complete() {
            return ensureAllWritten();
        }

        @Override
        public boolean saveToSnapshot() {
            return ensureAllWritten();
        }

        private boolean ensureAllWritten() {
            boolean allWritten = numConcurrentOps.get() == 0;
            checkError();
            return allWritten;
        }

        private void checkError() {
            Throwable t = lastError.get();
            if (t != null) {
                throw sneakyThrow(t);
            }
        }
    }

    private static final class EntryProcessorWriterSupplier<T, K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final SerializableClientConfig clientConfig;
        private final DistributedFunction<? super T, ? extends K> toKeyFn;
        private final DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;
        private final boolean isLocal;
        private transient HazelcastInstance client;
        private transient HazelcastInstance instance;

        private EntryProcessorWriterSupplier(
                @Nonnull String name,
                @Nullable SerializableClientConfig clientConfig,
                @Nonnull DistributedFunction<? super T, ? extends K> toKeyFn,
                @Nonnull DistributedFunction<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
                boolean isLocal
        ) {
            this.name = name;
            this.clientConfig = clientConfig;
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
            this.isLocal = isLocal;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (clientConfig != null) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() ->
                    new EntryProcessorWriter<>(instance, name, toKeyFn, toEntryProcessorFn, isLocal))
                         .limit(count)
                         .collect(toList());
        }
    }

    private static class HazelcastWriterSupplier<B, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig clientConfig;
        private final DistributedFunction<HazelcastInstance, DistributedConsumer<B>> instanceToFlushBufferFn;
        private final DistributedFunction<Processor.Context, B> newBufferFn;
        private final DistributedBiConsumer<B, T> addToBufferFn;
        private final DistributedConsumer<B> disposeBufferFn;

        private transient HazelcastInstance client;
        private transient HazelcastInstance instance;

        HazelcastWriterSupplier(
                SerializableClientConfig clientConfig,
                DistributedFunction<Processor.Context, B> newBufferFn,
                DistributedBiConsumer<B, T> addToBufferFn,
                DistributedFunction<HazelcastInstance, DistributedConsumer<B>> instanceToFlushBufferFn,
                DistributedConsumer<B> disposeBufferFn
        ) {
            this.clientConfig = clientConfig;
            this.instanceToFlushBufferFn = instanceToFlushBufferFn;
            this.newBufferFn = newBufferFn;
            this.addToBufferFn = addToBufferFn;
            this.disposeBufferFn = disposeBufferFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (isRemote()) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        private boolean isRemote() {
            return clientConfig != null;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteBufferedP<>(
                    newBufferFn, addToBufferFn, instanceToFlushBufferFn.apply(instance), disposeBufferFn)
            ).limit(count).collect(toList());
        }
    }

    public static class ApplyFnEntryProcessor<K, V, T>
    implements EntryProcessor<K, V>, EntryBackupProcessor<K, V>, IdentifiedDataSerializable {
        private Map<K, T> keysToUpdate;
        private DistributedBiFunction<? super V, ? super T, ? extends V> updateFn;

        public ApplyFnEntryProcessor() {
        }

        public ApplyFnEntryProcessor(
                Map<K, T> keysToUpdate,
                DistributedBiFunction<? super V, ? super T, ? extends V> updateFn
        ) {
            this.keysToUpdate = keysToUpdate;
            this.updateFn = updateFn;
        }

        @Override
        public Object process(Entry<K, V> entry) {
            V oldValue = entry.getValue();
            T item = keysToUpdate.get(entry.getKey());
            if (item == null && !keysToUpdate.containsKey(entry.getKey())) {
                // Implementing equals/hashCode is not required for IMap keys since serialized version is used
                // instead. After serializing/deserializing the keys they will have different identity. And since they
                // don't implement the methods, they key can't be found in the map.
                throw new JetException("The new item not found in the map - is equals/hashCode " +
                        "correctly implemented for the key? Key type: " + entry.getKey().getClass().getName());
            }
            V newValue = updateFn.apply(oldValue, item);
            entry.setValue(newValue);
            return null;
        }

        @Override
        public EntryBackupProcessor<K, V> getBackupProcessor() {
            return this;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(keysToUpdate);
            out.writeObject(updateFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            keysToUpdate = in.readObject();
            updateFn = in.readObject();
        }

        @Override
        public void processBackup(Entry<K, V> entry) {
            process(entry);
        }

        @Override
        public int getFactoryId() {
            return SerializationConstants.FACTORY_ID;
        }

        @Override
        public int getId() {
            return SerializationConstants.APPLY_FN_ENTRY_PROCESSOR;
        }
    }

}
