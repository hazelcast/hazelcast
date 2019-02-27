/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BinaryOperatorEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.asClientConfig;
import static com.hazelcast.jet.impl.util.Util.asXmlString;
import static com.hazelcast.jet.impl.util.Util.callbackOf;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.tryIncrement;
import static com.hazelcast.util.MapUtil.createHashMap;
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
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull BinaryOperatorEx<V> mergeFn
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
            @Nonnull String mapName,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(updateFn, "updateFn");

        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<UpdateMapContext<K, V, T>, T>(
                asXmlString(clientConfig),
                instance -> procContext -> new UpdateMapContext<>(instance, mapName, toKeyFn, updateFn, isLocal),
                UpdateMapContext::add,
                instance -> UpdateMapContext::flush,
                UpdateMapContext::finish
        ));
    }

    private static final class UpdateMapContext<K, V, T> {
        private static final int MAX_PARALLEL_ASYNC_OPS = 1000;

        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
        private final boolean isLocal;
        private final IPartitionService memberPartitionService;
        private final ClientPartitionService clientPartitionService;
        private final SerializationService serializationService;

        private final Semaphore concurrentAsyncOpsSemaphore = new Semaphore(MAX_PARALLEL_ASYNC_OPS);
        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        private final IMap<K, V> map;

        // one map per partition to store the temporary values
        private final Map<Data, Object>[] tmpMaps;

        UpdateMapContext(
                HazelcastInstance instance,
                String mapName,
                @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn,
                boolean isLocal) {
            this.toKeyFn = toKeyFn;
            this.updateFn = updateFn;
            this.isLocal = isLocal;

            map = instance.getMap(mapName);
            int partitionCount;
            if (isLocal) {
                HazelcastInstanceImpl castedInstance = (HazelcastInstanceImpl) instance;
                clientPartitionService = null;
                memberPartitionService = castedInstance.node.nodeEngine.getPartitionService();
                serializationService = castedInstance.getSerializationService();
                partitionCount = memberPartitionService.getPartitionCount();
            } else {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) instance;
                clientPartitionService = clientProxy.client.getClientPartitionService();
                memberPartitionService = null;
                serializationService = clientProxy.getSerializationService();
                partitionCount = clientPartitionService.getPartitionCount();
            }
            tmpMaps = new Map[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                tmpMaps[i] = new HashMap<>();
            }
        }

        void add(T item) {
            K key = toKeyFn.apply(item);
            int partitionId;
            Data keyData;
            if (isLocal) {
                // We pre-serialize the key and value to avoid double serialization when partitionId
                // is calculated and when the value for backup operation is re-serialized
                keyData = serializationService.toData(key, ((MapProxyImpl) map).getPartitionStrategy());
                partitionId = memberPartitionService.getPartitionId(keyData);
            } else {
                // We ignore partition strategy for remote connection, the client doesn't know it.
                // TODO we might be able to fix this after https://github.com/hazelcast/hazelcast/issues/13950 is fixed
                // The functionality should work, but will be ineffective: the submitOnKey calls will have wrongly
                // partitioned data.
                keyData = serializationService.toData(key);
                partitionId = clientPartitionService.getPartitionId(keyData);
            }
            Data itemData = serializationService.toData(item);
            tmpMaps[partitionId].merge(keyData, itemData, (o, n) -> ApplyFnEntryProcessor.append(o, (Data) n));
        }

        void flush() {
            try {
                if (firstError.get() != null) {
                    if (firstError.get() instanceof HazelcastInstanceNotActiveException) {
                        throw handleInstanceNotActive((HazelcastInstanceNotActiveException) firstError.get(), isLocal);
                    }
                    throw sneakyThrow(firstError.get());
                }
                for (int partitionId = 0; partitionId < tmpMaps.length; partitionId++) {
                    if (tmpMaps[partitionId].isEmpty()) {
                        continue;
                    }
                    ApplyFnEntryProcessor<K, V, T> entryProcessor =
                            new ApplyFnEntryProcessor<>(tmpMaps[partitionId], updateFn);
                    try {
                        // block until we get a permit
                        concurrentAsyncOpsSemaphore.acquire();
                    } catch (InterruptedException e) {
                        return;
                    }

                    submitToKeys(map, tmpMaps[partitionId].keySet(), entryProcessor)
                            .andThen(callbackOf(r -> concurrentAsyncOpsSemaphore.release(), t -> {
                                firstError.compareAndSet(null, t);
                                concurrentAsyncOpsSemaphore.release();
                            }));
                    tmpMaps[partitionId] = new HashMap<>();
                }
            } catch (HazelcastInstanceNotActiveException e) {
                throw handleInstanceNotActive(e, isLocal);
            }
        }

        public void finish() {
            try {
                // Acquire all initial permits. These won't be available until all async ops finish.
                concurrentAsyncOpsSemaphore.acquire(MAX_PARALLEL_ASYNC_OPS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> ICompletableFuture<Map<K, V>> submitToKeys(
            IMap<K, V> map, Set<Data> keys, EntryProcessor<K, V> entryProcessor) {
        // TODO remove this method once submitToKeys is public API
        // we force Set<Data> instead of Set<K> to avoid re-serialization of keys
        // this relies on an implementation detail of submitToKeys method.
        if (map instanceof MapProxyImpl) {
            return ((MapProxyImpl) map).submitToKeys(keys, entryProcessor);
        } else if (map instanceof ClientMapProxy) {
            return ((ClientMapProxy) map).submitToKeys(keys, entryProcessor);
        } else {
            throw new RuntimeException("Unexpected map class: " + map.getClass().getName());
        }
    }


    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toEntryProcessorFn, "toEntryProcessorFn");

        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new EntryProcessorWriterSupplier<>(
                        name,
                        asXmlString(clientConfig),
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
                asXmlString(clientConfig),
                instance -> procContext -> new ArrayMap(),
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
                ConsumerEx.noop()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeCacheSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                asXmlString(clientConfig),
                instance -> procContext -> new ArrayMap(),
                ArrayMap::add,
                CacheFlush.flushToCache(name, isLocal),
                ConsumerEx.noop()
        ));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new HazelcastWriterSupplier<>(
                asXmlString(clientConfig),
                instance -> procContext -> new ArrayList<>(),
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
                ConsumerEx.noop()
        ));
    }

    private static RuntimeException handleInstanceNotActive(HazelcastInstanceNotActiveException e, boolean isLocal) {
        // if we are writing to a local instance, restarting the job should resolve the error
        return isLocal ? new RestartableException(e) : e;
    }

    /**
     * Wrapper class needed to conceal the JCache API while
     * serializing/deserializing other lambdas
     */
    private static class CacheFlush {

        @SuppressWarnings("unchecked")
        static FunctionEx<HazelcastInstance, ConsumerEx<ArrayMap>> flushToCache(
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
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;
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
                @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
                boolean isLocal
        ) {
            this.map = instance.getMap(name);
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
            this.isLocal = isLocal;
        }

        @Override
        public boolean isCooperative() {
            return false;
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
        private final String clientXml;
        private final FunctionEx<? super T, ? extends K> toKeyFn;
        private final FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn;
        private final boolean isLocal;
        private transient HazelcastInstance client;
        private transient HazelcastInstance instance;

        private EntryProcessorWriterSupplier(
                @Nonnull String name,
                @Nullable String clientXml,
                @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V>> toEntryProcessorFn,
                boolean isLocal
        ) {
            this.name = name;
            this.clientXml = clientXml;
            this.toKeyFn = toKeyFn;
            this.toEntryProcessorFn = toEntryProcessorFn;
            this.isLocal = isLocal;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (clientXml != null) {
                instance = client = newHazelcastClient(asClientConfig(clientXml));
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

        private final String clientXml;
        private final FunctionEx<HazelcastInstance, ConsumerEx<B>> instanceToFlushBufferFn;
        private final FunctionEx<HazelcastInstance, FunctionEx<Processor.Context, B>>
                instanceToNewBufferFn;
        private final BiConsumerEx<B, T> addToBufferFn;
        private final ConsumerEx<B> disposeBufferFn;

        private transient FunctionEx<Processor.Context, B> newBufferFn;
        private transient ConsumerEx<B> flushBufferFn;
        private transient HazelcastInstance client;

        HazelcastWriterSupplier(
                String clientXml,
                FunctionEx<HazelcastInstance, FunctionEx<Processor.Context, B>> instanceToNewBufferFn,
                BiConsumerEx<B, T> addToBufferFn,
                FunctionEx<HazelcastInstance, ConsumerEx<B>> instanceToFlushBufferFn,
                ConsumerEx<B> disposeBufferFn
        ) {
            this.clientXml = clientXml;
            this.instanceToFlushBufferFn = instanceToFlushBufferFn;
            this.instanceToNewBufferFn = instanceToNewBufferFn;
            this.addToBufferFn = addToBufferFn;
            this.disposeBufferFn = disposeBufferFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance;
            if (isRemote()) {
                instance = client = newHazelcastClient(asClientConfig(clientXml));
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
            flushBufferFn = instanceToFlushBufferFn.apply(instance);
            newBufferFn = instanceToNewBufferFn.apply(instance);
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        private boolean isRemote() {
            return clientXml != null;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteBufferedP<>(newBufferFn, addToBufferFn, flushBufferFn, disposeBufferFn))
                         .limit(count).collect(toList());
        }
    }

    @SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized")
    public static class ApplyFnEntryProcessor<K, V, T>
            implements EntryProcessor<K, V>, EntryBackupProcessor<K, V>, IdentifiedDataSerializable,
            SerializationServiceAware {
        private Map<Data, Object> keysToUpdate;
        private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
        private SerializationService serializationService;

        public ApplyFnEntryProcessor() {
        }

        ApplyFnEntryProcessor(
                Map<Data, Object> keysToUpdate,
                BiFunctionEx<? super V, ? super T, ? extends V> updateFn
        ) {
            this.keysToUpdate = keysToUpdate;
            this.updateFn = updateFn;
        }

        @Override
        public Object process(Entry<K, V> entry) {
            // it should not matter that we don't take the PartitionStrategy here into account
            Data keyData = serializationService.toData(entry.getKey());
            Object item = keysToUpdate.get(keyData);
            if (item == null && !keysToUpdate.containsKey(keyData)) {
                // Implementing equals/hashCode is not required for IMap keys since serialized version is used
                // instead. After serializing/deserializing the keys they will have different identity. And since they
                // don't implement the methods, they key can't be found in the map.
                throw new JetException("The new item not found in the map - is equals/hashCode " +
                        "correctly implemented for the key? Key type: " + entry.getKey().getClass().getName());
            }
            if (item instanceof List) {
                for (Data o : ((List<Data>) item)) {
                    handle(entry, o);
                }
            } else {
                handle(entry, (Data) item);
            }
            return null;
        }

        private void handle(Entry<K, V> entry, Data itemData) {
            T item = serializationService.toObject(itemData);
            V oldValue = entry.getValue();
            V newValue = updateFn.apply(oldValue, item);
            entry.setValue(newValue);
        }

        @Override
        public EntryBackupProcessor<K, V> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Entry<K, V> entry) {
            process(entry);
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(keysToUpdate.size());
            for (Entry<Data, Object> en : keysToUpdate.entrySet()) {
                out.writeData(en.getKey());
                Object value = en.getValue();
                if (value instanceof Data) {
                    out.writeInt(1);
                    out.writeData((Data) value);
                } else if (value instanceof List) {
                    List<Data> list = (List<Data>) value;
                    out.writeInt(list.size());
                    for (Data data : list) {
                        out.writeData(data);
                    }
                } else {
                    assert false : "Unknown value type: " + value.getClass();
                }
            }
            out.writeObject(updateFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int keysToUpdateSize = in.readInt();
            keysToUpdate = createHashMap(keysToUpdateSize);
            for (int i = 0; i < keysToUpdateSize; i++) {
                Data key = in.readData();
                int size = in.readInt();
                Object value;
                if (size == 1) {
                    value = in.readData();
                } else {
                    List<Data> list = new ArrayList<>(size);
                    for (int j = 0; j < size; j++) {
                        list.add(in.readData());
                    }
                    value = list;
                }
                keysToUpdate.put(key, value);
            }
            updateFn = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetDataSerializerHook.APPLY_FN_ENTRY_PROCESSOR;
        }

        // used to group entries when more than one entry exists for the same key
        public static Object append(Object value, Data item) {
            List<Data> list;
            if (value instanceof List) {
                list = (List) value;
            } else {
                list = new ArrayList<>();
                list.add((Data) value);
            }
            list.add(item);
            return list;
        }
    }
}
