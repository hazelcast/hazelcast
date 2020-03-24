/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.map.EntryProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Collections.singletonMap;

/**
 * This is private API. Check out the {@link SinkProcessors} class for
 * public factory methods.
 */
public final class HazelcastWriters {

    private HazelcastWriters() {
    }

    @Nonnull
    public static ProcessorMetaSupplier writeMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig
    ) {
        return preferLocalParallelismOne(new WriteMapP.Supplier(asXmlString(clientConfig), name));
    }

    @Nonnull
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
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String mapName,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(updateFn, "updateFn");

        return ProcessorMetaSupplier.of(new UpdateMapP.Supplier<>(
                asXmlString(clientConfig), mapName, toKeyFn, updateFn
        ));
    }

    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toEntryProcessorFn, "toEntryProcessorFn");

        return ProcessorMetaSupplier.of(new UpdateMapWithEntryProcessorP.Supplier<>(
                name, asXmlString(clientConfig), toKeyFn, toEntryProcessorFn
        ));
    }

    @Nonnull
    public static <K, V> ProcessorMetaSupplier writeCacheSupplier(
            @Nonnull String name, @Nullable ClientConfig clientConfig
    ) {
        return ProcessorMetaSupplier.of(2, new WriteCachePSupplier<>(clientConfig, name));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        boolean isLocal = clientConfig == null;
        return preferLocalParallelismOne(new WriterSupplier<>(
                asXmlString(clientConfig),
                ArrayList::new,
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

    public static ProcessorMetaSupplier writeObservableSupplier(@Nonnull String name) {
        return new ProcessorMetaSupplier() {
            @Nonnull
            @Override
            public Map<String, String> getTags() {
                return singletonMap(ObservableImpl.OWNED_OBSERVABLE, name);
            }

            @Override
            public int preferredLocalParallelism() {
                return 1;
            }

            @Nonnull @Override
            public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                return address -> new WriteObservableP.Supplier(name);
            }
        };
    }

    static RuntimeException handleInstanceNotActive(HazelcastInstanceNotActiveException e, boolean isLocal) {
        // if we are writing to a local instance, restarting the job should resolve the error
        return isLocal ? new RestartableException(e) : e;
    }

    private static class WriteCachePSupplier<K, V> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;

        WriteCachePSupplier(@Nullable ClientConfig clientConfig, @Nonnull String name) {
            super(asXmlString(clientConfig));
            this.name = name;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
            ICache<Data, Data> cache = instance.getCacheManager().getCache(name);

            FunctionEx<Context, ArrayMap<Data, Data>> bufferCreator = context -> new ArrayMap<>();
            BiConsumerEx<ArrayMap<Data, Data>, Entry<K, V>> entryReceiver = (buffer, entry) -> {
                Data key = serializationService.toData(entry.getKey());
                Data value = serializationService.toData(entry.getValue());
                buffer.add(new SimpleEntry<>(key, value));
            };
            ConsumerEx<ArrayMap<Data, Data>> bufferFlusher = buffer -> {
                try {
                    cache.putAll(buffer);
                } catch (HazelcastInstanceNotActiveException e) {
                    throw handleInstanceNotActive(e, isLocal());
                }
                buffer.clear();
            };

            return new WriteBufferedP<>(bufferCreator, entryReceiver, bufferFlusher, ConsumerEx.noop());
        }
    }

    static final class ArrayMap<K, V> extends AbstractMap<K, V> {

        private final List<Entry<K, V>> entries;
        private final ArraySet set = new ArraySet();

        ArrayMap() {
            entries = new ArrayList<>();
        }

        ArrayMap(int size) {
            entries = new ArrayList<>(size);
        }

        @Override @Nonnull
        public Set<Entry<K, V>> entrySet() {
            return set;
        }

        public void add(Map.Entry<K, V> entry) {
            entries.add(entry);
        }

        @Override
        public V get(Object key) {
            throw new UnsupportedOperationException();
        }

        private class ArraySet extends AbstractSet<Entry<K, V>> {

            @Override @Nonnull
            public Iterator<Entry<K, V>> iterator() {
                return entries.iterator();
            }

            @Override
            public int size() {
                return entries.size();
            }

            @Override
            public void clear() {
                entries.clear();
            }
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }

    private static class WriterSupplier<B, T> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private final FunctionEx<HazelcastInstance, ConsumerEx<B>> instanceToFlushBufferFn;
        private final SupplierEx<B> newBufferFn;
        private final BiConsumerEx<B, T> addToBufferFn;
        private final ConsumerEx<B> disposeBufferFn;

        WriterSupplier(
                String clientXml,
                SupplierEx<B> newBufferFn,
                BiConsumerEx<B, T> addToBufferFn,
                FunctionEx<HazelcastInstance, ConsumerEx<B>> instanceToFlushBufferFn,
                ConsumerEx<B> disposeBufferFn
        ) {
            super(clientXml);
            this.newBufferFn = newBufferFn;
            this.addToBufferFn = addToBufferFn;
            this.instanceToFlushBufferFn = instanceToFlushBufferFn;
            this.disposeBufferFn = disposeBufferFn;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
            ConsumerEx<B> flushBufferFn = instanceToFlushBufferFn.apply(instance);
            return new WriteBufferedP<>(ctx -> newBufferFn.get(), addToBufferFn, flushBufferFn, disposeBufferFn);
        }
    }
}

