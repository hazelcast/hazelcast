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
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.security.permission.RingBufferPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
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
import static com.hazelcast.jet.impl.connector.AsyncHazelcastWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.security.PermissionsUtil.cachePutPermission;
import static com.hazelcast.security.PermissionsUtil.listAddPermission;
import static com.hazelcast.security.PermissionsUtil.mapPutPermission;
import static com.hazelcast.security.PermissionsUtil.mapUpdatePermission;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * This is private API. Check out the {@link SinkProcessors} class for
 * public factory methods.
 */
public final class HazelcastWriters {

    private HazelcastWriters() {
    }

    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        String clientXml = asXmlString(clientConfig);
        return preferLocalParallelismOne(mapPutPermission(clientXml, name),
                new WriteMapP.Supplier<>(clientXml, name, toKeyFn, toValueFn));
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
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(updateFn, "updateFn");

        String clientXml = asXmlString(clientConfig);
        return ProcessorMetaSupplier.of(mapUpdatePermission(clientXml, name),
                AbstractHazelcastConnectorSupplier.ofMap(clientXml,
                        SecuredFunctions.updateMapProcessorFn(name, clientXml, toKeyFn, updateFn)));
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

        String clientXml = asXmlString(clientConfig);
        return ProcessorMetaSupplier.of(mapUpdatePermission(clientXml, name),
                AbstractHazelcastConnectorSupplier.ofMap(clientXml,
                        SecuredFunctions.updateWithEntryProcessorFn(MAX_PARALLEL_ASYNC_OPS_DEFAULT, name, clientXml,
                                toKeyFn, toEntryProcessorFn)));
    }

    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapSupplier(
            int maxParallelAsyncOps,
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toEntryProcessorFn, "toEntryProcessorFn");

        String clientXml = asXmlString(clientConfig);
        return ProcessorMetaSupplier.of(mapUpdatePermission(clientXml, name),
                AbstractHazelcastConnectorSupplier.ofMap(clientXml,
                        SecuredFunctions.updateWithEntryProcessorFn(maxParallelAsyncOps, name, clientXml,
                                toKeyFn, toEntryProcessorFn)));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeCacheSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        return preferLocalParallelismOne(cachePutPermission(clientXml, name),
                new WriteCachePSupplier<>(clientXml, name));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        return preferLocalParallelismOne(listAddPermission(clientXml, name),
                new WriteListPSupplier<>(clientXml, name));
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

            @Override
            public Permission getRequiredPermission() {
                return new RingBufferPermission(name, ACTION_CREATE, ACTION_PUT);
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

        WriteCachePSupplier(@Nullable String clientXml, @Nonnull String name) {
            super(clientXml);
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

        @Override
        public List<Permission> permissions() {
            return singletonList(cachePutPermission(clientXml, name));
        }
    }

    private static class WriteListPSupplier<T> extends AbstractHazelcastConnectorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;

        WriteListPSupplier(@Nullable String clientXml, @Nonnull String name) {
            super(clientXml);
            this.name = name;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
            IList<Object> list = instance.getList(name);

            FunctionEx<Context, List<Data>> bufferCreator = context -> new ArrayList<>();
            BiConsumerEx<List<Data>, T> itemReceiver = (buffer, item) -> buffer.add(serializationService.toData(item));
            ConsumerEx<List<Data>> bufferFlusher = buffer -> {
                try {
                    list.addAll(buffer);
                } catch (HazelcastInstanceNotActiveException e) {
                    throw handleInstanceNotActive(e, isLocal());
                }
                buffer.clear();
            };

            return new WriteBufferedP<>(bufferCreator, itemReceiver, bufferFlusher, ConsumerEx.noop());
        }

        @Override
        public List<Permission> permissions() {
            return singletonList(listAddPermission(clientXml, name));
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
}
