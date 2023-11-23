/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.security.impl.function.SecuredFunctions;
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

    // Dummy function to make the EE SecuredFunctionTest.java pass.
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        MapSinkConfiguration<T, K, V> params = new MapSinkConfiguration<>(name);
        params.setClientConfig(clientConfig);
        params.setToKeyFn(toKeyFn);
        params.setToValueFn(toValueFn);

        String clientXml = asXmlString(clientConfig);
        params.setClientXml(clientXml);

        return writeMapSupplier(params);
    }

    /**
     * Update map with key and value functions
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeMapSupplier(MapSinkConfiguration<T, K, V> params) {
        if (params.hasDataSourceConnection()) {
            WriteMapP.Supplier<? super T, ? extends K, ? extends V> supplier = WriteMapP.Supplier.createNew(params);

            return preferLocalParallelismOne(supplier);
        } else if (params.hasClientConfig()) {
            //Set clientXml
            String clientXml = asXmlString(params.getClientConfig());
            params.setClientXml(clientXml);

            WriteMapP.Supplier<? super T, ? extends K, ? extends V> supplier = WriteMapP.Supplier.createNew(params);

            return preferLocalParallelismOne(supplier);
        } else {
            WriteMapP.Supplier<? super T, ? extends K, ? extends V> supplier = WriteMapP.Supplier.createNew(params);

            Permission permission = mapPutPermission(null, params.getMapName());
            return preferLocalParallelismOne(permission, supplier);
        }
    }

    /**
     * Update map with a merge function
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier mergeMapSupplier(MapSinkConfiguration<T, K, V> params) {
        // Get reference to functions because MapSinkMergeParams is not serializable
        FunctionEx<? super T, ? extends K> toKeyFn = params.getToKeyFn();
        FunctionEx<? super T, ? extends V> toValueFn = params.getToValueFn();
        BinaryOperatorEx<V> mergeFn = params.getMergeFn();

        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toValueFn, "toValueFn");
        checkSerializable(mergeFn, "mergeFn");

        if (params.hasDataSourceConnection()) {
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                    params.getMapName(),
                    "",
                    toKeyFn,
                    (V oldValue, T item) -> {
                        V newValue = toValueFn.apply(item);
                        if (oldValue == null) {
                            return newValue;
                        }
                        return mergeFn.apply(oldValue, newValue);
                    });
            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            processorSupplier.setDataConnectionName(params.getDataConnectionName());
            return ProcessorMetaSupplier.of(processorSupplier);

        } else if (params.hasClientConfig()) {
            String clientXml = asXmlString(params.getClientConfig());
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                    params.getMapName(),
                    "",
                    toKeyFn,
                    (V oldValue, T item) -> {
                        V newValue = toValueFn.apply(item);
                        if (oldValue == null) {
                            return newValue;
                        }
                        return mergeFn.apply(oldValue, newValue);
                    });
            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            processorSupplier.setClientXml(clientXml);
            return ProcessorMetaSupplier.of(processorSupplier);

        } else {
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                    params.getMapName(),
                    null,
                    toKeyFn,
                    (V oldValue, T item) -> {
                        V newValue = toValueFn.apply(item);
                        if (oldValue == null) {
                            return newValue;
                        }
                        return mergeFn.apply(oldValue, newValue);
                    });
            Permission permission = mapUpdatePermission(null, params.getMapName());
            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            return ProcessorMetaSupplier.of(permission, processorSupplier);
        }
    }

    // Dummy function to make the EE SecuredFunctionTest.java pass.
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

    /**
     * Update map with an update function
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(MapSinkConfiguration<T, K, V> params) {
        checkSerializable(params.getToKeyFn(), "toKeyFn");
        checkSerializable(params.getUpdateFn(), "updateFn");

        if (params.hasDataSourceConnection()) {
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                    params.getMapName(),
                    "",
                    params.getToKeyFn(),
                    params.getUpdateFn());

            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            processorSupplier.setDataConnectionName(params.getDataConnectionName());

            return ProcessorMetaSupplier.of(processorSupplier);

        }  else if (params.hasClientConfig()) {
            String clientXml = asXmlString(params.getClientConfig());
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                    params.getMapName(),
                    "",
                    params.getToKeyFn(),
                    params.getUpdateFn());

            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            processorSupplier.setClientXml(clientXml);

            return ProcessorMetaSupplier.of(processorSupplier);
        } else {
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                    params.getMapName(),
                    null,
                    params.getToKeyFn(),
                    params.getUpdateFn());

            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            Permission permission = mapUpdatePermission(null, params.getMapName());
            return ProcessorMetaSupplier.of(permission, processorSupplier);
        }
    }

    // Dummy function to make the EE SecuredFunctionTest.java pass.
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        MapSinkEntryProcessorParams<T, K, V, R> params = new MapSinkEntryProcessorParams<>(name);
        params.setMaxParallelAsyncOps(MAX_PARALLEL_ASYNC_OPS_DEFAULT);
        params.setToKeyFn(toKeyFn);
        params.setToEntryProcessorFn(toEntryProcessorFn);

        return updateMapSupplier(params);

    }

    /**
     * Update map with an EntryProcessor
     */
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapSupplier(MapSinkEntryProcessorParams<T, K, V, R> params) {
        checkSerializable(params.getToKeyFn(), "toKeyFn");
        checkSerializable(params.getToEntryProcessorFn(), "toEntryProcessorFn");

        if (params.hasDataSourceConnection()) {
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateWithEntryProcessorFn(
                    params.getMaxParallelAsyncOps(),
                    params.getMapName(),
                    "",
                    params.getToKeyFn(),
                    params.getToEntryProcessorFn());

            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            processorSupplier.setDataConnectionName(params.getDataConnectionName());

            return ProcessorMetaSupplier.of(processorSupplier);
        } else if (params.hasClientConfig()) {
            String clientXml = asXmlString(params.getClientConfig());
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateWithEntryProcessorFn(
                    params.getMaxParallelAsyncOps(),
                    params.getMapName(),
                    "",
                    params.getToKeyFn(),
                    params.getToEntryProcessorFn());

            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);
            processorSupplier.setClientXml(clientXml);

            return ProcessorMetaSupplier.of(processorSupplier);
        } else {
            FunctionEx<HazelcastInstance, Processor> processorFunction = SecuredFunctions.updateWithEntryProcessorFn(
                    params.getMaxParallelAsyncOps(),
                    params.getMapName(),
                    null,
                    params.getToKeyFn(),
                    params.getToEntryProcessorFn());

            ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(processorFunction);

            Permission permission = mapUpdatePermission(null, params.getMapName());
            return ProcessorMetaSupplier.of(permission, processorSupplier);
        }
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

    @SuppressWarnings("AnonInnerLength")
    public static ProcessorMetaSupplier writeObservableSupplier(@Nonnull String name) {
        return new ProcessorMetaSupplier() {
            private static final long serialVersionUID = 1L;

            @Nonnull
            @Override
            public Map<String, String> getTags() {
                return singletonMap(ObservableImpl.OWNED_OBSERVABLE, name);
            }

            @Override
            public int preferredLocalParallelism() {
                return 1;
            }

            @Nonnull
            @Override
            public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                return address -> new WriteObservableP.Supplier(name);
            }

            @Override
            public Permission getRequiredPermission() {
                return new RingBufferPermission(name, ACTION_CREATE, ACTION_PUT);
            }

            @Override
            public boolean isReusable() {
                return true;
            }

            @Override
            public boolean initIsCooperative() {
                return true;
            }

            @Override
            public boolean closeIsCooperative() {
                return true;
            }
        };
    }

    static RuntimeException handleInstanceNotActive(HazelcastInstanceNotActiveException e, boolean isLocal) {
        // if we are writing to a local instance, restarting the job should resolve the error
        return isLocal ? new RestartableException(e) : e;
    }

    private static class WriteCachePSupplier<K, V> extends AbstractHazelcastConnectorSupplier {

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

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

        @Override
        @Nonnull
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

            @Override
            @Nonnull
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
