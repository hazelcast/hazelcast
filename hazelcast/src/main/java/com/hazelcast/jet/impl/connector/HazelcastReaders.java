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

import com.hazelcast.cache.impl.CacheEntriesWithCursor;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.protocol.codec.CacheIterateEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchWithQueryCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalCacheReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalMapQueryReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteCacheReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteMapQueryReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteProcessorSupplier;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Permission;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

public final class HazelcastReaders {

    private HazelcastReaders() {
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalCacheSupplier(@Nonnull String cacheName) {
        return new LocalProcessorMetaSupplier<
                InternalCompletableFuture<CacheEntriesWithCursor>, CacheEntriesWithCursor, Entry<Data, Data>>(
                new LocalCacheReaderFunction(cacheName)
        ) {
            @Override
            public Permission getRequiredPermission() {
                return new CachePermission(cacheName, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    public static class LocalCacheReaderFunction implements BiFunctionEx<HazelcastInstance,
            InternalSerializationService, ReadMapOrCacheP.Reader<InternalCompletableFuture<CacheEntriesWithCursor>,
            CacheEntriesWithCursor, Entry<Data, Data>>>, IdentifiedDataSerializable {
        private String cacheName;

        public LocalCacheReaderFunction() {
        }

        public LocalCacheReaderFunction(String cacheName) {
            this.cacheName = cacheName;
        }

        @Override
        public ReadMapOrCacheP.Reader<InternalCompletableFuture<CacheEntriesWithCursor>, CacheEntriesWithCursor,
                Entry<Data, Data>> applyEx(HazelcastInstance hzInstance,
                                           InternalSerializationService serializationService) throws Exception {
            return new LocalCacheReader(hzInstance, serializationService, cacheName);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(cacheName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            cacheName = in.readString();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.LOCAL_CACHE_READER_FUNCTION;
        }
    }

    @Nonnull
    public static ProcessorSupplier readRemoteCacheSupplier(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        String clientXml = ImdgUtil.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, new RemoteCacheReaderFunction(cacheName));
    }

    public static class RemoteCacheReaderFunction implements FunctionEx<HazelcastInstance,
            ReadMapOrCacheP.Reader<ClientInvocationFuture, CacheIterateEntriesCodec.ResponseParameters,
                    Entry<Data, Data>>>, IdentifiedDataSerializable {
        private String cacheName;

        public RemoteCacheReaderFunction() {
        }

        public RemoteCacheReaderFunction(String cacheName) {
            this.cacheName = cacheName;
        }

        @Override
        public ReadMapOrCacheP.Reader<ClientInvocationFuture, CacheIterateEntriesCodec.ResponseParameters,
                Entry<Data, Data>> applyEx(HazelcastInstance hzInstance) throws Exception {
            return new RemoteCacheReader(hzInstance, cacheName);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(cacheName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            cacheName = in.readString();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.REMOTE_CACHE_READER_FUNCTION;
        }
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalMapSupplier(@Nonnull String mapName) {
        return new LocalProcessorMetaSupplier<
                InternalCompletableFuture<MapEntriesWithCursor>, MapEntriesWithCursor, Entry<Data, Data>>(
                new LocalMapReaderFunction(mapName)
        ) {
            @Override
            public Permission getRequiredPermission() {
                return new MapPermission(mapName, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    public static class LocalMapReaderFunction implements BiFunctionEx<HazelcastInstance, InternalSerializationService,
            ReadMapOrCacheP.Reader<InternalCompletableFuture<MapEntriesWithCursor>, MapEntriesWithCursor, Entry<Data, Data>>>,
            IdentifiedDataSerializable {
        private String mapName;

        public LocalMapReaderFunction() {
        }

        public LocalMapReaderFunction(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public ReadMapOrCacheP.Reader<InternalCompletableFuture<MapEntriesWithCursor>, MapEntriesWithCursor, Entry<Data, Data>>
        applyEx(HazelcastInstance instance, InternalSerializationService serializationService) throws Exception {
            return new LocalMapReader(instance, serializationService, mapName);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.LOCAL_MAP_READER_FUNCTION;
        }
    }

    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readLocalMapSupplier(
            @Nonnull String mapName,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(Objects.requireNonNull(predicate), "predicate");
        checkSerializable(Objects.requireNonNull(projection), "projection");

        return new LocalProcessorMetaSupplier<InternalCompletableFuture<ResultSegment>, ResultSegment, QueryResultRow>(
                new LocalMapQueryReaderFunction<>(mapName, predicate, projection)
        ) {
            @Override
            public Permission getRequiredPermission() {
                return new MapPermission(mapName, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    public static class LocalMapQueryReaderFunction<K, V, T> implements BiFunctionEx<HazelcastInstance,
            InternalSerializationService, ReadMapOrCacheP.Reader<InternalCompletableFuture<ResultSegment>,
            ResultSegment, QueryResultRow>>, IdentifiedDataSerializable {

        private String mapName;
        private Predicate<? super K, ? super V> predicate;
        private Projection<? super Entry<K, V>, ? extends T> projection;

        public LocalMapQueryReaderFunction() {
        }

        public LocalMapQueryReaderFunction(String mapName, Predicate<? super K, ? super V> predicate,
                                           Projection<? super Entry<K, V>, ? extends T> projection) {
            this.mapName = mapName;
            this.predicate = predicate;
            this.projection = projection;
        }

        @Override
        public ReadMapOrCacheP.Reader<InternalCompletableFuture<ResultSegment>, ResultSegment, QueryResultRow>
        applyEx(HazelcastInstance hzInstance, InternalSerializationService serializationService) throws Exception {
            return new LocalMapQueryReader(hzInstance, serializationService, mapName, predicate, projection);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeObject(predicate);
            out.writeObject(projection);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            predicate = in.readObject();
            projection = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.LOCAL_MAP_QUERY_READER_FUNCTION;
        }
    }

    @Nonnull
    public static ProcessorSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        String clientXml = ImdgUtil.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, new RemoteMapReaderFunction(mapName));
    }

    public static class RemoteMapReaderFunction implements FunctionEx<HazelcastInstance,
            ReadMapOrCacheP.Reader<ClientInvocationFuture, MapFetchEntriesCodec.ResponseParameters, Entry<Data, Data>>>,
            IdentifiedDataSerializable {
        private String mapName;

        public RemoteMapReaderFunction() {
        }

        public RemoteMapReaderFunction(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public ReadMapOrCacheP.Reader<ClientInvocationFuture, MapFetchEntriesCodec.ResponseParameters,
                Entry<Data, Data>> applyEx(HazelcastInstance hzInstance) throws Exception {
            return new RemoteMapReader(hzInstance, mapName);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.REMOTE_MAP_READER_FUNCTION;
        }
    }

    @Nonnull
    public static <K, V, T> ProcessorSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(Objects.requireNonNull(predicate), "predicate");
        checkSerializable(Objects.requireNonNull(projection), "projection");

        String clientXml = ImdgUtil.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, new RemoteMapQueryReaderFunction<>(mapName, predicate,
                projection));
    }

    public static class RemoteMapQueryReaderFunction<K, V, T> implements FunctionEx<HazelcastInstance,
            ReadMapOrCacheP.Reader<ClientInvocationFuture, MapFetchWithQueryCodec.ResponseParameters, Data>>,
            IdentifiedDataSerializable {

        private String mapName;
        private Predicate<? super K, ? super V> predicate;
        private Projection<? super Entry<K, V>, ? extends T> projection;

        public RemoteMapQueryReaderFunction() {
        }

        public RemoteMapQueryReaderFunction(String mapName, Predicate<? super K, ? super V> predicate,
                                            Projection<? super Entry<K, V>, ? extends T> projection) {
            this.mapName = mapName;
            this.predicate = predicate;
            this.projection = projection;
        }

        @Override
        public ReadMapOrCacheP.Reader<ClientInvocationFuture, MapFetchWithQueryCodec.ResponseParameters, Data>
        applyEx(HazelcastInstance hzInstance) throws Exception {
            return new RemoteMapQueryReader(hzInstance, mapName, predicate, projection);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeObject(predicate);
            out.writeObject(projection);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            predicate = in.readObject();
            projection = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.REMOTE_MAP_QUERY_READER_FUNCTION;
        }
    }

    public static ProcessorMetaSupplier localOrRemoteListSupplier(String name, ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        Permission permission = PermissionsUtil.listReadPermission(clientXml, name);
        return forceTotalParallelismOne(
                ProcessorSupplier.of(SecuredFunctions.readListProcessorFn(name, clientXml)), name, permission);
    }
}
