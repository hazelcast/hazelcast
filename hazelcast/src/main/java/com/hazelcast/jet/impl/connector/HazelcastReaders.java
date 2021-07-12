/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
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
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
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
                (hzInstance, serializationService) -> new LocalCacheReader(hzInstance, serializationService, cacheName)
        ) {
            @Override
            public Permission getRequiredPermission() {
                return new CachePermission(cacheName, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    @Nonnull
    public static ProcessorSupplier readRemoteCacheSupplier(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        String clientXml = ImdgUtil.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, hzInstance -> new RemoteCacheReader(hzInstance, cacheName));
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalMapSupplier(@Nonnull String mapName) {
        return new LocalProcessorMetaSupplier<
                InternalCompletableFuture<MapEntriesWithCursor>, MapEntriesWithCursor, Entry<Data, Data>>(
                (hzInstance, serializationService) -> new LocalMapReader(hzInstance, serializationService, mapName)
        ) {
            @Override
            public Permission getRequiredPermission() {
                return new MapPermission(mapName, ACTION_CREATE, ACTION_READ);
            }
        };
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
                (hzInstance, serializationService) ->
                        new LocalMapQueryReader(hzInstance, serializationService, mapName, predicate, projection)
        ) {
            @Override
            public Permission getRequiredPermission() {
                return new MapPermission(mapName, ACTION_CREATE, ACTION_READ);
            }
        };
    }

    @Nonnull
    public static ProcessorSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        String clientXml = ImdgUtil.asXmlString(clientConfig);
        return new RemoteProcessorSupplier<>(clientXml, hzInstance -> new RemoteMapReader(hzInstance, mapName));
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
        return new RemoteProcessorSupplier<>(clientXml,
                hzInstance -> new RemoteMapQueryReader(hzInstance, mapName, predicate, projection));
    }

    public static ProcessorMetaSupplier localOrRemoteListSupplier(String name, ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        Permission permission = PermissionsUtil.listReadPermission(clientConfig, name);
        return forceTotalParallelismOne(ProcessorSupplier.of(() -> new ReadIListP(name, clientXml)), name, permission);
    }
}
