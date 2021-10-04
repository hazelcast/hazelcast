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

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheEntriesWithCursor;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalCacheReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalMapQueryReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.NonExistentCacheReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.NonExistentMapQueryReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.NonExistentMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.NonExistingRemoteCacheReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.NonExistingRemoteMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.NonExistingRemoteQueryMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteCacheReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteMapQueryReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteMapReader;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.RemoteProcessorSupplier;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.MapPermission;

import javax.annotation.Nonnull;
import java.security.Permission;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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
                CompletableFuture<CacheEntriesWithCursor>, CacheEntriesWithCursor, Entry<Data, Data>>(
                (hzInstance, serializationService) -> {
                    String name = HazelcastCacheManager.CACHE_MANAGER_PREFIX + cacheName;
                    if (existsDistributedObject(hzInstance, CacheService.SERVICE_NAME, name)) {
                        return new LocalCacheReader(hzInstance, serializationService, cacheName);
                    }
                    return new NonExistentCacheReader(cacheName);
                }
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
        return new RemoteProcessorSupplier<>(clientXml, hzInstance -> {
            String name = HazelcastCacheManager.CACHE_MANAGER_PREFIX + cacheName;
            if (existsDistributedObject(hzInstance, CacheService.SERVICE_NAME, name)) {
                return new RemoteCacheReader(hzInstance, cacheName);
            }
            return new NonExistingRemoteCacheReader(hzInstance, cacheName);
        });
    }

    @Nonnull
    public static ProcessorMetaSupplier readLocalMapSupplier(@Nonnull String mapName) {
        return new LocalProcessorMetaSupplier<
                CompletableFuture<MapEntriesWithCursor>, MapEntriesWithCursor, Entry<Data, Data>>(
                (hzInstance, serializationService) -> {
                    if (existsDistributedObject(hzInstance, MapService.SERVICE_NAME, mapName)) {
                        return new LocalMapReader(hzInstance, serializationService, mapName);

                    }
                    return new NonExistentMapReader(mapName);
                }
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

        return new LocalProcessorMetaSupplier<CompletableFuture<ResultSegment>, ResultSegment, QueryResultRow>(
                (hzInstance, serializationService) -> {
                    if (existsDistributedObject(hzInstance, MapService.SERVICE_NAME, mapName)) {
                        return new LocalMapQueryReader(hzInstance, serializationService, mapName, predicate, projection);

                    }
                    return new NonExistentMapQueryReader(mapName);
                }
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
        return new RemoteProcessorSupplier<>(clientXml, hzInstance -> {
            if (existsDistributedObject(hzInstance, MapService.SERVICE_NAME, mapName)) {
                return new RemoteMapReader(hzInstance, mapName);
            }
            return new NonExistingRemoteMapReader(hzInstance, mapName);
        });
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
                hzInstance -> {
                    if (existsDistributedObject(hzInstance, MapService.SERVICE_NAME, mapName)) {
                        return new RemoteMapQueryReader(hzInstance, mapName, predicate, projection);
                    }
                    return new NonExistingRemoteQueryMapReader(hzInstance, mapName);
                });
    }

    public static ProcessorMetaSupplier localOrRemoteListSupplier(String name, ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        Permission permission = PermissionsUtil.listReadPermission(clientXml, name);
        return forceTotalParallelismOne(
                ProcessorSupplier.of(SecuredFunctions.readListProcessorFn(name, clientXml)), name, permission);
    }

    private static boolean existsDistributedObject(HazelcastInstance instance, String serviceName, String name) {
        return ((AbstractJetInstance) instance.getJet()).existsDistributedObject(serviceName, name);
    }
}
