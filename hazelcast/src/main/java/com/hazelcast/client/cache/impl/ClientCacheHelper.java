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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec;
import com.hazelcast.client.impl.protocol.codec.holder.CacheConfigHolder;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * Helper class for some client cache related stuff.
 */
final class ClientCacheHelper {
    private ClientCacheHelper() {
    }

    /**
     * Gets the cache configuration from the server.
     *
     * @param client          the client instance which will send the operation to server
     * @param cacheName       full cache name with prefixes
     * @param simpleCacheName pure cache name without any prefix
     * @param <K>             type of the key of the cache
     * @param <V>             type of the value of the cache
     * @return the cache configuration if it can be found
     */
    static <K, V> CacheConfig<K, V> getCacheConfig(HazelcastClientInstanceImpl client,
                                                   String cacheName, String simpleCacheName) {
        ClientMessage request = CacheGetConfigCodec.encodeRequest(cacheName, simpleCacheName);
        try {
            int partitionId = client.getClientPartitionService().getPartitionId(cacheName);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, cacheName, partitionId);
            Future<ClientMessage> future = clientInvocation.invoke();
            ClientMessage responseMessage = future.get();
            SerializationService serializationService = client.getSerializationService();

            CacheConfigHolder cacheConfigHolder = CacheGetConfigCodec.decodeResponse(responseMessage);
            if (cacheConfigHolder == null) {
                return null;
            }

            return cacheConfigHolder.asCacheConfig(serializationService);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Creates a new cache configuration on Hazelcast members.
     *
     * @param client         the client instance which will send the operation to server
     * @param newCacheConfig the cache configuration to be sent to server
     * @param <K>            type of the key of the cache
     * @param <V>            type of the value of the cache
     * @param urgent         whether creating the config is urgent or not(urgent messages can be send in DISCONNECTED state )
     * @return the created cache configuration
     * @see com.hazelcast.cache.impl.operation.AddCacheConfigOperation
     */
    static <K, V> CacheConfig<K, V> createCacheConfig(HazelcastClientInstanceImpl client,
                                                      CacheConfig<K, V> newCacheConfig, boolean urgent) {
        try {
            String nameWithPrefix = newCacheConfig.getNameWithPrefix();
            int partitionId = client.getClientPartitionService().getPartitionId(nameWithPrefix);

            InternalSerializationService serializationService = client.getSerializationService();
            ClientMessage request = CacheCreateConfigCodec
                    .encodeRequest(CacheConfigHolder.of(newCacheConfig, serializationService), true);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, nameWithPrefix, partitionId);
            Future<ClientMessage> future = urgent ? clientInvocation.invokeUrgent() : clientInvocation.invoke();
            final ClientMessage response = future.get();
            final CacheConfigHolder cacheConfigHolder = CacheCreateConfigCodec.decodeResponse(response);
            if (cacheConfigHolder == null) {
                return null;
            }
            return cacheConfigHolder.asCacheConfig(serializationService);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Enables/disables statistics or management support of cache on the all servers in the cluster.
     *
     * @param client    the client instance which will send the operation to server
     * @param cacheName full cache name with prefixes
     * @param statOrMan flag that represents which one of the statistics or management will be enabled
     * @param enabled   flag which represents whether it is enable or disable
     */
    static void enableStatisticManagementOnNodes(HazelcastClientInstanceImpl client, String cacheName,
                                                 boolean statOrMan, boolean enabled) {
        Collection<Member> members = client.getClientClusterService().getMemberList();
        Collection<Future> futures = new ArrayList<Future>();
        for (Member member : members) {
            try {
                UUID uuid = member.getUuid();
                ClientMessage request = CacheManagementConfigCodec.encodeRequest(cacheName, statOrMan, enabled, uuid);
                ClientInvocation clientInvocation = new ClientInvocation(client, request, cacheName, uuid);
                Future<ClientMessage> future = clientInvocation.invoke();
                futures.add(future);
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }
        // make sure all configs are created
        FutureUtil.waitWithDeadline(futures, CacheProxyUtil.AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
