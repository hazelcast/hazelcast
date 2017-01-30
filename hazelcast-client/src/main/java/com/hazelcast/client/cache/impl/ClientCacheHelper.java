/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.LegacyCacheConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

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
            ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);
            Future<ClientMessage> future = clientInvocation.invoke();
            ClientMessage responseMessage = future.get();
            SerializationService serializationService = client.getSerializationService();

            return deserializeCacheConfig(client, responseMessage, serializationService, clientInvocation);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static <K, V> CacheConfig<K, V> deserializeCacheConfig(HazelcastClientInstanceImpl client,
                                                                   ClientMessage responseMessage,
                                                                   SerializationService serializationService,
                                                                   ClientInvocation clientInvocation) {
        Data responseData = CacheGetConfigCodec.decodeResponse(responseMessage).response;
        ClientConnection sendConnection = clientInvocation.getSendConnection();
        if (null != sendConnection && BuildInfo.UNKNOWN_HAZELCAST_VERSION == sendConnection.getConnectedServerVersion()) {
            boolean compatibilityEnabled = client.getProperties().getBoolean(ClientProperty.COMPATIBILITY_3_6_SERVER_ENABLED);
            if (compatibilityEnabled) {
                LegacyCacheConfig<K, V> legacyConfig = serializationService.toObject(responseData, LegacyCacheConfig.class);
                if (null == legacyConfig) {
                    return null;
                }
                return legacyConfig.getConfigAndReset();
            }
        }

        return serializationService.toObject(responseData);
    }

    /**
     * Sends the cache configuration to server.
     *
     * @param client             the client instance which will send the operation to server
     * @param configs            {@link ConcurrentMap} based store that holds cache configurations
     * @param cacheName          full cache name with prefixes
     * @param newCacheConfig     the cache configuration to be sent to server
     * @param createAlsoOnOthers flag which represents whether cache config
     *                           will be sent to other nodes by the target node
     * @param syncCreate         flag which represents response will be waited from the server
     * @param <K>                type of the key of the cache
     * @param <V>                type of the value of the cache
     * @return the created cache configuration
     */
    static <K, V> CacheConfig<K, V> createCacheConfig(HazelcastClientInstanceImpl client,
                                                      ConcurrentMap<String, CacheConfig> configs,
                                                      String cacheName,
                                                      CacheConfig<K, V> newCacheConfig,
                                                      boolean createAlsoOnOthers,
                                                      boolean syncCreate) {
        try {
            CacheConfig<K, V> currentCacheConfig = configs.get(cacheName);
            int partitionId = client.getClientPartitionService().getPartitionId(newCacheConfig.getNameWithPrefix());

            Object resolvedConfig = resolveCacheConfig(client, newCacheConfig, partitionId);

            Data configData = client.getSerializationService().toData(resolvedConfig);
            ClientMessage request = CacheCreateConfigCodec.encodeRequest(configData, createAlsoOnOthers);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);
            Future<ClientMessage> future = clientInvocation.invoke();
            if (syncCreate) {
                final ClientMessage response = future.get();
                final Data data = CacheCreateConfigCodec.decodeResponse(response).response;
                return resolveCacheConfig(client, clientInvocation, data);
            } else {
                return currentCacheConfig;
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static <K, V> CacheConfig<K, V> resolveCacheConfig(HazelcastClientInstanceImpl client,
                                                               ClientInvocation clientInvocation, Data configData) {
        ClientConnection sendConnection = clientInvocation.getSendConnection();
        if (null != sendConnection && BuildInfo.UNKNOWN_HAZELCAST_VERSION == sendConnection.getConnectedServerVersion()) {
            boolean compatibilityEnabled = client.getProperties().getBoolean(ClientProperty.COMPATIBILITY_3_6_SERVER_ENABLED);
            if (compatibilityEnabled) {
                LegacyCacheConfig legacyConfig = client.getSerializationService().toObject(configData, LegacyCacheConfig.class);
                if (null == legacyConfig) {
                    return null;
                }
                return legacyConfig.getConfigAndReset();
            }
        }
        return client.getSerializationService().toObject(configData);
    }

    private static <K, V> Object resolveCacheConfig(HazelcastClientInstanceImpl client, CacheConfig<K, V> newCacheConfig,
                                                         int partitionId)
            throws IOException {
        ClientConnection sendConnection = client.getInvocationService().getConnection(partitionId);
        if (null != sendConnection && BuildInfo.UNKNOWN_HAZELCAST_VERSION == sendConnection.getConnectedServerVersion()) {
            boolean compatibilityEnabled = client.getProperties().getBoolean(ClientProperty.COMPATIBILITY_3_6_SERVER_ENABLED);
            if (compatibilityEnabled) {
                return new LegacyCacheConfig<K, V>(newCacheConfig);
            }
        }
        return newCacheConfig;
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
                Address address = member.getAddress();
                ClientMessage request = CacheManagementConfigCodec.encodeRequest(cacheName, statOrMan, enabled, address);
                ClientInvocation clientInvocation = new ClientInvocation(client, request, address);
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
