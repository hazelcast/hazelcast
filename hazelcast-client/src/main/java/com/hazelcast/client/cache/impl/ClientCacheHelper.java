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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec;
import com.hazelcast.client.spi.impl.AbstractClientInvocationService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.LegacyCacheConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.StringUtil.timeToString;

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
     * Creates a new cache configuration on Hazelcast members.
     *
     * @param client             the client instance which will send the operation to server
     * @param newCacheConfig     the cache configuration to be sent to server
     * @param <K>                type of the key of the cache
     * @param <V>                type of the value of the cache
     * @return the created cache configuration
     * @see com.hazelcast.cache.impl.operation.CacheCreateConfigOperation
     */
    static <K, V> CacheConfig<K, V> createCacheConfig(HazelcastClientInstanceImpl client,
                                                      CacheConfig<K, V> newCacheConfig) {
        try {
            String nameWithPrefix = newCacheConfig.getNameWithPrefix();
            int partitionId = client.getClientPartitionService().getPartitionId(nameWithPrefix);

            Object resolvedConfig = resolveCacheConfigWithRetry(client, newCacheConfig, partitionId);

            Data configData = client.getSerializationService().toData(resolvedConfig);
            ClientMessage request = CacheCreateConfigCodec.encodeRequest(configData, true);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, nameWithPrefix, partitionId);
            Future<ClientMessage> future = clientInvocation.invoke();
            final ClientMessage response = future.get();
            final Data data = CacheCreateConfigCodec.decodeResponse(response).response;
            return resolveCacheConfig(client, clientInvocation, data);
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

        Address address = getSendAddress(client, partitionId);
        ClientConnection sendConnection = (ClientConnection) client.getConnectionManager().getOrConnect(address);
        if (BuildInfo.UNKNOWN_HAZELCAST_VERSION == sendConnection.getConnectedServerVersion()) {
            boolean compatibilityEnabled = client.getProperties().getBoolean(ClientProperty.COMPATIBILITY_3_6_SERVER_ENABLED);
            if (compatibilityEnabled) {
                return new LegacyCacheConfig<K, V>(newCacheConfig);
            }
        }
        return newCacheConfig;
    }

    private static Address getSendAddress(HazelcastClientInstanceImpl client, int partitionId) throws IOException {
        Address address;
        if (client.getClientConfig().getNetworkConfig().isSmartRouting()) {
            address = client.getClientPartitionService().getPartitionOwner(partitionId);
            if (address == null) {
                throw new IOException("Partition does not have an owner. partitionId: " + partitionId);
            }
        } else {
            address = client.getConnectionManager().getOwnerConnectionAddress();
            if (address == null) {
                throw new IOException("NonSmartClientInvocationService: Owner connection is not available.");
            }
        }
        return address;
    }

    private static <K, V> Object resolveCacheConfigWithRetry(HazelcastClientInstanceImpl client,
                                                             CacheConfig<K, V> newCacheConfig, int partitionId) {
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) client.getInvocationService();
        long invocationRetryPauseMillis = invocationService.getInvocationRetryPauseMillis();
        long invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();
        long startMillis = System.currentTimeMillis();
        Exception lastException;
        do {
            try {
                return resolveCacheConfig(client, newCacheConfig, partitionId);
            } catch (Exception e) {
                lastException = e;
            }

            timeOutOrSleepBeforeNextTry(startMillis, invocationRetryPauseMillis, invocationTimeoutMillis, lastException);

        } while (client.getLifecycleService().isRunning());
        throw new HazelcastClientNotActiveException("Client is shut down");
    }

    private static void sleepBeforeNextTry(long invocationRetryPauseMillis) {
        try {
            Thread.sleep(invocationRetryPauseMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static void timeOutOrSleepBeforeNextTry(long startMillis, long invocationRetryPauseMillis,
                                                    long invocationTimeoutMillis, Exception lastException) {
        long nowInMillis = System.currentTimeMillis();
        long elapsedMillis = nowInMillis - startMillis;
        boolean timedOut = elapsedMillis > invocationTimeoutMillis;

        if (timedOut) {
            throwOperationTimeoutException(startMillis, nowInMillis, elapsedMillis, invocationTimeoutMillis, lastException);
        } else {
            sleepBeforeNextTry(invocationRetryPauseMillis);
        }
    }

    private static void throwOperationTimeoutException(long startMillis, long nowInMillis,
                                                       long elapsedMillis, long invocationTimeoutMillis,
                                                       Exception lastException) {
        throw new OperationTimeoutException("Creating cache config is timed out."
                + " Current time: " + timeToString(nowInMillis) + ", "
                + " Start time : " + timeToString(startMillis) + ", "
                + " Client invocation timeout : " + invocationTimeoutMillis + " ms, "
                + " Elapsed time : " + elapsedMillis + " ms. ", lastException);
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
                ClientInvocation clientInvocation = new ClientInvocation(client, request, cacheName, address);
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
