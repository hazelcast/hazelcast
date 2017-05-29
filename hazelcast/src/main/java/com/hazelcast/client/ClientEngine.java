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

package com.hazelcast.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.Config;
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map;

/**
 * The client Engine.
 *
 * todo: what is the purpose of the client engine.
 */
@PrivateApi
public interface ClientEngine {

    int getClientEndpointCount();

    IPartitionService getPartitionService();

    ClusterService getClusterService();

    EventService getEventService();

    ProxyService getProxyService();

    Config getConfig();

    ILogger getLogger(Class clazz);

    Address getMasterAddress();

    Address getThisAddress();

    String getThisUuid();

    MemberImpl getLocalMember();

    SecurityContext getSecurityContext();

    /**
     * Returns the SerializationService.
     *
     * @return the SerializationService
     */
    SerializationService getSerializationService();

    /**
     * Returns Map which contains number of connected clients to the cluster.
     *
     * The returned map can be used to get information about connected clients to the cluster.
     *
     * @return Map<ClientType,Integer> .
     */
    Map<ClientType, Integer> getConnectedClientStats();

    /**
     *
     * The statistics is a String that is composed of key=value pairs separated by ',' . The following characters are escaped in
     * IMap and ICache names by the escape character '\' : '=' '.' ',' '\'
     *
     * The statistics key identify the category and name of the statistics. It is formatted as:
     * mainCategory.subCategory.statisticName
     *
     * An e.g. Operating system committedVirtualMemorySize path would be: os.committedVirtualMemorySize
     *
     * The statistics key names can be one of the following (Used IMap named <example.fastmap> and ICache Named
     * <StatTestCacheName> and assuming that the near cache is configured):
     *
     * clientType
     * clusterConnectionTimestamp
     * credentials.principal
     * clientAddress
     * clientName
     * enterprise
     * lastStatisticsCollectionTime
     * nearcache.<example\.fastmap>.creationTime
     * nearcache.<example\.fastmap>.evictions
     * nearcache.<example\.fastmap>.expirations
     * nearcache.<example\.fastmap>.hits
     * nearcache.<example\.fastmap>.lastPersistenceDuration
     * nearcache.<example\.fastmap>.lastPersistenceFailure
     * nearcache.<example\.fastmap>.lastPersistenceKeyCount
     * nearcache.<example\.fastmap>.lastPersistenceTime
     * nearcache.<example\.fastmap>.lastPersistenceWrittenBytes
     * nearcache.<example\.fastmap>.misses
     * nearcache.<example\.fastmap>.ownedEntryCount
     * nearcache.<example\.fastmap>.ownedEntryMemoryCost
     * nearcache.hz/<StatTestCacheName>.creationTime
     * nearcache.hz/<StatTestCacheName>.evictions
     * nearcache.hz/<StatTestCacheName>.expirations
     * nearcache.hz/<StatTestCacheName>.hits
     * nearcache.hz/<StatTestCacheName>.lastPersistenceDuration
     * nearcache.hz/<StatTestCacheName>.lastPersistenceFailure
     * nearcache.hz/<StatTestCacheName>.lastPersistenceKeyCount
     * nearcache.hz/<StatTestCacheName>.lastPersistenceTime
     * nearcache.hz/<StatTestCacheName>.lastPersistenceWrittenBytes
     * nearcache.hz/<StatTestCacheName>.misses
     * nearcache.hz/<StatTestCacheName>.ownedEntryCount
     * nearcache.hz/<StatTestCacheName>.ownedEntryMemoryCost
     * os.committedVirtualMemorySize
     * os.freePhysicalMemorySize
     * os.freeSwapSpaceSize
     * os.maxFileDescriptorCount
     * os.openFileDescriptorCount
     * os.processCpuTime
     * os.systemLoadAverage
     * os.totalPhysicalMemorySize
     * os.totalSwapSpaceSize
     * runtime.availableProcessors
     * runtime.freeMemory
     * runtime.maxMemory
     * runtime.totalMemory
     * runtime.uptime
     * runtime.usedMemory
     * userExecutor.queueSize
     *
     * Not: Please observe that the name for the ICache appears to be the hazelcast instance name "hz" followed by "/" and
     * followed by the cache name provided which is StatTestCacheName.
     *
     *
     * @return Map of [client UUID String, client statistics String]
     */
    Map<String, String> getClientStatistics();

    String getOwnerUuid(String clientUuid);

    void handleClientMessage(ClientMessage message, Connection connection);
}
