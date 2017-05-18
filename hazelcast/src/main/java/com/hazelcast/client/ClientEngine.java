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

import java.util.List;
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
     * The statistics key paths can be one of the following (An example for an IMap named StatTestMapName and ICache Named
     * StatTestCacheName and near cache is configured):
     *
     *   /runtime/maxMemory
     *   /nearcache/StatTestMapName/LastPersistenceWrittenBytes
     *   /nearcache/StatTestMapName/Misses
     *   /nearcache/StatTestMapName/OwnedEntryCount
     *   /os/systemLoadAverage
     *   /nearcache/StatTestMapName/LastPersistenceKeyCount
     *   /nearcache/StatTestMapName/LastPersistenceFailure
     *   /nearcache/hz/StatTestCacheName/Expirations
     *   /nearcache/hz/StatTestCacheName/LastPersistenceFailure
     *   /runtime/availableProcessors
     *   /runtime/freeMemory
     *   /nearcache/hz/StatTestCacheName/OwnedEntryCount
     *   /os/processCpuTime
     *   /nearcache/StatTestMapName/LastPersistenceTime
     *   /os/processCpuLoad
     *   /nearcache/hz/StatTestCacheName/Hits
     *   /nearcache/StatTestMapName/LastPersistenceDuration
     *   /nearcache/hz/StatTestCacheName/LastPersistenceDuration
     *   /nearcache/hz/StatTestCacheName/OwnedEntryMemoryCost
     *   /os/systemCpuLoad
     *   /runtime/uptime
     *   /os/freePhysicalMemorySize
     *   /os/committedVirtualMemorySize
     *   /os/maxFileDescriptorCount
     *   /runtime/usedMemory
     *   /nearcache/hz/StatTestCacheName/CreationTime
     *   /nearcache/StatTestMapName/Evictions
     *   //userExecutor/queueSize
     *   /os/totalSwapSpaceSize
     *   /runtime/totalMemory
     *   /os/openFileDescriptorCount
     *   /nearcache/StatTestMapName/Expirations
     *   /nearcache/hz/StatTestCacheName/LastPersistenceKeyCount
     *   /nearcache/hz/StatTestCacheName/Evictions
     *   /userExecutor/queueSize
     *   /os/freeSwapSpaceSize
     *   /nearcache/StatTestMapName/Hits
     *   /nearcache/StatTestMapName/OwnedEntryMemoryCost
     *   /nearcache/hz/StatTestCacheName/Misses
     *   /nearcache/StatTestMapName/CreationTime
     *   /nearcache/hz/StatTestCacheName/LastPersistenceWrittenBytes
     *   /os/totalPhysicalMemorySize
     *   /nearcache/hz/StatTestCacheName/LastPersistenceTime
     *   /ClusterConnectionTimestamp
     *   /ClientType
     *
     * Not: Please observe that the name for the ICache appears to be the hazelcast instance name "hz" followed by "/" and
     * followed by the cache name provided in the application which is StatTestCacheName.
     *
     * @return List of [client UUID String, List of statistics key path and value]
     */
    List<Map.Entry<String, List<Map.Entry<String, String>>>> getClientStatistics();

    String getOwnerUuid(String clientUuid);

    void handleClientMessage(ClientMessage message, Connection connection);
}
