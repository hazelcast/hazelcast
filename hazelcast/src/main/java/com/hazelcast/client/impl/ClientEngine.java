/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.protocol.ClientExceptions;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientType;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.util.function.Consumer;

import java.util.Collection;
import java.util.Map;

/**
 * The client Engine.
 *
 * todo: what is the purpose of the client engine.
 */
public interface ClientEngine extends Consumer<ClientMessage> {

    /**
     * Registers client endpoint to endpointManager.
     * Only authenticated endpoints should be registered here.
     * bind can be called twice for same connection, as long as client is allowed to be registered all calls to this
     * method returns true
     *
     * A selector could prevent endpoint to be registered
     * see {@link #applySelector}
     *
     * @param endpoint to be registered to client engine
     * @return false if client is not allowed to join because of a selector, true otherwise
     */
    boolean bind(ClientEndpoint endpoint);

    Collection<Client> getClients();

    int getClientEndpointCount();

    IPartitionService getPartitionService();

    ClusterService getClusterService();

    EventService getEventService();

    ProxyService getProxyService();

    ILogger getLogger(Class clazz);

    /**
     * @return the address of this member that listens for CLIENT protocol connections. When advanced network config
     * is in use, it will be different from the MEMBER listening address reported eg by {@code Node.getThisAddress()}
     */
    Address getThisAddress();

    String getThisUuid();

    ClientEndpointManager getEndpointManager();

    ClientExceptions getClientExceptions();

    SecurityContext getSecurityContext();

    TransactionManagerService getTransactionManagerService();

    ClientPartitionListenerService getPartitionListenerService();

    /**
     * Returns Map which contains number of connected clients to the cluster.
     *
     * The returned map can be used to get information about connected clients to the cluster.
     *
     * @return Map<ClientType, Integer> .
     */
    Map<ClientType, Integer> getConnectedClientStats();

    /**
     * The statistics is a String that is composed of key=value pairs separated by ',' . The following characters are escaped in
     * IMap and ICache names by the escape character '\' : '=' '.' ',' '\'
     *
     * The statistics key identify the category and name of the statistics. It is formatted as:
     * mainCategory.subCategory.statisticName
     *
     * An e.g. Operating system committedVirtualMemorySize path would be: os.committedVirtualMemorySize
     *
     * The statistics key names can be one of the following (Used IMap named <example.fastmap> and ICache Named
     * <StatTestCacheName> and assuming that the Near Cache is configured):
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
     * @return Map of [client UUID String, client statistics String]
     */
    Map<String, String> getClientStatistics();

    String getOwnerUuid(String clientUuid);

    /**
     * @param client to check if allowed through current ClientSelector
     * @return true if allowed, false otherwise
     */
    boolean isClientAllowed(Client client);

    /**
     * Only Clients that can pass through filter are allowed to connect to cluster.
     * Only one selector can be active at a time. Applying new one will override old selector.
     *
     * @param selector to select a client or group of clients to act upon
     */
    void applySelector(ClientSelector selector);


    /**
     * Locates the cluster member that has the provided client address and returns its member address,
     * to be used for intra-cluster communication. This is required when clients deliver messages with
     * designated target members, since clients may be unaware of the actual member address (when
     * advanced network config is enabled).
     * Throws a {@link com.hazelcast.spi.exception.TargetNotMemberException} when no member with the
     * provided client address can be located.
     *
     * @param clientAddress the client address of the member
     * @return the member address of the member
     */
    Address memberAddressOf(Address clientAddress);

    /**
     * Locates the client address of the given member address. Performs the reverse transformation
     * of {@link #memberAddressOf(Address)}.
     *
     * @param memberAddress the member address of the member
     * @return the client address of the member
     */
    Address clientAddressOf(Address memberAddress);
}
