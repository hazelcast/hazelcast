/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.Client;
import com.hazelcast.client.impl.protocol.ClientExceptions;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.statistics.ClientStatistics;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.AddressChecker;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.transaction.TransactionManagerService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

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

    @Nonnull
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

    ClientEndpointManager getEndpointManager();

    ClientExceptions getClientExceptions();

    SecurityContext getSecurityContext();

    TransactionManagerService getTransactionManagerService();

    ClusterViewListenerService getClusterListenerService();

    /**
     * Returns Map which contains number of connected clients to the cluster.
     *
     * The returned map can be used to get information about connected clients to the cluster.
     *
     * @return {@code Map&lt;String, Integer&gt;}.
     */
    Map<String, Integer> getConnectedClientStats();

    /**
     * Returns the latest client statistics mapped to the client UUIDs.
     *
     * @return map of the client statistics
     */
    Map<UUID, ClientStatistics> getClientStatistics();

    /**
     * @param client to check if allowed through current ClientSelector.
     *               <p>
     *               Note: Management Center clients ({@link ConnectionType#MC_JAVA_CLIENT}) are always allowed.
     * @return true if allowed, false otherwise
     */
    boolean isClientAllowed(Client client);

    /**
     * Only Clients that can pass through filter are allowed to connect to cluster.
     * Only one selector can be active at a time. Applying new one will override old selector.
     * <p>
     * Note: the only exception to this rule are Management Center clients ({@link ConnectionType#MC_JAVA_CLIENT}).
     *
     * @param selector to select a client or group of clients to act upon
     */
    void applySelector(ClientSelector selector);

    /**
     * Notify client engine that a client with given uuid required a resource (lock) on this member
     *
     * @param uuid client uuid
     */
    void onClientAcquiredResource(UUID uuid);

    void addBackupListener(UUID clientUUID, Consumer<Long> backupListener);

    boolean deregisterBackupListener(UUID clientUUID);

    void dispatchBackupEvent(UUID clientUUID, long clientCorrelationId);

    AddressChecker getManagementTasksChecker();
}
