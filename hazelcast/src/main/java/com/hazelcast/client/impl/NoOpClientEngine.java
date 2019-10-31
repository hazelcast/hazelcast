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
import com.hazelcast.client.Client;
import com.hazelcast.client.ClientType;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.transaction.TransactionManagerService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class NoOpClientEngine implements ClientEngine {

    @Override
    public boolean bind(ClientEndpoint endpoint) {
        return true;
    }

    @Nonnull
    @Override
    public Collection<Client> getClients() {
        return emptyList();
    }

    @Override
    public int getClientEndpointCount() {
        return 0;
    }

    @Override
    public IPartitionService getPartitionService() {
        return null;
    }

    @Override
    public ClusterService getClusterService() {
        return null;
    }

    @Override
    public EventService getEventService() {
        return null;
    }

    @Override
    public ProxyService getProxyService() {
        return null;
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return null;
    }

    @Override
    public Address getThisAddress() {
        return null;
    }

    @Override
    public ClientEndpointManager getEndpointManager() {
        return null;
    }

    @Override
    public ClientExceptions getClientExceptions() {
        return null;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return null;
    }

    @Override
    public TransactionManagerService getTransactionManagerService() {
        return null;
    }

    @Override
    public ClientPartitionListenerService getPartitionListenerService() {
        return null;
    }

    @Override
    public Map<ClientType, Integer> getConnectedClientStats() {
        return emptyMap();
    }

    @Override
    public Map<UUID, String> getClientStatistics() {
        return emptyMap();
    }

    @Override
    public boolean isClientAllowed(Client client) {
        return true;
    }

    @Override
    public void applySelector(ClientSelector selector) {

    }

    @Override
    public void accept(ClientMessage clientMessage) {

    }

    @Override
    public Address memberAddressOf(Address clientAddress) {
        throw new TargetNotMemberException("NoOpClientEngine does not supply translation from client to "
                + "member address");
    }

    @Override
    public Address clientAddressOf(Address clientAddress) {
        throw new TargetNotMemberException("NoOpClientEngine does not supply translation from member to "
                + "client address");
    }

    @Override
    public void onClientAcquiredResource(UUID uuid) {

    }

    @Override
    public void addBackupListener(UUID clientUUID, Consumer<Long> backupListener) {

    }

    @Override
    public void dispatchBackupEvent(UUID clientUUID, long clientCorrelationId) {

    }

    @Override
    public boolean deregisterBackupListener(UUID clientUUID) {
        return false;
    }
}
