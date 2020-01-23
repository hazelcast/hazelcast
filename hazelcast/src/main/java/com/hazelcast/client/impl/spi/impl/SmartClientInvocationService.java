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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.nio.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientLocalBackupListenerCodec;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.nio.Connection;

import java.io.IOException;
import java.util.UUID;

public class SmartClientInvocationService extends AbstractClientInvocationService {

    private static ListenerMessageCodec backupListener = new ListenerMessageCodec() {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return ClientLocalBackupListenerCodec.encodeRequest();
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return ClientLocalBackupListenerCodec.decodeResponse(clientMessage).response;
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return null;
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return false;
        }
    };
    private boolean isBackupAckToClientEnabled;

    public SmartClientInvocationService(HazelcastClientInstanceImpl client) {
        super(client);
    }

    public void addBackupListener() {
        isBackupAckToClientEnabled = client.getClientConfig().isBackupAckToClientEnabled();
        if (isBackupAckToClientEnabled) {
            ClientListenerService listenerService = client.getListenerService();
            listenerService.registerListener(backupListener, new BackupEventHandler());
        }
    }

    public class BackupEventHandler extends ClientLocalBackupListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        @Override
        public void handleBackupEvent(long sourceInvocationCorrelationId) {
            ClientInvocation invocation = getInvocation(sourceInvocationCorrelationId);
            if (invocation == null) {
                if (invocationLogger.isFinestEnabled()) {
                    invocationLogger.finest("Invocation not found for backup event, invocation id "
                            + sourceInvocationCorrelationId);
                }
                return;
            }
            invocation.notifyBackupComplete();
        }
    }

    @Override
    public void invokeOnPartitionOwner(ClientInvocation invocation, int partitionId) throws IOException {
        UUID partitionOwner = partitionService.getPartitionOwner(partitionId);
        if (partitionOwner == null) {
            if (invocationLogger.isFinestEnabled()) {
                invocationLogger.finest("Partition owner is not assigned yet, Retrying on random target");
            }
            invokeOnRandomTarget(invocation);
            return;
        }
        invokeOnTarget(invocation, partitionOwner);
    }

    @Override
    public void invokeOnRandomTarget(ClientInvocation invocation) throws IOException {
        Connection connection = connectionManager.getRandomConnection();
        if (connection == null) {
            throw new IOException("No connection found to invoke");
        }
        send0(invocation, (ClientConnection) connection);
    }

    @Override
    public void invokeOnTarget(ClientInvocation invocation, UUID uuid) throws IOException {
        assert (uuid != null);
        Member member = client.getClientClusterService().getMember(uuid);
        if (member == null) {
            if (invocationLogger.isFinestEnabled()) {
                invocationLogger.finest("Target : " + uuid + " is not in the member list, Retrying on random target");
            }
            invokeOnRandomTarget(invocation);
            return;
        }
        Connection connection = getConnection(member.getUuid());
        invokeOnConnection(invocation, (ClientConnection) connection);
    }

    private Connection getConnection(UUID target) throws IOException {
        Connection connection = connectionManager.getConnection(target);
        if (connection == null) {
            throw new IOException("No available connection to member " + target);
        }
        return connection;
    }

    @Override
    public void invokeOnConnection(ClientInvocation invocation, ClientConnection connection) throws IOException {
        send0(invocation, connection);
    }

    private void send0(ClientInvocation invocation, ClientConnection connection) throws IOException {
        if (isBackupAckToClientEnabled) {
            invocation.getClientMessage().getStartFrame().flags |= ClientMessage.BACKUP_AWARE_FLAG;
        }
        send(invocation, connection);
    }
}
