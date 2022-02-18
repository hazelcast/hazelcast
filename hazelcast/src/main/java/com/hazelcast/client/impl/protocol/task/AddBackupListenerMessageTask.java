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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientLocalBackupListenerCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class AddBackupListenerMessageTask
        extends AbstractAddListenerMessageTask<Void>
        implements Consumer<Long> {

    public AddBackupListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean acceptOnIncompleteStart() {
        return true;
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        UUID uuid = endpoint.getUuid();
        if (logger.isFinestEnabled()) {
            logger.finest("Client is adding backup listener. client uuid " + uuid);
        }
        clientEngine.addBackupListener(uuid, this);

        return newCompletedFuture(uuid);
    }

    @Override
    protected void addDestroyAction(UUID registrationId) {
        endpoint.addDestroyAction(registrationId, () -> clientEngine.deregisterBackupListener(registrationId, this));
    }

    @Override
    public void accept(Long backupCorrelationId) {
        ClientMessage eventMessage = ClientLocalBackupListenerCodec.encodeBackupEvent(backupCorrelationId);
        eventMessage.getStartFrame().flags |= ClientMessage.BACKUP_EVENT_FLAG;
        sendClientMessage(eventMessage);
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientLocalBackupListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    public Permission getRequiredPermission() {
        return null;
    }

}
