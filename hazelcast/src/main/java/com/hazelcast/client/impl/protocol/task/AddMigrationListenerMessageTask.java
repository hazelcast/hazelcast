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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddMigrationListenerCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.partition.ReplicaMigrationEvent;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.internal.partition.MigrationEventHandler.MIGRATION_FINISHED;
import static com.hazelcast.internal.partition.MigrationEventHandler.MIGRATION_STARTED;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class AddMigrationListenerMessageTask
        extends AbstractAddListenerMessageTask<Boolean> {

    public AddMigrationListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final IPartitionService partitionService = getService(getServiceName());


        final MigrationListener listener = new MigrationListener() {

            @Override
            public void migrationStarted(MigrationState state) {
                sendIfAlive(encodeMigrationEvent(state, MIGRATION_STARTED));
            }

            @Override
            public void migrationFinished(MigrationState state) {
                sendIfAlive(encodeMigrationEvent(state, MIGRATION_FINISHED));
            }

            @Override
            public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
                sendIfAlive(encodeReplicaMigrationEvent(event));
            }

            @Override
            public void replicaMigrationFailed(ReplicaMigrationEvent event) {
                sendIfAlive(encodeReplicaMigrationEvent(event));
            }
        };

        if (parameters) {
            return newCompletedFuture(partitionService.addLocalMigrationListener(listener));
        }

        return partitionService.addMigrationListenerAsync(listener);
    }

    private void sendIfAlive(ClientMessage eventMessage) {
        if (endpoint.isAlive()) {
            sendClientMessage(null, eventMessage);
        }
    }

    private ClientMessage encodeReplicaMigrationEvent(ReplicaMigrationEvent event) {
        return ClientAddMigrationListenerCodec.encodeReplicaMigrationEvent(
                new MigrationStateImpl(
                        event.getMigrationState().getStartTime(),
                        event.getMigrationState().getPlannedMigrations(),
                        event.getMigrationState().getCompletedMigrations(),
                        event.getMigrationState().getTotalElapsedTime()
                ),
                event.getPartitionId(),
                event.getReplicaIndex(),
                event.getSource() != null ? event.getSource().getUuid() : null,
                event.getDestination() != null ? event.getDestination().getUuid() : null,
                event.isSuccess(),
                event.getElapsedTime()
        );
    }

    private ClientMessage encodeMigrationEvent(MigrationState event, int partitionId) {
        return ClientAddMigrationListenerCodec.encodeMigrationEvent(
                new MigrationStateImpl(
                        event.getStartTime(),
                        event.getPlannedMigrations(),
                        event.getCompletedMigrations(),
                        event.getTotalElapsedTime()
                ),
                partitionId
        );
    }

    @Override
    protected Boolean decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddMigrationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddMigrationListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return MIGRATION_EVENT_TOPIC;
    }

    @Override
    public String getMethodName() {
        return "addMigrationListener";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
