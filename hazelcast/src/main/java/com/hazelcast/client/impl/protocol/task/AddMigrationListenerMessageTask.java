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
import com.hazelcast.client.impl.protocol.codec.ClientAddMigrationListenerCodec;
import com.hazelcast.client.impl.proxy.PartitionServiceProxy.MigrationProcessState;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;

import javax.annotation.Nullable;
import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class AddMigrationListenerMessageTask
        extends AbstractAddListenerMessageTask<Boolean> {

    public AddMigrationListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        IPartitionService partitionService = getService(getServiceName());

        MigrationListener listener = createMigrationListener();

        if (parameters) {
            return newCompletedFuture(partitionService.addLocalMigrationListener(listener));
        }

        return partitionService.addMigrationListenerAsync(listener);
    }

    private MigrationListener createMigrationListener() {
        return new MigrationListener() {

                @Override
                public void migrationStarted(MigrationState state) {
                    sendIfAlive(encodeMigrationEvent(state, MigrationProcessState.STARTED));
                }

                @Override
                public void migrationFinished(MigrationState state) {
                    sendIfAlive(encodeMigrationEvent(state, MigrationProcessState.FINISHED));
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
    }

    private void sendIfAlive(ClientMessage eventMessage) {
        if (endpoint.isAlive()) {
            // use a constant "key" to enforce migration event handling order
            sendClientMessage(MIGRATION_EVENT_TOPIC.hashCode(), eventMessage);
        }
    }

    private ClientMessage encodeReplicaMigrationEvent(ReplicaMigrationEvent event) {
        return ClientAddMigrationListenerCodec.encodeReplicaMigrationEvent(
                event.getMigrationState(),
                event.getPartitionId(),
                event.getReplicaIndex(),
                getMemberUuid(event.getSource()),
                getMemberUuid(event.getDestination()),
                event.isSuccess(),
                event.getElapsedTime()
        );
    }

    @Nullable
    private UUID getMemberUuid(@Nullable Member source) {
        return source != null ? source.getUuid() : null;
    }

    private ClientMessage encodeMigrationEvent(MigrationState migrationState, MigrationProcessState processState) {
        return ClientAddMigrationListenerCodec.encodeMigrationEvent(migrationState, processState.getId());
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
