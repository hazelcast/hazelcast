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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAddMigrationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemoveMigrationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.internal.partition.ReplicaMigrationEventImpl;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.ReplicaMigrationEvent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * @author mdogan 5/16/13
 */
public final class PartitionServiceProxy implements PartitionService {

    /**
     * Used for specifying the state of the migration process.
     */
    public enum MigrationProcessState {

        /**
         * Migration process started.
         */
        STARTED(0),

        /**
         * Migration process finished.
         */
        FINISHED(1);

        private final int id;

        MigrationProcessState(int id) {
            this.id = id;
        }

        @Nullable
        public static MigrationProcessState fromId(int id) {
            switch (id) {
                case 0:
                    return STARTED;
                case 1:
                    return FINISHED;
                default:
                    // For the possibility of future extension
                    // allow old readers to not throw exception
                    // for unknown values.
                    return null;
            }
        }

        public int getId() {
            return id;
        }
    }

    private final ClientPartitionService partitionService;
    private final ClientListenerService listenerService;
    private final ClientClusterService clusterService;

    public PartitionServiceProxy(ClientPartitionService partitionService,
                                 ClientListenerService listenerService,
                                 ClientClusterService clusterService) {
        this.partitionService = partitionService;
        this.listenerService = listenerService;
        this.clusterService = clusterService;
    }

    @Override
    public Set<Partition> getPartitions() {
        final int partitionCount = partitionService.getPartitionCount();
        Set<Partition> partitions = new LinkedHashSet<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            final Partition partition = partitionService.getPartition(i);
            partitions.add(partition);
        }
        return partitions;
    }

    @Override
    public Partition getPartition(@Nonnull Object key) {
        checkNotNull(key, "key cannot be null");
        final int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartition(partitionId);
    }

    @Override
    public UUID addMigrationListener(@Nonnull MigrationListener migrationListener) {
        checkNotNull(migrationListener, "migrationListener can't be null");
        EventHandler<ClientMessage> handler = new ClientMigrationEventHandler(migrationListener);
        return listenerService.registerListener(createMigrationListenerCodec(), handler);
    }

    @Override
    public boolean removeMigrationListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId can't be null");
        return listenerService.deregisterListener(registrationId);
    }

    @Override
    public UUID addPartitionLostListener(@Nonnull PartitionLostListener partitionLostListener) {
        checkNotNull(partitionLostListener, "migrationListener can't be null");
        EventHandler<ClientMessage> handler = new ClientPartitionLostEventHandler(partitionLostListener);
        return listenerService.registerListener(createPartitionLostListenerCodec(), handler);
    }

    @Override
    public boolean removePartitionLostListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId can't be null");
        return listenerService.deregisterListener(registrationId);
    }

    @Override
    public boolean isClusterSafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMemberSafe(Member member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocalMemberSafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    private ListenerMessageCodec createPartitionLostListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ClientAddPartitionLostListenerCodec.encodeRequest(localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ClientAddPartitionLostListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ClientRemovePartitionLostListenerCodec.encodeRequest(realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ClientRemovePartitionLostListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    private ListenerMessageCodec createMigrationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ClientAddMigrationListenerCodec.encodeRequest(localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ClientAddMigrationListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ClientRemoveMigrationListenerCodec.encodeRequest(realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ClientRemoveMigrationListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    private class ClientPartitionLostEventHandler extends ClientAddPartitionLostListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final PartitionLostListener listener;

        ClientPartitionLostEventHandler(PartitionLostListener listener) {
            this.listener = listener;
        }

        @Override
        public void handlePartitionLostEvent(int partitionId, int lostBackupCount, UUID source) {
            Member member = clusterService.getMember(source);
            listener.partitionLost(new PartitionLostEventImpl(partitionId, lostBackupCount, member.getAddress()));
        }
    }

    private class ClientMigrationEventHandler extends ClientAddMigrationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final MigrationListener listener;

        ClientMigrationEventHandler(MigrationListener listener) {
            this.listener = listener;
        }

        @Override
        public void handleMigrationEvent(MigrationState migrationState, int type) {
            MigrationProcessState migrationProcessState = MigrationProcessState.fromId(type);
            if (migrationProcessState == null) {
                // Do not throw exception for future extensions of the state type.
                return;
            }
            switch (migrationProcessState) {
                case STARTED:
                    listener.migrationStarted(migrationState);
                    break;
                case FINISHED:
                    listener.migrationFinished(migrationState);
                    break;
                default:
                    // We shouldn't hit this line as the future extensions
                    // should return early from the above check. Defining
                    // default clause to make checkstyle happy.
            }
        }

        @Override
        public void handleReplicaMigrationEvent(MigrationState migrationState,
                                                int partitionId,
                                                int replicaIndex,
                                                @Nullable UUID sourceUuid,
                                                @Nullable UUID destUuid,
                                                boolean success,
                                                long elapsedTime) {

            @Nullable Member source = findMember(sourceUuid);
            @Nullable Member destination = findMember(destUuid);

            ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(migrationState, partitionId,
                    replicaIndex, source, destination, success, elapsedTime);

            if (event.isSuccess()) {
                listener.replicaMigrationCompleted(event);
            } else {
                listener.replicaMigrationFailed(event);
            }
        }

        private Member findMember(@Nullable UUID memberUuid) {
            return memberUuid != null ? clusterService.getMember(memberUuid) : null;
        }
    }
}
