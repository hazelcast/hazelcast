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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionAwareService;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.ReplicaMigrationEventImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.partition.IPartitionService.SERVICE_NAME;
import static com.hazelcast.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.internal.partition.impl.MigrationListenerAdapter.MIGRATION_FINISHED;
import static com.hazelcast.internal.partition.impl.MigrationListenerAdapter.MIGRATION_STARTED;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.SYSTEM_EXECUTOR;

/**
 * Maintains registration of partition-system related listeners and dispatches corresponding events.
 */
public class PartitionEventManager {

    private final Node node;
    private final NodeEngineImpl nodeEngine;

    public PartitionEventManager(Node node) {
        this.node = node;
        this.nodeEngine = node.nodeEngine;
    }

    /** Sends a {@link ReplicaMigrationEvent} to the registered event listeners. */
    public void sendMigrationEvent(MigrationState state, MigrationInfo migrationInfo, long elapsed) {
        ClusterServiceImpl clusterService = node.getClusterService();
        PartitionReplica sourceReplica = migrationInfo.getSource();
        PartitionReplica destReplica = migrationInfo.getDestination();
        Member source = sourceReplica != null ? clusterService.getMember(sourceReplica.address(), sourceReplica.uuid()) : null;
        Member destination = clusterService.getMember(destReplica.address(), destReplica.uuid());

        int partitionId = migrationInfo.getPartitionId();
        int replicaIndex = migrationInfo.getDestinationNewReplicaIndex();
        boolean success = migrationInfo.getStatus() == MigrationStatus.SUCCESS;
        ReplicaMigrationEvent
                event = new ReplicaMigrationEventImpl(state, partitionId, replicaIndex, source, destination, success, elapsed);

        sendMigrationEvent(event);
    }

    public void sendMigrationProcessStartedEvent(MigrationState state) {
        ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(state, MIGRATION_STARTED, 0, null, null, false, 0L);
        sendMigrationEvent(event);
    }

    public void sendMigrationProcessCompletedEvent(MigrationState state) {
        ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(state, MIGRATION_FINISHED, 0, null, null, false, 0L);
        sendMigrationEvent(event);
    }

    private void sendMigrationEvent(ReplicaMigrationEvent event) {
        EventService eventService = nodeEngine.getEventService();
        // All migration events are sent in order.
        eventService.publishEvent(SERVICE_NAME, MIGRATION_EVENT_TOPIC, event, MIGRATION_EVENT_TOPIC.hashCode());
    }

    public UUID addMigrationListener(MigrationListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final MigrationListenerAdapter adapter = new MigrationListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, MIGRATION_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    public boolean removeMigrationListener(UUID registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, MIGRATION_EVENT_TOPIC, registrationId);
    }

    public UUID addPartitionLostListener(PartitionLostListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final PartitionLostListenerAdapter adapter = new PartitionLostListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    public CompletableFuture<UUID> addPartitionLostListenerAsync(PartitionLostListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final PartitionLostListenerAdapter adapter = new PartitionLostListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        return eventService.registerListenerAsync(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, adapter)
                           .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    public UUID addLocalPartitionLostListener(PartitionLostListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener can't be null");
        }

        final PartitionLostListenerAdapter adapter = new PartitionLostListenerAdapter(listener);

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration =
                eventService.registerLocalListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, adapter);
        return registration.getId();
    }

    public boolean removePartitionLostListener(UUID registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, registrationId);
    }

    public CompletableFuture<Boolean> removePartitionLostListenerAsync(UUID registrationId) {
        if (registrationId == null) {
            throw new NullPointerException("registrationId can't be null");
        }

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListenerAsync(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, registrationId);
    }

    public void onPartitionLost(IPartitionLostEvent event) {
        assert event instanceof PartitionLostEvent;
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService
                .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, event, event.getPartitionId());
    }

    public void sendPartitionLostEvent(int partitionId, int lostReplicaIndex) {
        IPartitionLostEvent event = new PartitionLostEventImpl(partitionId, lostReplicaIndex,
                nodeEngine.getThisAddress());
        InternalPartitionLostEventPublisher publisher = new InternalPartitionLostEventPublisher(nodeEngine, event);
        nodeEngine.getExecutionService().execute(SYSTEM_EXECUTOR, publisher);
    }

    /** Task which notifies all {@link PartitionAwareService}s that replicas have been lost. */
    private static class InternalPartitionLostEventPublisher
            implements Runnable {

        private final NodeEngineImpl nodeEngine;

        private final IPartitionLostEvent event;

        InternalPartitionLostEventPublisher(NodeEngineImpl nodeEngine, IPartitionLostEvent event) {
            this.nodeEngine = nodeEngine;
            this.event = event;
        }

        @Override
        public void run() {
            for (PartitionAwareService service : nodeEngine.getServices(PartitionAwareService.class)) {
                try {
                    service.onPartitionLost(event);
                } catch (Exception e) {
                    final ILogger logger = nodeEngine.getLogger(InternalPartitionLostEventPublisher.class);
                    logger.warning("Handling partitionLostEvent failed. Service: " + service.getClass() + " Event: " + event, e);
                }
            }
        }

        public IPartitionLostEvent getEvent() {
            return event;
        }
    }

}
