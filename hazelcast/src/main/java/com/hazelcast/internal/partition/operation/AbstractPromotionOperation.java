/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationEvent.MigrationStatus;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionMigrationEvent;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;

// Runs locally when the node becomes owner of a partition
abstract class AbstractPromotionOperation extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    // this is the replica index of the partition owner before the promotion.
    protected final MigrationInfo migrationInfo;

    AbstractPromotionOperation(MigrationInfo migrationInfo) {
        this.migrationInfo = migrationInfo;
    }

    /** Sends a {@link MigrationEvent} to registered listeners with status {@code status}. */
    void sendMigrationEvent(final MigrationStatus status) {
        final int partitionId = getPartitionId();
        final NodeEngine nodeEngine = getNodeEngine();
        final Member localMember = nodeEngine.getLocalMember();
        final MigrationEvent event = new MigrationEvent(partitionId, null, localMember, status);

        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, event, partitionId);
    }

    PartitionMigrationEvent getPartitionMigrationEvent() {
        return new PartitionMigrationEvent(DESTINATION, getPartitionId(), migrationInfo.getDestinationCurrentReplicaIndex(), 0);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getId() {
        throw new UnsupportedOperationException();
    }
}
