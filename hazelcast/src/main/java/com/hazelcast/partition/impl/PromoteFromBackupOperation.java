/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.partition.InternalPartitionService.SERVICE_NAME;

// runs locally...
final class PromoteFromBackupOperation extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private Address deadAddress;

    public PromoteFromBackupOperation(Address deadAddress) {
        this.deadAddress = deadAddress;
    }

    @Override
    public void beforeRun() throws Exception {
        sendMigrationEvent(MigrationEvent.MigrationStatus.STARTED);
    }

    @Override
    public void run() throws Exception {
        logPromotingPartition();
        try {
            PartitionMigrationEvent event = createPartitionMigrationEvent();
            sendToAllMigrationAwareServices(event);
        } finally {
            clearPartitionMigratingFlag();
        }
    }

    @Override
    public void afterRun() throws Exception {
        sendMigrationEvent(MigrationEvent.MigrationStatus.COMPLETED);
    }

    private void sendMigrationEvent(MigrationEvent.MigrationStatus status) {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        MemberImpl localMember = nodeEngine.getLocalMember();
        MemberImpl deadMember = new MemberImpl(deadAddress, false);
        MigrationEvent event = new MigrationEvent(partitionId, deadMember, localMember, status);
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        eventService.publishEvent(SERVICE_NAME, registrations, event, partitionId);
    }

    private void clearPartitionMigratingFlag() {
        InternalPartitionServiceImpl service = getService();
        InternalPartitionImpl partition = service.getPartition(getPartitionId());
        partition.setMigrating(false);
    }

    private void sendToAllMigrationAwareServices(PartitionMigrationEvent event) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (MigrationAwareService service : nodeEngine.getServices(MigrationAwareService.class)) {
            try {
                service.beforeMigration(event);
                service.commitMigration(event);
            } catch (Throwable e) {
                logMigrationError(e);
            }
        }
    }

    private PartitionMigrationEvent createPartitionMigrationEvent() {
        int partitionId = getPartitionId();
        return new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, partitionId);
    }

    private void logMigrationError(Throwable e) {
        ILogger logger = getLogger();
        logger.warning("While promoting partition " + getPartitionId(), e);
    }

    private void logPromotingPartition() {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Promoting partition " + getPartitionId());
        }
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
}
