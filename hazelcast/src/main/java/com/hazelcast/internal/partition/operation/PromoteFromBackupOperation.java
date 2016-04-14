/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionEventManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.COMPLETED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.STARTED;
import static com.hazelcast.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;

// Runs locally when the node becomes owner of a partition
//
// Finds the replica indices that are on the sync-waiting state. Those indices represents the lost backups of the partition.
// Therefore, it publishes InternalPartitionLostEvent objects to notify related services. It also updates the version for the
// lost replicas to the first available version value after the lost backups, or 0 if N/A
public final class PromoteFromBackupOperation
        extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    // this is the replica index of the partition owner before the promotion.
    private final int currentReplicaIndex;

    private ILogger logger;

    public PromoteFromBackupOperation(int currentReplicaIndex) {
        this.currentReplicaIndex = currentReplicaIndex;
    }

    void initLogger() {
        logger = getLogger();
    }

    @Override
    public void beforeRun()
            throws Exception {
        initLogger();
        sendMigrationEvent(STARTED);
    }

    @Override
    public void run()
            throws Exception {
        shiftUpReplicaVersions();
        migrateServices();
    }

    @Override
    public void afterRun()
            throws Exception {
        sendMigrationEvent(COMPLETED);
    }

    private void sendMigrationEvent(final MigrationStatus status) {
        final int partitionId = getPartitionId();
        final NodeEngine nodeEngine = getNodeEngine();
        final Member localMember = nodeEngine.getLocalMember();
        final MigrationEvent event = new MigrationEvent(partitionId, null, localMember, status);

        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, event, partitionId);
    }

    private void shiftUpReplicaVersions() {
        final int partitionId = getPartitionId();
        try {
            final InternalPartitionServiceImpl partitionService = getService();
            // returns the internal array itself, not the copy
            final long[] versions = partitionService.getPartitionReplicaVersions(partitionId);

            final int lostReplicaIndex = currentReplicaIndex - 1;

            if (currentReplicaIndex > 1) {
                final long[] versionsCopy = Arrays.copyOf(versions, versions.length);
                final long version = versions[lostReplicaIndex];
                Arrays.fill(versions, 0, lostReplicaIndex, version);

                if (logger.isFinestEnabled()) {
                    logger.finest(
                            "Partition replica is lost! partitionId=" + partitionId + " lost replicaIndex=" + lostReplicaIndex
                                    + " replica versions before shift up=" + Arrays.toString(versionsCopy)
                                    + " replica versions after shift up=" + Arrays.toString(versions));
                }
            } else if (logger.isFinestEnabled()) {
                logger.finest("PROMOTE partitionId=" + getPartitionId() + " from currentReplicaIndex=" + currentReplicaIndex);
            }

            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendPartitionLostEvent(partitionId, lostReplicaIndex);
        } catch (Throwable e) {
            logger.warning("Promotion failed. partitionId=" + partitionId + " replicaIndex=" + currentReplicaIndex, e);
        }
    }

    private void migrateServices() {
        try {
            sendToAllMigrationAwareServices(new PartitionMigrationEvent(DESTINATION, getPartitionId(), currentReplicaIndex, 0));
        } finally {
            clearPartitionMigratingFlag();
        }
    }

    private void sendToAllMigrationAwareServices(PartitionMigrationEvent event) {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (MigrationAwareService service : nodeEngine.getServices(MigrationAwareService.class)) {
            try {
                service.beforeMigration(event);
                service.commitMigration(event);
            } catch (Throwable e) {
                getLogger().warning("While promoting partitionId=" + getPartitionId(), e);
            }
        }
    }

    private void clearPartitionMigratingFlag() {
        final InternalPartitionServiceImpl service = getService();
        PartitionStateManager partitionStateManager = service.getPartitionStateManager();
        final InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(getPartitionId());
        partition.setMigrating(false);
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
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        throw new UnsupportedOperationException();
    }

}
