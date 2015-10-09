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

import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationEvent.MigrationStatus;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.COMPLETED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.STARTED;
import static com.hazelcast.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static com.hazelcast.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.partition.impl.PartitionReplicaChangeReason.MEMBER_REMOVED;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;

// Runs locally when the node becomes owner of a partition
//
// Finds the replica indices that are on the sync-waiting state. Those indices represents the lost backups of the partition.
// Therefore, it publishes InternalPartitionLostEvent objects to notify related services. It also updates the version for the
// lost replicas to the first available version value after the lost backups, or 0 if N/A
final class PromoteFromBackupOperation
        extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final PartitionReplicaChangeReason reason;

    private final Address oldAddress;

    private ILogger logger;

    public PromoteFromBackupOperation(PartitionReplicaChangeReason reason, Address oldAddress) {
        this.reason = reason;
        this.oldAddress = oldAddress;
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
        handleLostBackups();
        promoteFromBackups();
    }

    @Override
    public void afterRun()
            throws Exception {
        sendMigrationEvent(COMPLETED);
    }

    private void sendMigrationEvent(final MigrationStatus status) {
        if (reason != MEMBER_REMOVED) {
            return;
        }
        final int partitionId = getPartitionId();
        final NodeEngine nodeEngine = getNodeEngine();
        final Member localMember = nodeEngine.getLocalMember();
        final MemberImpl deadMember = new MemberImpl(oldAddress, false);
        final MigrationEvent event = new MigrationEvent(partitionId, deadMember, localMember, status);

        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
        eventService.publishEvent(SERVICE_NAME, registrations, event, partitionId);
    }

    void handleLostBackups() {
        final int partitionId = getPartitionId();
        try {
            final InternalPartitionService partitionService = getService();
            // returns the internal array itself, not the copy
            final long[] versions = partitionService.getPartitionReplicaVersions(partitionId);

            if (reason == MEMBER_REMOVED) {
                final int lostReplicaIndex = getLostReplicaIndex(versions);
                if (lostReplicaIndex > 0) {
                    overwriteLostReplicaVersionsWithFirstAvailableVersion(partitionId, versions, lostReplicaIndex);
                }

                sendPartitionLostEvent(partitionId, lostReplicaIndex);
            } else {
                resetSyncWaitingVersions(partitionId, versions);
            }
        } catch (Throwable e) {
            logger.warning("Partition lost detection failed. partitionId=" + partitionId, e);
        }
    }

    void promoteFromBackups() {
        if (reason != MEMBER_REMOVED) {
            return;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Promoting partitionId=" + getPartitionId());
        }

        try {
            sendToAllMigrationAwareServices(new PartitionMigrationEvent(DESTINATION, getPartitionId()));
        } finally {
            clearPartitionMigratingFlag();
        }
    }

    private int getLostReplicaIndex(final long[] versions) {
        int biggestLostReplicaIndex = 0;

        for (int replicaIndex = 1; replicaIndex <= versions.length; replicaIndex++) {
            if (versions[replicaIndex - 1] == InternalPartition.SYNC_WAITING) {
                biggestLostReplicaIndex = replicaIndex;
            }
        }

        return biggestLostReplicaIndex;
    }

    private void overwriteLostReplicaVersionsWithFirstAvailableVersion(final int partitionId, final long[] versions,
                                                                       final int lostReplicaIndex) {
        if (logger.isFinestEnabled()) {
            logger.finest("Partition replica is lost! partitionId=" + partitionId + " lost-replicaIndex=" + lostReplicaIndex
                            + " replicaVersions=" + Arrays.toString(versions));
        }

        final long forcedVersion = lostReplicaIndex < versions.length ? versions[lostReplicaIndex] : 0;
        for (int replicaIndex = lostReplicaIndex; replicaIndex > 0; replicaIndex--) {
            versions[replicaIndex - 1] = forcedVersion;
        }
    }

    private void resetSyncWaitingVersions(int partitionId, long[] versions) {
        long[] versionsBeforeReset = null;
        for (int replicaIndex = 1; replicaIndex <= versions.length; replicaIndex++) {
            if (versions[replicaIndex - 1] == InternalPartition.SYNC_WAITING) {
                if (versionsBeforeReset == null) {
                    versionsBeforeReset = Arrays.copyOf(versions, versions.length);
                }

                versions[replicaIndex - 1] = 0;
            }
        }

        if (logger.isFinestEnabled() && versionsBeforeReset != null) {
            logger.finest("Resetting all SYNC_WAITING Versions. partitionId=" + partitionId + " versionsBeforeReset=" + Arrays
                    .toString(versionsBeforeReset));
        }
    }

    private void sendPartitionLostEvent(int partitionId, int lostReplicaIndex) {
        final InternalPartitionLostEvent event = new InternalPartitionLostEvent(partitionId, lostReplicaIndex,
                getNodeEngine().getThisAddress());
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final InternalPartitionLostEventPublisher publisher = new InternalPartitionLostEventPublisher(nodeEngine, event);
        nodeEngine.getExecutionService().execute(SYSTEM_EXECUTOR, publisher);
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
        final InternalPartitionImpl partition = service.getPartition(getPartitionId());
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

    static class InternalPartitionLostEventPublisher
            implements Runnable {

        private final NodeEngineImpl nodeEngine;

        private final InternalPartitionLostEvent event;

        public InternalPartitionLostEventPublisher(NodeEngineImpl nodeEngine, InternalPartitionLostEvent event) {
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

        public InternalPartitionLostEvent getEvent() {
            return event;
        }
    }
}
