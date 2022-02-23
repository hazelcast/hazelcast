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

package com.hazelcast.internal.crdt;

import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Task responsible for migrating the CRDT states for all
 * {@link CRDTReplicationAwareService}. This task is a runnable that is
 * meant to be executed by an executor. The task may be interrupted in
 * which case some CRDT states may not be replicated.
 */
class CRDTMigrationTask implements Runnable {
    /** Delay in seconds for a failed migration retry */
    private static final int MIGRATION_RETRY_DELAY_SECONDS = 1;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final CRDTReplicationMigrationService replicationMigrationService;

    CRDTMigrationTask(NodeEngine nodeEngine, CRDTReplicationMigrationService replicationMigrationService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.replicationMigrationService = replicationMigrationService;
    }

    @Override
    public void run() {
        try {
            if (nodeEngine.getLocalMember().isLiteMember()) {
                return;
            }
            final Collection<Member> members = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
            final Member firstDataMember = members.iterator().next();
            // the first data member always owns all of the CRDT states

            if (firstDataMember.equals(nodeEngine.getLocalMember())) {
                // if we are the first data member, there is nothing to migrate
                return;
            }

            final int localReplicaIndex = getLocalMemberListIndex();
            boolean allMigrated = true;
            for (CRDTReplicationAwareService service : replicationMigrationService.getReplicationServices()) {
                // local replica owns CRDTs with configured replica count of index + 1 and more
                // so we must migrate everything with configured replica count of less than index + 1
                allMigrated &= migrate(service, firstDataMember, localReplicaIndex + 1);
            }
            if (!allMigrated) {
                replicationMigrationService.scheduleMigrationTask(MIGRATION_RETRY_DELAY_SECONDS);
            }
        } finally {
            // we left the interrupt status unchanged while replicating so we clear it here
            Thread.interrupted();
        }
    }

    /**
     * Performs migration of a {@link CRDTReplicationAwareService} to the
     * given target.
     *
     * @param service                   the service to migrate
     * @param target                    the target to migrate to
     * @param maxConfiguredReplicaCount the maximum configured replica count
     *                                  for the CRDTs to be migrated (excluding)
     * @see CRDTReplicationAwareService
     */
    private boolean migrate(CRDTReplicationAwareService service, Member target, int maxConfiguredReplicaCount) {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        final OperationService operationService = nodeEngine.getOperationService();
        final CRDTReplicationContainer migrationOperation = service.prepareMigrationOperation(maxConfiguredReplicaCount);

        if (migrationOperation == null) {
            logger.finest("Skipping migration of " + service.getName() + " for target " + target);
            return true;
        }
        try {
            logger.finest("Migrating " + service.getName() + " to " + target);
            operationService.invokeOnTarget(null, migrationOperation.getOperation(), target.getAddress()).joinInternal();
            final boolean allMigrated = service.clearCRDTState(migrationOperation.getVectorClocks());
            if (!allMigrated) {
                logger.fine(service.getName() + " CRDTs have been mutated since migrated to target " + target
                        + ". Rescheduling migration in " + MIGRATION_RETRY_DELAY_SECONDS + " second(s).");
            }
            return allMigrated;
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed migration of " + service.getName() + " for target " + target
                        + ". Rescheduling migration in " + MIGRATION_RETRY_DELAY_SECONDS + " second(s).", e);
            } else {
                logger.info("Failed migration of " + service.getName() + " for target " + target
                        + ". Rescheduling migration in " + MIGRATION_RETRY_DELAY_SECONDS + " second(s).");
            }
            return false;
        }
    }

    /**
     * Returns the index of the local member in the membership list containing
     * only data members.
     */
    private int getLocalMemberListIndex() {
        final Collection<Member> dataMembers = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        int index = -1;
        for (Member dataMember : dataMembers) {
            index++;
            if (dataMember.equals(nodeEngine.getLocalMember())) {
                return index;
            }
        }
        return index;
    }
}
