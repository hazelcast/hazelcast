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

import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Task responsible for replicating the CRDT states for all
 * {@link CRDTReplicationAwareService}. This task is a runnable that is
 * meant to be executed by an executor. The task may be interrupted in
 * which case some CRDT states may not be replicated.
 */
class CRDTReplicationTask implements Runnable {
    private final NodeEngine nodeEngine;
    private final int maxTargets;
    private final ILogger logger;
    private final CRDTReplicationMigrationService replicationMigrationService;
    private int lastTargetIndex;

    CRDTReplicationTask(NodeEngine nodeEngine, int maxTargets, CRDTReplicationMigrationService replicationMigrationService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.maxTargets = maxTargets;
        this.replicationMigrationService = replicationMigrationService;
    }

    @Override
    public void run() {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return;
        }
        try {
            final Collection<Member> viableTargets = getNonLocalReplicaAddresses();
            if (viableTargets.size() == 0) {
                return;
            }
            final Member[] targets = pickTargets(viableTargets, lastTargetIndex, maxTargets);
            lastTargetIndex = (lastTargetIndex + targets.length) % viableTargets.size();
            for (CRDTReplicationAwareService service : replicationMigrationService.getReplicationServices()) {
                for (Member target : targets) {
                    replicate(service, target);
                }
            }
        } finally {
            // we left the interrupt status unchanged while replicating so we clear it here
            Thread.interrupted();
        }
    }

    /**
     * Return the list of non-local CRDT replicas in the cluster.
     */
    private List<Member> getNonLocalReplicaAddresses() {
        final Collection<Member> dataMembers = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        final ArrayList<Member> nonLocalDataMembers = new ArrayList<Member>(dataMembers);
        nonLocalDataMembers.remove(nodeEngine.getLocalMember());
        return nonLocalDataMembers;
    }

    /**
     * Performs replication of a {@link CRDTReplicationAwareService} to the
     * given target. The service may optimise the returned operation based on
     * the target member and the previous successful replication operations.
     *
     * @param service the service to replicate
     * @param target  the target to replicate to
     * @see CRDTReplicationAwareService
     */
    private void replicate(CRDTReplicationAwareService service, Member target) {
        if (Thread.currentThread().isInterrupted()) {
            return;
        }
        final int targetIndex = getDataMemberListIndex(target);
        final Map<String, VectorClock> lastSuccessfullyReplicatedClocks =
                replicationMigrationService.getReplicatedVectorClocks(service.getName(), target.getUuid());

        final OperationService operationService = nodeEngine.getOperationService();
        final CRDTReplicationContainer replicationOperation =
                service.prepareReplicationOperation(lastSuccessfullyReplicatedClocks, targetIndex);

        if (replicationOperation == null) {
            logger.finest("Skipping replication of " + service.getName() + " for target " + target);
            return;
        }
        try {
            logger.finest("Replicating " + service.getName() + " to " + target);
            operationService.invokeOnTarget(null, replicationOperation.getOperation(), target.getAddress()).joinInternal();
            replicationMigrationService.setReplicatedVectorClocks(service.getName(), target.getUuid(),
                    replicationOperation.getVectorClocks());
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed replication of " + service.getName() + " for target " + target, e);
            } else {
                logger.info("Failed replication of " + service.getName() + " for target " + target);
            }
        }
    }

    /**
     * Returns the index of the {@code member} in the membership list containing
     * only data members.
     *
     * @param member the member to find
     */
    private int getDataMemberListIndex(Member member) {
        final Collection<Member> dataMembers = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        int index = -1;
        for (Member dataMember : dataMembers) {
            index++;
            if (dataMember.equals(member)) {
                return index;
            }
        }
        return index;
    }

    /**
     * Picks up to {@code maxTargets} from the provided {@code members}
     * collection. The {@code startingIndex} parameter determines which
     * subset of members can be returned. By increasing the parameter by the
     * size of the returned array you can imitate a "sliding window", meaning
     * that each time it is invoked it will rotate through a list of viable
     * targets and return a sub-collection based on the previous method call.
     * A member may be skipped if the collection of viable targets changes
     * between two invocations but if the collection does not change,
     * eventually all targets should be returned by this method.
     *
     * @param members       a collection of members to choose from
     * @param startingIndex the index of the first returned member
     * @param maxTargets    the maximum number of members to return
     * @return the chosen targets
     * @see CRDTReplicationConfig#getMaxConcurrentReplicationTargets()
     */
    private Member[] pickTargets(Collection<Member> members, int startingIndex, int maxTargets) {
        final Member[] viableTargetArray = members.toArray(new Member[0]);
        final Member[] pickedTargets = new Member[Math.min(maxTargets, viableTargetArray.length)];

        for (int i = 0; i < pickedTargets.length; i++) {
            startingIndex = (startingIndex + 1) % viableTargetArray.length;
            pickedTargets[i] = viableTargetArray[startingIndex];
        }
        return pickedTargets;
    }
}
