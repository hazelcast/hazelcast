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
import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.internal.services.GracefulShutdownAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Service that handles replication and migration of CRDT data for all CRDT
 * implementations.
 * The CRDT implementations for which it performs replication and migration
 * must implement {@link CRDTReplicationAwareService}.
 * <p>
 * The replication is performed periodically on an executor with the name
 * {@value #CRDT_REPLICATION_MIGRATION_EXECUTOR}. You may configure this
 * executor accordingly.
 * The migration mechanism uses the same executor but it is performed only
 * on membership changes and on CRDT state merge. The migration checks are
 * triggered on CRDT state merge because CRDT states can get merged when
 * a replica is shutting down and it is trying to replicate any previously
 * unreplicated state to any member in the cluster, regardless of the
 * configured replica count.
 *
 * @see CRDTReplicationConfig#getReplicationPeriodMillis()
 * @see CRDTReplicationConfig#getMaxConcurrentReplicationTargets()
 */
public class CRDTReplicationMigrationService implements ManagedService, MembershipAwareService,
        GracefulShutdownAwareService {
    /**
     * The name of this service
     */
    public static final String SERVICE_NAME = "hz:impl:CRDTReplicationMigrationService";
    /**
     * The executor for the CRDT replication and migration tasks
     */
    public static final String CRDT_REPLICATION_MIGRATION_EXECUTOR = "hz:CRDTReplicationMigration";

    private ScheduledFuture<?> replicationTask;
    private NodeEngine nodeEngine;
    private ILogger logger;
    private ReplicatedVectorClocks replicationVectorClocks;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        final CRDTReplicationConfig replicationConfig = nodeEngine.getConfig().getCRDTReplicationConfig();

        final int replicationPeriod = replicationConfig != null
                ? replicationConfig.getReplicationPeriodMillis()
                : CRDTReplicationConfig.DEFAULT_REPLICATION_PERIOD_MILLIS;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.replicationVectorClocks = new ReplicatedVectorClocks();

        int maxTargets = replicationConfig != null
                ? replicationConfig.getMaxConcurrentReplicationTargets()
                : CRDTReplicationConfig.DEFAULT_MAX_CONCURRENT_REPLICATION_TARGETS;
        this.replicationTask = nodeEngine.getExecutionService().scheduleWithRepetition(
                CRDT_REPLICATION_MIGRATION_EXECUTOR, new CRDTReplicationTask(nodeEngine, maxTargets, this),
                replicationPeriod, replicationPeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        ScheduledFuture<?> task = replicationTask;
        if (task != null) {
            replicationTask = null;
            task.cancel(terminate);
        }
    }


    /**
     * Attempts to replicate only the unreplicated CRDT state to any non-local
     * member in the cluster. The state may be unreplicated because the CRDT
     * state has been changed (via mutation or merge with an another CRDT) but
     * has not yet been disseminated through the usual replication mechanism
     * to any member.
     * This method will iterate through the member list and try and replicate
     * to at least one member. The method returns once all of the unreplicated
     * state has been replicated successfully or when there are no more members
     * to attempt processing.
     * This method will replicate all of the unreplicated CRDT states to any
     * data member in the cluster, regardless if that member is actually the
     * replica for some CRDT (because of a configured replica count). It is
     * the responsibility of that member to migrate the state for which it is
     * not a replica. The configured replica count can therefore be broken
     * during shutdown to increase the chance of survival of unreplicated CRDT
     * data (if the actual replicas are unreachable).
     *
     * @see CRDTReplicationTask
     */
    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return true;
        }
        long timeoutNanos = unit.toNanos(timeout);
        for (CRDTReplicationAwareService service : getReplicationServices()) {
            service.prepareToSafeShutdown();
            final CRDTReplicationContainer replicationOperation = service.prepareReplicationOperation(
                    replicationVectorClocks.getLatestReplicatedVectorClock(service.getName()), 0);
            if (replicationOperation == null) {
                logger.fine("Skipping replication since all CRDTs are replicated");
                continue;
            }
            long start = System.nanoTime();
            if (!tryProcessOnOtherMembers(replicationOperation.getOperation(), service.getName(), timeoutNanos)) {
                logger.warning("Failed replication of CRDTs for " + service.getName() + ". CRDT state may be lost.");
            }
            timeoutNanos -= (System.nanoTime() - start);
            if (timeoutNanos < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to process the {@code operation} on at least one non-local
     * member. The method will iterate through the member list and try once on
     * each member.
     * The method returns as soon as the first member successfully processes
     * the operation or once there are no more members to try.
     *
     * @param serviceName the service name
     * @return {@code true} if at least one member successfully processed the
     * operation, {@code false} otherwise.
     */
    private boolean tryProcessOnOtherMembers(Operation operation, String serviceName, long timeoutNanos) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<Member> targets = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        final Member localMember = nodeEngine.getLocalMember();

        for (Member target : targets) {
            if (target.equals(localMember)) {
                continue;
            }
            long start = System.nanoTime();
            try {
                logger.fine("Replicating " + serviceName + " to " + target);
                InternalCompletableFuture<Object> future =
                        operationService.createInvocationBuilder(null, operation, target.getAddress())
                                .setTryCount(1)
                                .invoke();
                future.get(timeoutNanos, TimeUnit.NANOSECONDS);
                return true;
            } catch (Exception e) {
                logger.fine("Failed replication of " + serviceName + " for target " + target, e);
            }

            timeoutNanos -= (System.nanoTime() - start);
            if (timeoutNanos < 0) {
                break;
            }
        }
        return false;
    }

    /**
     * Returns a collection of all known CRDT replication aware services
     */
    Collection<CRDTReplicationAwareService> getReplicationServices() {
        return nodeEngine.getServices(CRDTReplicationAwareService.class);
    }

    /**
     * Returns the vector clocks for the given {@code serviceName} and
     * {@code memberUUID}.
     * The vector clock map contains mappings from CRDT name to the last
     * successfully replicated CRDT vector clock. All CRDTs in this map should
     * be of the same type.
     * If there are no vector clocks for the given parameters, this method
     * returns an empty map.
     *
     * @param serviceName the service name
     * @param memberUUID  the target member UUID
     * @return the last successfully replicated CRDT state vector clocks or
     * an empty map if the CRDTs have not yet been replicated to this member
     * @see CRDTReplicationAwareService
     */
    Map<String, VectorClock> getReplicatedVectorClocks(String serviceName, UUID memberUUID) {
        return replicationVectorClocks.getReplicatedVectorClock(serviceName, memberUUID);
    }

    /**
     * Sets the replicated vector clocks for the given {@code serviceName} and
     * {@code memberUUID}.
     * The vector clock map contains mappings from CRDT name to the last
     * successfully replicated CRDT state version. All CRDTs in this map should
     * be of the same type.
     *
     * @param serviceName  the service name
     * @param memberUUID   the target member UUID
     * @param vectorClocks the vector clocks to set
     * @see CRDTReplicationAwareService
     */
    void setReplicatedVectorClocks(String serviceName, UUID memberUUID, Map<String, VectorClock> vectorClocks) {
        replicationVectorClocks.setReplicatedVectorClocks(serviceName, memberUUID, vectorClocks);
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        scheduleMigrationTask(0);
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        scheduleMigrationTask(0);
    }

    /**
     * Schedules a {@link CRDTMigrationTask} with a delay of {@code delaySeconds}
     * seconds.
     */
    void scheduleMigrationTask(long delaySeconds) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return;
        }
        nodeEngine.getExecutionService().schedule(CRDT_REPLICATION_MIGRATION_EXECUTOR,
                new CRDTMigrationTask(nodeEngine, this), delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return "CRDTReplicationMigrationService{}";
    }
}
