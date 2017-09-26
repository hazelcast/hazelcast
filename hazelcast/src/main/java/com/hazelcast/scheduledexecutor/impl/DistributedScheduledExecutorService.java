/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;

/**
 * Scheduled executor service, middle-man responsible for managing Scheduled Executor containers.
 */
public class DistributedScheduledExecutorService
        implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:scheduledExecutorService";

    public static final int MEMBER_BIN = -1;

    private NodeEngine nodeEngine;

    private ScheduledExecutorPartition[] partitions;

    private ScheduledExecutorMemberBin memberBin;

    private final ConcurrentMap<String, Boolean> shutdownExecutors
            = new ConcurrentHashMap<String, Boolean>();

    private final Set<ScheduledFutureProxy> lossListeners =
            synchronizedSet(newSetFromMap(new WeakHashMap<ScheduledFutureProxy, Boolean>()));

    private final AtomicBoolean migrationMode = new AtomicBoolean();

    private String partitionLostRegistration;

    private String membershipListenerRegistration;

    public DistributedScheduledExecutorService() {
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.nodeEngine = nodeEngine;
        this.partitions = new ScheduledExecutorPartition[partitionCount];
        reset();
    }

    public ScheduledExecutorPartition getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public ScheduledExecutorContainerHolder getPartitionOrMemberBin(int id) {
        if (id == MEMBER_BIN) {
            return memberBin;
        }

        return getPartition(id);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void reset() {
        shutdown(true);

        memberBin = new ScheduledExecutorMemberBin(nodeEngine);

        if (partitionLostRegistration == null) {
            registerPartitionListener();
        }

        if (membershipListenerRegistration == null) {
            registerMembershipListener();
        }

        for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
            if (partitions[partitionId] != null) {
                partitions[partitionId].destroy();
            }
            partitions[partitionId] = new ScheduledExecutorPartition(nodeEngine, partitionId);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        shutdownExecutors.clear();

        if (memberBin != null) {
            memberBin.destroy();
        }

        lossListeners.clear();

        unRegisterPartitionListenerIfExists();
        unRegisterMembershipListenerIfExists();

        for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
            if (partitions[partitionId] != null) {
                partitions[partitionId].destroy();
            }
        }
    }

    void addLossListener(ScheduledFutureProxy future) {
        this.lossListeners.add(future);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new ScheduledExecutorServiceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        if (shutdownExecutors.remove(name) == null) {
            ((InternalExecutionService) nodeEngine.getExecutionService()).shutdownScheduledDurableExecutor(name);
        }

        resetPartitionOrMemberBinContainer(name);
    }

    public void shutdownExecutor(String name) {
        if (shutdownExecutors.putIfAbsent(name, Boolean.TRUE) == null) {
            ((InternalExecutionService) nodeEngine.getExecutionService()).shutdownScheduledDurableExecutor(name);
        }
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.containsKey(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        ScheduledExecutorPartition partition = partitions[partitionId];
        return partition.prepareReplicationOperation(event.getReplicaIndex(), migrationMode.get());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationMode.compareAndSet(false, true);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            discardStash(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteStash();
        }
        migrationMode.set(false);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            discardStash(event.getPartitionId(), event.getCurrentReplicaIndex());
        } else if (event.getCurrentReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteStash();
        }
        migrationMode.set(false);
    }

    private void discardStash(int partitionId, int thresholdReplicaIndex) {
        ScheduledExecutorPartition partition = partitions[partitionId];
        partition.disposeObsoleteReplicas(thresholdReplicaIndex);
    }

    private void resetPartitionOrMemberBinContainer(String name) {
        if (memberBin != null) {
            memberBin.destroyContainer(name);
        }

        for (ScheduledExecutorPartition partition : partitions) {
            partition.destroyContainer(name);
        }
    }

    private void registerPartitionListener() {
        this.partitionLostRegistration = getNodeEngine().getPartitionService().addPartitionLostListener(
                new PartitionLostListener() {
                    @Override
                    public void partitionLost(PartitionLostEvent event) {
                        // use toArray before iteration since it is done under mutex
                        ScheduledFutureProxy[] futures = lossListeners.toArray(new ScheduledFutureProxy[lossListeners.size()]);
                        for (ScheduledFutureProxy future : futures) {
                            future.notifyPartitionLost(event);
                        }
                    }
                }
        );
    }

    private void unRegisterPartitionListenerIfExists() {
        if (this.partitionLostRegistration == null) {
            return;
        }

        try {
            getNodeEngine().getPartitionService().removePartitionLostListener(this.partitionLostRegistration);
        } catch (Exception ex) {
            if (peel(ex, HazelcastInstanceNotActiveException.class, null)
                    instanceof HazelcastInstanceNotActiveException) {
                throw rethrow(ex);
            }
        }

        this.partitionLostRegistration = null;
    }

    private void registerMembershipListener() {
        this.membershipListenerRegistration = getNodeEngine().getClusterService().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent event) {
                // use toArray before iteration since it is done under mutex
                ScheduledFutureProxy[] futures = lossListeners.toArray(new ScheduledFutureProxy[lossListeners.size()]);
                for (ScheduledFutureProxy future : futures) {
                    future.notifyMemberLost(event);
                }
            }
        });
    }

    private void unRegisterMembershipListenerIfExists() {
        if (this.membershipListenerRegistration == null) {
            return;
        }

        try {
            getNodeEngine().getClusterService().removeMembershipListener(membershipListenerRegistration);
        } catch (Exception ex) {
            if (peel(ex, HazelcastInstanceNotActiveException.class, null)
                    instanceof HazelcastInstanceNotActiveException) {
                throw rethrow(ex);
            }
        }

        this.membershipListenerRegistration = null;
    }
}
