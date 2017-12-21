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

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.scheduledexecutor.impl.operations.SplitBrainMergeOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;

/**
 * Scheduled executor service, middle-man responsible for managing Scheduled Executor containers.
 */
public class DistributedScheduledExecutorService
        implements ManagedService, RemoteService, MigrationAwareService, SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:scheduledExecutorService";

    public static final int MEMBER_BIN = -1;

    private NodeEngine nodeEngine;

    private ScheduledExecutorPartition[] partitions;

    private ScheduledExecutorMemberBin memberBin;

    private SplitBrainMergePolicyProvider mergePolicyProvider;

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
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
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
    public Runnable prepareMergeRunnable() {
        Map<Integer, Map<String, Collection<ScheduledTaskDescriptor>>> state =
                new HashMap<Integer, Map<String, Collection<ScheduledTaskDescriptor>>>();

        for (int partition=0; partition<partitions.length; partition++) {
            if (nodeEngine.getPartitionService().isPartitionOwner(partition)) {
                state.put(partition, partitions[partition].prepareOwnedSnapshot());
            }
        }

        return new Merger(state);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationMode.compareAndSet(false, true);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            discardReserved(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteSuspended();
        }
        migrationMode.set(false);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            discardReserved(event.getPartitionId(), event.getCurrentReplicaIndex());
        } else if (event.getCurrentReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteSuspended();
        }
        migrationMode.set(false);
    }

    private void discardReserved(int partitionId, int thresholdReplicaIndex) {
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

    private MergePolicyConfig getMergePolicyConfig(String name) {
        return getNodeEngine().getConfig().getScheduledExecutorConfig(name)
                              .getMergePolicyConfig();
    }

    private SplitBrainMergePolicy getMergePolicy(String name) {
        return mergePolicyProvider.getMergePolicy(
                getMergePolicyConfig(name).getPolicy());
    }

    private class Merger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private Map<Integer, Map<String, Collection<ScheduledTaskDescriptor>>> partitionsSnapshot;

        Merger(Map<Integer, Map<String, Collection<ScheduledTaskDescriptor>>> map) {
            this.partitionsSnapshot = map;
        }

        @Override
        public void run() {
            // we cannot merge into a 3.9 cluster, since not all members may understand the CollectionMergeOperation
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                return;
            }

            final ILogger logger = nodeEngine.getLogger(DistributedScheduledExecutorService.class);
            final Semaphore semaphore = new Semaphore(0);

            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            int size = 0;
            int operationCount = 0;
            List<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>> mergeEntries;
            try {
                for (Map.Entry<Integer, Map<String, Collection<ScheduledTaskDescriptor>>> partition : partitionsSnapshot.entrySet()) {
                    int partitionId = partition.getKey();
                    Map<String, Collection<ScheduledTaskDescriptor>> containers = partition.getValue();

                    for (Map.Entry<String, Collection<ScheduledTaskDescriptor>> container : containers.entrySet()) {
                        String containerName = container.getKey();
                        Collection<ScheduledTaskDescriptor> tasks = container.getValue();

                        int batchSize = getMergePolicyConfig(containerName).getBatchSize();
                        SplitBrainMergePolicy mergePolicy = getMergePolicy(containerName);

                        mergeEntries = new ArrayList<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>>();
                        for (ScheduledTaskDescriptor descriptor : tasks) {
                            SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> entryView = createSplitBrainMergeEntryView(
                                    descriptor);
                            mergeEntries.add(entryView);
                            size++;

                            if (mergeEntries.size() == batchSize) {
                                sendBatch(partitionId, containerName, mergePolicy, mergeEntries, mergeCallback);
                                mergeEntries = new ArrayList<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>>(batchSize);
                                operationCount++;
                            }
                        }

                        tasks.clear();
                        if (mergeEntries.size() > 0) {
                            sendBatch(partitionId, containerName, mergePolicy, mergeEntries, mergeCallback);
                            operationCount++;
                        }
                    }
                }
                partitionsSnapshot.clear();

            } catch (Exception ex) {
                logger.warning("ScheduledExecutor merging didn't complete successfully.", ex);
                throw rethrow(ex);
            }

            try {
                semaphore.tryAcquire(operationCount, size * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for merge operation...");
            }
        }

        private void sendBatch(int partitionId, String name, SplitBrainMergePolicy mergePolicy,
                               List<SplitBrainMergeEntryView<String, ScheduledTaskDescriptor>> mergeEntries, ExecutionCallback<Object> mergeCallback) {
            SplitBrainMergeOperation operation = new SplitBrainMergeOperation(name, mergePolicy, mergeEntries);
            try {
                nodeEngine.getOperationService()
                          .invokeOnPartition(SERVICE_NAME, operation, partitionId)
                          .andThen(mergeCallback);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }
}
