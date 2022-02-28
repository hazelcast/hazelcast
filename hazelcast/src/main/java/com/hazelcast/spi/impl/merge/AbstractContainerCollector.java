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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.internal.partition.IPartitionService;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * Collects non-threadsafe containers for {@link SplitBrainHandlerService}
 * capable data structures.
 * <p>
 * The {@link com.hazelcast.internal.cluster.impl.ClusterMergeTask ClusterMergeTask}
 * collects all mergeable data from all Hazelcast services. The problem is that it
 * runs on an arbitrary thread, but the data is only modified by specific
 * {@link com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread partition threads}.
 * This can cause visibility issues due to a missing happens-before relation.
 * <p>
 * The collector fetches the data via {@link PartitionSpecificRunnable} (which
 * run on the partition threads) and stores them in a {@link ConcurrentHashMap}.
 * This guarantees the visibility for the {@code ClusterMergeTask}.
 * <p>
 * The collector can be implemented for data structures which reference their containers by
 * <ul>
 * <li>partition IDs using {@link AbstractContainerCollector}</li>
 * <li>container name using {@link AbstractNamedContainerCollector}</li>
 * </ul>
 * <b>Note:</b> The collector will retain the collected containers for the merge runnable.
 * So {@link ManagedService#reset()} is not allowed to clear or destroy those containers.
 * To ensure this the collector removes the link between the data structure and a container
 * via {@link Iterator#remove()} in {@link CollectContainerRunnable#run()}.
 * <p>
 * The cleanup of the containers itself is done in {@link #destroy()} via {@link #destroy(Object)}
 * after the merge is done, or directly if the container is not collected. Containers from backup
 * partitions are directly cleaned via {@link #destroyBackup(Object)}.
 *
 * @param <C> container of the data structure
 */
public abstract class AbstractContainerCollector<C> {

    private final ConcurrentMap<Integer, Collection<C>> containersByPartitionId
            = new ConcurrentHashMap<Integer, Collection<C>>();

    private final OperationExecutor operationExecutor;
    private final IPartitionService partitionService;
    private final SplitBrainMergePolicyProvider mergePolicyProvider;

    private CountDownLatch latch;

    protected AbstractContainerCollector(NodeEngine nodeEngine) {
        this.operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService()).getOperationExecutor();
        this.partitionService = nodeEngine.getPartitionService();
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
    }

    /**
     * Collects the containers from the data structure in a thread-safe way.
     */
    public final void run() {
        int partitionCount = partitionService.getPartitionCount();
        latch = new CountDownLatch(partitionCount);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            operationExecutor.execute(new CollectContainerRunnable(partitionId));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the collected containers by partition ID.
     *
     * @return the collected containers
     */
    public final ConcurrentMap<Integer, Collection<C>> getCollectedContainers() {
        return containersByPartitionId;
    }

    /**
     * Destroys all collected containers.
     */
    public final void destroy() {
        for (Collection<C> containers : containersByPartitionId.values()) {
            for (C container : containers) {
                destroy(container);
            }
        }
        containersByPartitionId.clear();
        onDestroy();
    }

    /**
     * Will be called by {@link #destroy()}.
     * <p>
     * Can be overridden by implementations to cleanup local resources.
     */
    protected void onDestroy() {
        // NOP
    }

    /**
     * Returns all containers of the data structure for the given partition ID.
     *
     * @return {@link Iterator} over the containers of the given partition
     */
    protected abstract Iterator<C> containerIterator(int partitionId);

    /**
     * Returns the {@link MergePolicyConfig} of the container.
     *
     * @return the {@link MergePolicyConfig} of the container
     */
    protected abstract MergePolicyConfig getMergePolicyConfig(C container);

    /**
     * Destroys the owned data in the container.
     * <p>
     * Is called if a container is not collected or after the merge has been done.
     */
    protected abstract void destroy(C container);

    /**
     * Destroys the backup data in the container.
     * <p>
     * Is called if the container is not collected, since it's in a backup partition.
     */
    protected abstract void destroyBackup(C container);

    /**
     * Returns the number of collected merging values in this collector.
     * <p>
     * The count is used to calculate the timeout value to wait for merge operations to complete.
     * <p>
     * <b>Note:</b> Depending on the data structure, this can be the number of collected containers
     * or the number of merging values within the collected containers.
     *
     * @return the number of collected merge values
     */
    protected abstract int getMergingValueCount();

    /**
     * Determines if the container should be merged.
     * <p>
     * Can be overridden if there are additional restrictions beside the merge policy,
     * if a container is mergeable or not.
     *
     * @return {@code true} if the container is mergeable, {@code false} otherwise
     */
    protected boolean isMergeable(C container) {
        return true;
    }

    /**
     * Empty iterator for {@link #containerIterator(int)} calls, if the requested partition is empty.
     */
    protected final class EmptyIterator implements Iterator<C> {

        public EmptyIterator() {
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public C next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Collects containers from a specific partition ID.
     */
    private final class CollectContainerRunnable implements PartitionSpecificRunnable {

        private final Collection<C> containers = new LinkedList<C>();

        private final int partitionId;

        CollectContainerRunnable(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                Iterator<C> iterator = containerIterator(partitionId);
                while (iterator.hasNext()) {
                    C container = iterator.next();
                    collect(container);
                    iterator.remove();
                }
            } finally {
                if (!containers.isEmpty()) {
                    containersByPartitionId.put(partitionId, containers);
                }
                latch.countDown();
            }
        }

        private void collect(C container) {
            // just collect owned partitions
            if (partitionService.isPartitionOwner(partitionId)) {
                MergePolicyConfig mergePolicyconfig = getMergePolicyConfig(container);
                SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(mergePolicyconfig.getPolicy());
                if (isMergeable(container) && !(mergePolicy instanceof DiscardMergePolicy)) {
                    containers.add(container);
                } else {
                    destroy(container);
                }
            } else {
                destroyBackup(container);
            }
        }
    }
}
