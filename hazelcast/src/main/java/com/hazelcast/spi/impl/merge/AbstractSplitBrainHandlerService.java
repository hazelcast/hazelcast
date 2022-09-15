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

import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.currentThread;

/**
 * Collects mergeable stores and passes them to merge-runnable.
 *
 * @param <Store> store of a partition
 * @since 3.10
 */
public abstract class AbstractSplitBrainHandlerService<Store> implements SplitBrainHandlerService {

    private final IPartitionService partitionService;
    private final OperationExecutor operationExecutor;

    protected AbstractSplitBrainHandlerService(NodeEngine nodeEngine) {
        this.partitionService = nodeEngine.getPartitionService();
        this.operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService()).getOperationExecutor();
    }

    @Override
    public final Runnable prepareMergeRunnable() {
        return newMergeRunnable(collectStores());
    }

    private Collection<Store> collectStores() {
        ConcurrentLinkedQueue<Store> mergingStores = new ConcurrentLinkedQueue<>();

        int partitionCount = partitionService.getPartitionCount();
        final CountDownLatch latch = new CountDownLatch(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            operationExecutor.execute(new StoreCollector(mergingStores, i, latch));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }

        return mergingStores;
    }

    /**
     * Collects store instances inside a partition to get data to merge
     * and prepare merge operations.
     */
    private class StoreCollector implements PartitionSpecificRunnable {
        private final int partitionId;
        private final CountDownLatch latch;
        private final ConcurrentLinkedQueue<Store> mergingStores;

        StoreCollector(ConcurrentLinkedQueue<Store> mergingStores,
                       int partitionId,
                       CountDownLatch latch) {
            this.mergingStores = mergingStores;
            this.partitionId = partitionId;
            this.latch = latch;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            LinkedList<Store> storesToDestroy = new LinkedList<>();
            try {
                Iterator<Store> iterator = storeIterator(partitionId);
                while (iterator.hasNext()) {
                    Store store = iterator.next();
                    if (isLocalPartition(partitionId)
                            && hasEntries(store)
                            && hasMergeablePolicy(store)) {
                        mergingStores.add(store);
                    } else {
                        storesToDestroy.add(store);
                    }
                    onStoreCollection(store);
                    iterator.remove();
                }
                asyncDestroyStores(storesToDestroy, partitionId);
            } finally {
                latch.countDown();
            }
        }
    }

    void asyncDestroyStores(final Collection<Store> stores, final int partitionID) {
        operationExecutor.execute(new PartitionSpecificRunnable() {
            @Override
            public void run() {
                for (Store store : stores) {
                    destroyStore(store);
                }
            }

            @Override
            public int getPartitionId() {
                return partitionID;
            }
        });
    }

    private boolean isLocalPartition(int partitionId) {
        return partitionService.isPartitionOwner(partitionId);
    }

    /**
     * Final cleanup before starting merge after {@link StoreCollector}
     * collects a store. If we do this cleanup upon join of merging node,
     * concurrently running merge and migration operations can cause
     * inconsistency over shared data. For example, dropping of map
     * indexes is done at this stage not to lose index data.
     */
    protected void onStoreCollection(Store store) {

    }

    /**
     * Returns a runnable which merges the given {@link Store} instances.
     *
     * @return a merge runnable for the given stores
     */
    protected abstract Runnable newMergeRunnable(Collection<Store> mergingStores);

    /**
     * Returns an {@link Iterator} over all {@link Store} instances in the given partition.
     *
     * @return an iterator over all stores
     */
    protected abstract Iterator<Store> storeIterator(int partitionId);

    /**
     * Destroys the given {@link Store}.
     */
    protected abstract void destroyStore(Store store);

    /**
     * Checks if the given {@link Store} has entries.
     *
     * @return {@code true} if the store has entries, {@code false} otherwise
     */
    protected abstract boolean hasEntries(Store store);

    /**
     * Checks if the given {@link Store} has a mergeable merge policy.
     *
     * @return {@code true} if the store has a mergeable merge policy, {@code false} otherwise
     */
    protected abstract boolean hasMergeablePolicy(Store store);
}
