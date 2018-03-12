/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Disposable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.MutableLong;
import com.hazelcast.util.function.BiConsumer;
import com.hazelcast.version.Version;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.config.ConfigValidator.checkMergePolicySupportsInMemoryFormat;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Used by {@link com.hazelcast.cache.ICache} and {@link com.hazelcast.core.IMap}
 * to provide a shared merge runnable for {@link SplitBrainHandlerService#prepareMergeRunnable()}.
 *
 * @param <Store> type of the store in a partition
 */
public abstract class AbstractMergeRunnable<Store, MergingItem> implements Runnable, Disposable {

    private static final long TIMEOUT_FACTOR = 500;
    private static final long MINIMAL_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private final Semaphore semaphore = new Semaphore(0);

    private final ILogger logger;
    private final String serviceName;
    private final ClusterService clusterService;
    private final InternalSerializationService serializationService;
    private final OperationService operationService;
    private final IPartitionService partitionService;
    private final Collection<Store> backupStores;
    private final Map<String, Collection<Store>> collectedStores;
    private final Map<String, Collection<Store>> collectedStoresWithLegacyPolicies;

    protected AbstractMergeRunnable(String serviceName, Map<String, Collection<Store>> collectedStores,
                                    Map<String, Collection<Store>> collectedStoresWithLegacyPolicies,
                                    Collection<Store> backupStores, NodeEngine nodeEngine) {
        this.serviceName = serviceName;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.clusterService = nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.backupStores = backupStores;
        this.collectedStores = collectedStores;
        this.collectedStoresWithLegacyPolicies = collectedStoresWithLegacyPolicies;
    }

    @Override
    public final void run() {
        int mergedCount = 0;

        mergedCount += mergeWithSplitBrainMergePolicy();
        mergedCount += mergeWithLegacyMergePolicy();

        waitMergeEnd(mergedCount);
    }

    private int mergeWithSplitBrainMergePolicy() {
        int mergedCount = 0;
        for (Map.Entry<String, Collection<Store>> entry : collectedStores.entrySet()) {
            String dataStructureName = entry.getKey();
            Collection<Store> recordStores = entry.getValue();

            SplitBrainMergePolicy mergePolicy = ((SplitBrainMergePolicy) getMergePolicy(dataStructureName));
            if (!isClusterVersion310OrLater(dataStructureName, mergePolicy)) {
                continue;
            }

            int batchSize = getBatchSize(dataStructureName);
            MergingItemBiConsumer consumer = new MergingItemBiConsumer(dataStructureName, mergePolicy, batchSize);

            for (Store recordStore : recordStores) {
                consumeStore(recordStore, consumer);
            }

            consumer.consumeRemaining();

            mergedCount += consumer.mergedCount;
        }
        return mergedCount;
    }

    private boolean isClusterVersion310OrLater(String dataStructureName, SplitBrainMergePolicy policy) {
        Version v310 = V3_10;
        Version currentVersion = clusterService.getClusterVersion();

        if (currentVersion.isGreaterOrEqual(v310)) {
            return true;
        }
        // RU_COMPAT_3_9
        String msg = "Cannot merge '%s' with merge policy '%s'."
                + " Cluster version should be %s or later but found %s";
        logger.info(format(msg, dataStructureName, policy, v310, currentVersion));

        return false;
    }

    private int mergeWithLegacyMergePolicy() {
        LegacyOperationBiConsumer consumer = new LegacyOperationBiConsumer();

        for (Map.Entry<String, Collection<Store>> entry : collectedStoresWithLegacyPolicies.entrySet()) {
            String dataStructureName = entry.getKey();

            if (checkMergePolicySupportsInMemoryFormat(dataStructureName,
                    getMergePolicy(dataStructureName).getClass().getName(),
                    getInMemoryFormat(dataStructureName),
                    clusterService.getClusterVersion(), false, logger)) {

                Collection<Store> recordStores = entry.getValue();
                for (Store recordStore : recordStores) {
                    consumeStoreLegacy(recordStore, consumer);
                }
            }
        }

        return consumer.mergedCount;
    }

    private void waitMergeEnd(int mergedCount) {
        try {
            long timeoutMillis = Math.max(mergedCount * TIMEOUT_FACTOR, MINIMAL_TIMEOUT_MILLIS);
            if (!semaphore.tryAcquire(mergedCount, timeoutMillis, MILLISECONDS)) {
                logger.warning("Split-brain healing didn't finish within the timeout...");
            }
        } catch (InterruptedException e) {
            logger.finest("Interrupted while waiting for split-brain healing...");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Consumer to use when a {@link SplitBrainMergePolicy} type is used
     */
    private class MergingItemBiConsumer implements BiConsumer<Integer, MergingItem> {

        private final int batchSize;
        private final int partitionCount;
        private final String dataStructureName;
        private final Address[] addresses;
        private final MutableLong[] counterPerMember;
        private final SplitBrainMergePolicy mergePolicy;
        private final List<MergingItem>[] mergingItemsPerPartition;
        private final Map<Address, List<Integer>> memberPartitionsMap;

        private int mergedCount;

        MergingItemBiConsumer(String dataStructureName, SplitBrainMergePolicy mergePolicy, int batchSize) {
            this.dataStructureName = dataStructureName;
            this.batchSize = batchSize;
            this.mergePolicy = mergePolicy;
            this.memberPartitionsMap = partitionService.getMemberPartitionsMap();
            this.partitionCount = partitionService.getPartitionCount();
            this.addresses = new Address[partitionCount];
            this.counterPerMember = new MutableLong[partitionCount];
            //noinspection unchecked
            this.mergingItemsPerPartition = (List<MergingItem>[]) new List[partitionCount];

            init();
        }

        private void init() {
            // create a mapping between partition IDs and
            // a) an entry counter per member (a batch operation is sent out
            //    once this counter matches the batch size)
            // b) the member address (so we can retrieve the target address
            //    from the current partition ID)
            for (Map.Entry<Address, List<Integer>> addressListEntry : memberPartitionsMap.entrySet()) {
                MutableLong counter = new MutableLong();
                Address address = addressListEntry.getKey();
                for (int partitionId : addressListEntry.getValue()) {
                    counterPerMember[partitionId] = counter;
                    addresses[partitionId] = address;
                }
            }
        }

        @Override
        public void accept(Integer partitionId, MergingItem mergingItem) {
            List<MergingItem> entries = mergingItemsPerPartition[partitionId];
            if (entries == null) {
                entries = new LinkedList<MergingItem>();
                mergingItemsPerPartition[partitionId] = entries;
            }

            entries.add(mergingItem);
            mergedCount++;

            long currentSize = ++counterPerMember[partitionId].value;
            if (currentSize % batchSize == 0) {
                List<Integer> partitions = memberPartitionsMap.get(addresses[partitionId]);
                sendBatch(dataStructureName, partitions, mergingItemsPerPartition, mergePolicy);
            }
        }

        private void consumeRemaining() {
            for (Map.Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
                sendBatch(dataStructureName, entry.getValue(), mergingItemsPerPartition, mergePolicy);
            }
        }

        private void sendBatch(String dataStructureName, List<Integer> memberPartitions,
                               List<MergingItem>[] entriesPerPartition,
                               SplitBrainMergePolicy mergePolicy) {
            int size = memberPartitions.size();
            int[] partitions = new int[size];
            int index = 0;
            for (Integer partitionId : memberPartitions) {
                if (entriesPerPartition[partitionId] != null) {
                    partitions[index++] = partitionId;
                }
            }
            if (index == 0) {
                return;
            }
            // trim partition array to real size
            if (index < size) {
                partitions = Arrays.copyOf(partitions, index);
                size = index;
            }

            //noinspection unchecked
            List<MergingItem>[] entries = new List[size];
            index = 0;
            int totalSize = 0;
            for (int partitionId : partitions) {
                int batchSize = entriesPerPartition[partitionId].size();
                entries[index++] = entriesPerPartition[partitionId];
                totalSize += batchSize;
                entriesPerPartition[partitionId] = null;
            }
            if (totalSize == 0) {
                return;
            }

            sendMergingData(dataStructureName, mergePolicy, partitions, entries, totalSize);
        }

        private void sendMergingData(String dataStructureName, SplitBrainMergePolicy mergePolicy,
                                     int[] partitions, List<MergingItem>[] entries, int totalSize) {
            try {
                OperationFactory factory
                        = createMergeOperationFactory(dataStructureName, mergePolicy, partitions, entries);
                operationService.invokeOnPartitions(serviceName, factory, partitions);
            } catch (Throwable t) {
                logger.warning("Error while running merge operation: " + t.getMessage());
                throw rethrow(t);
            } finally {
                semaphore.release(totalSize);
            }
        }
    }

    /**
     * Consumer to use with legacy merge operations.
     */
    private class LegacyOperationBiConsumer implements BiConsumer<Integer, Operation> {

        private final ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
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

        private int mergedCount;

        @Override
        public void accept(Integer partitionId, Operation operation) {
            try {
                operationService.invokeOnPartition(serviceName, operation, partitionId)
                        .andThen(mergeCallback);
            } catch (Throwable t) {
                throw rethrow(t);
            }

            mergedCount++;
        }
    }

    @Override
    public final void dispose() {
        for (Collection<Store> stores : collectedStores.values()) {
            destroyStores(stores);
        }

        for (Collection<Store> stores : collectedStoresWithLegacyPolicies.values()) {
            destroyStores(stores);
        }

        destroyStores(backupStores);
    }

    protected InternalSerializationService getSerializationService() {
        return serializationService;
    }

    protected Data toData(Object object) {
        return serializationService.toData(object);
    }

    /**
     * Destroy a collection of stores
     */
    protected abstract void destroyStores(Collection<Store> stores);

    /**
     * Use to merge with policies which are a type of {@link SplitBrainMergePolicy}
     */
    protected abstract void consumeStore(Store recordStore, BiConsumer<Integer, MergingItem> consumer);

    /**
     * Use to merge with legacy merge policies
     */
    protected abstract void consumeStoreLegacy(Store recordStore, BiConsumer<Integer, Operation> consumer);

    /**
     * This batch size can only be used when merge policy is
     * a type of {@link SplitBrainMergePolicy}, legacy merge policies
     * don't support batch data sending.
     *
     * @return batch size from {@link com.hazelcast.config.MergePolicyConfig}
     */
    protected abstract int getBatchSize(String dataStructureName);

    /**
     * @return a type of {@link SplitBrainMergePolicy} or a legacy merge policy
     */
    protected abstract Object getMergePolicy(String dataStructureName);

    /**
     * @return in memory format of data structure
     */
    protected abstract InMemoryFormat getInMemoryFormat(String dataStructureName);

    /**
     * Returned {@link OperationFactory} is used for
     * {@link SplitBrainMergePolicy} types, legacy ones don't use this method.
     *
     * @return a new operation factory
     */
    protected abstract OperationFactory createMergeOperationFactory(String dataStructureName,
                                                                    SplitBrainMergePolicy mergePolicy,
                                                                    int[] partitions,
                                                                    List<MergingItem>[] entries);
}
