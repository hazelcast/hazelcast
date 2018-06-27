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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.MutableLong;
import com.hazelcast.util.function.BiConsumer;
import com.hazelcast.version.Version;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicySupportsInMemoryFormat;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Used by {@link com.hazelcast.cache.ICache}, {@link com.hazelcast.core.IMap}
 * and {@link com.hazelcast.core.ReplicatedMap} to provide a merge runnable
 * for {@link SplitBrainHandlerService#prepareMergeRunnable()}.
 *
 * @param <K>           type of the store key
 * @param <V>           type of the store value
 * @param <Store>       type of the store in a partition
 * @param <MergingItem> type of the merging item
 */
public abstract class AbstractMergeRunnable<K, V, Store, MergingItem extends MergingEntry<K, V>> implements Runnable {

    private static final long TIMEOUT_FACTOR = 500;
    private static final long MINIMAL_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private final Semaphore semaphore = new Semaphore(0);

    private final ILogger logger;
    private final String serviceName;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final IPartitionService partitionService;
    private final AbstractSplitBrainHandlerService<Store> splitBrainHandlerService;
    private final InternalSerializationService serializationService;

    private Map<String, Collection<Store>> mergingStoresByName;

    protected AbstractMergeRunnable(String serviceName,
                                    Collection<Store> mergingStores,
                                    AbstractSplitBrainHandlerService<Store> splitBrainHandlerService,
                                    NodeEngine nodeEngine) {
        this.mergingStoresByName = groupStoresByName(mergingStores);
        this.serviceName = serviceName;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.clusterService = nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.splitBrainHandlerService = splitBrainHandlerService;
    }

    private Map<String, Collection<Store>> groupStoresByName(Collection<Store> stores) {
        Map<String, Collection<Store>> storesByName = new HashMap<String, Collection<Store>>();
        for (Store store : stores) {
            String dataStructureName = getDataStructureName(store);

            Collection<Store> storeList = storesByName.get(dataStructureName);
            if (storeList == null) {
                storeList = new LinkedList<Store>();
                storesByName.put(dataStructureName, storeList);
            }
            storeList.add(store);
        }
        return storesByName;
    }

    @Override
    public final void run() {
        onRunStart();
        int mergedCount = 0;

        mergedCount += mergeWithSplitBrainMergePolicy();
        mergedCount += mergeWithLegacyMergePolicy();

        waitMergeEnd(mergedCount);
    }

    protected void onRunStart() {
        // Implementers can override this method.
    }

    private int mergeWithSplitBrainMergePolicy() {
        int mergedCount = 0;
        Iterator<Map.Entry<String, Collection<Store>>> iterator = mergingStoresByName.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Collection<Store>> entry = iterator.next();

            String dataStructureName = entry.getKey();
            Collection<Store> stores = entry.getValue();

            if (getMergePolicy(dataStructureName) instanceof SplitBrainMergePolicy) {
                if (canMerge(dataStructureName)) {
                    MergingItemBiConsumer consumer = newConsumer(dataStructureName);
                    for (Store store : stores) {
                        try {
                            mergeStore(store, consumer);
                            consumer.consumeRemaining();
                        } finally {
                            asyncDestroyStores(singleton(store));
                        }
                    }
                    mergedCount += consumer.mergedCount;
                    onMerge(dataStructureName);
                } else {
                    asyncDestroyStores(stores);
                }
                iterator.remove();
            }
        }
        return mergedCount;
    }

    /**
     * Check if data structure can use {@link SplitBrainMergePolicy}
     */
    private boolean canMerge(String dataStructureName) {
        Version currentVersion = clusterService.getClusterVersion();
        if (currentVersion.isGreaterOrEqual(V3_10)) {
            return true;
        }
        // RU_COMPAT_3_9
        String msg = "Cannot merge '%s' with merge policy '%s'. Cluster version should be %s or later but found %s";
        logger.info(format(msg, dataStructureName, getMergePolicy(dataStructureName), V3_10, currentVersion));
        return false;
    }

    private MergingItemBiConsumer newConsumer(String dataStructureName) {
        SplitBrainMergePolicy<V, MergingItem> policy = getSplitBrainMergePolicy(dataStructureName);
        int batchSize = getBatchSize(dataStructureName);
        return new MergingItemBiConsumer(dataStructureName, policy, batchSize);
    }

    @SuppressWarnings("unchecked")
    private SplitBrainMergePolicy<V, MergingItem> getSplitBrainMergePolicy(String dataStructureName) {
        return ((SplitBrainMergePolicy<V, MergingItem>) getMergePolicy(dataStructureName));
    }

    private int mergeWithLegacyMergePolicy() {
        LegacyOperationBiConsumer consumer = new LegacyOperationBiConsumer();

        Iterator<Map.Entry<String, Collection<Store>>> iterator = mergingStoresByName.entrySet().iterator();
        while (iterator.hasNext()) {
            try {
                Map.Entry<String, Collection<Store>> entry = iterator.next();

                String dataStructureName = entry.getKey();
                Collection<Store> stores = entry.getValue();

                if (canMergeLegacy(dataStructureName)) {
                    for (Store store : stores) {
                        try {
                            mergeStoreLegacy(store, consumer);
                        } finally {
                            asyncDestroyStores(singleton(store));
                        }
                    }
                    onMerge(dataStructureName);
                } else {
                    asyncDestroyStores(stores);
                }
            } finally {
                iterator.remove();
            }
        }

        return consumer.mergedCount;
    }

    /**
     * Check if data structures in-memory-format appropriate to merge
     * with legacy policies
     */
    private boolean canMergeLegacy(String dataStructureName) {
        Object mergePolicy = getMergePolicy(dataStructureName);
        InMemoryFormat inMemoryFormat = getInMemoryFormat(dataStructureName);
        Version clusterVersion = clusterService.getClusterVersion();

        return checkMergePolicySupportsInMemoryFormat(dataStructureName,
                mergePolicy, inMemoryFormat, clusterVersion, false, logger);
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
     * Consumer for {@link SplitBrainMergePolicy}.
     */
    private class MergingItemBiConsumer implements BiConsumer<Integer, MergingItem> {

        private final int batchSize;
        private final int partitionCount;
        private final String dataStructureName;
        private final Address[] addresses;
        private final MutableLong[] counterPerMember;
        private final SplitBrainMergePolicy<V, MergingItem> mergePolicy;
        private final List<MergingItem>[] mergingItemsPerPartition;
        private final Map<Address, List<Integer>> memberPartitionsMap;

        private int mergedCount;

        MergingItemBiConsumer(String dataStructureName, SplitBrainMergePolicy<V, MergingItem> mergePolicy, int batchSize) {
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

        private void sendBatch(String dataStructureName, List<Integer> memberPartitions, List<MergingItem>[] entriesPerPartition,
                               SplitBrainMergePolicy<V, MergingItem> mergePolicy) {
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

        private void sendMergingData(String dataStructureName, SplitBrainMergePolicy<V, MergingItem> mergePolicy,
                                     int[] partitions, List<MergingItem>[] entries, int totalSize) {
            try {
                OperationFactory factory = createMergeOperationFactory(dataStructureName, mergePolicy, partitions, entries);
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
     * Consumer for legacy merge operations.
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

    protected InternalSerializationService getSerializationService() {
        return serializationService;
    }

    protected Data toData(Object object) {
        return serializationService.toData(object);
    }

    protected Data toHeapData(Object object) {
        return serializationService.toData(object, DataType.HEAP);
    }

    private void asyncDestroyStores(Collection<Store> stores) {
        for (Store store : stores) {
            splitBrainHandlerService.asyncDestroyStores(singleton(store), getPartitionId(store));
        }
    }

    protected void onMerge(String dataStructureName) {
        // override to take action on merge
    }

    /**
     * Used to merge with {@link SplitBrainMergePolicy}.
     */
    protected abstract void mergeStore(Store recordStore, BiConsumer<Integer, MergingItem> consumer);

    /**
     * Used to merge with legacy merge policies.
     */
    protected abstract void mergeStoreLegacy(Store recordStore, BiConsumer<Integer, Operation> consumer);

    /**
     * This batch size can only be used with {@link SplitBrainMergePolicy},
     * legacy merge policies don't support batch data sending.
     *
     * @return batch size from {@link com.hazelcast.config.MergePolicyConfig}
     */
    protected abstract int getBatchSize(String dataStructureName);

    /**
     * @return a type of {@link SplitBrainMergePolicy} or a legacy merge policy
     */
    protected abstract Object getMergePolicy(String dataStructureName);

    protected abstract String getDataStructureName(Store store);

    protected abstract int getPartitionId(Store store);

    /**
     * @return in memory format of data structure
     */
    protected abstract InMemoryFormat getInMemoryFormat(String dataStructureName);

    /**
     * Returns an {@link OperationFactory} for {@link SplitBrainMergePolicy},
     * legacy merge policies don't use this method.
     *
     * @return a new operation factory
     */
    protected abstract OperationFactory createMergeOperationFactory(String dataStructureName,
                                                                    SplitBrainMergePolicy<V, MergingItem> mergePolicy,
                                                                    int[] partitions, List<MergingItem>[] entries);
}
