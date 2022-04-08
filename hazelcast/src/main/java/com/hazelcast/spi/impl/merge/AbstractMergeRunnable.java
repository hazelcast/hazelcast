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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Used by {@link com.hazelcast.cache.ICache}, {@link IMap}
 * and {@link ReplicatedMap} to provide a merge runnable
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

    protected final SplitBrainMergePolicyProvider mergePolicyProvider;

    private final ILogger logger;
    private final String serviceName;
    private final OperationService operationService;
    private final IPartitionService partitionService;
    private final InternalSerializationService serializationService;
    private final AbstractSplitBrainHandlerService<Store> splitBrainHandlerService;
    private final Semaphore semaphore = new Semaphore(0);

    private Map<String, Collection<Store>> mergingStoresByName;

    protected AbstractMergeRunnable(String serviceName,
                                    Collection<Store> mergingStores,
                                    AbstractSplitBrainHandlerService<Store> splitBrainHandlerService,
                                    NodeEngine nodeEngine) {
        this.mergingStoresByName = groupStoresByName(mergingStores);
        this.serviceName = serviceName;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.splitBrainHandlerService = splitBrainHandlerService;
    }

    private Map<String, Collection<Store>> groupStoresByName(Collection<Store> stores) {
        Map<String, Collection<Store>> storesByName = new HashMap<String, Collection<Store>>();
        for (Store store : stores) {
            String dataStructureName = getDataStructureName(store);

            Collection<Store> storeList
                    = storesByName.computeIfAbsent(dataStructureName, k -> new LinkedList<>());
            storeList.add(store);
        }
        return storesByName;
    }

    @Override
    public final void run() {
        onRunStart();
        int mergedCount = 0;

        mergedCount += mergeWithSplitBrainMergePolicy();

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
            iterator.remove();
        }
        return mergedCount;
    }

    private MergingItemBiConsumer newConsumer(String dataStructureName) {
        SplitBrainMergePolicy<V, MergingItem, Object> policy = getMergePolicy(dataStructureName);
        int batchSize = getBatchSize(dataStructureName);
        return new MergingItemBiConsumer(dataStructureName, policy, batchSize);
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
        private final SplitBrainMergePolicy<V, MergingItem, Object> mergePolicy;
        private final List<MergingItem>[] mergingItemsPerPartition;
        private final Map<Address, List<Integer>> memberPartitionsMap;

        private int mergedCount;

        MergingItemBiConsumer(String dataStructureName, SplitBrainMergePolicy<V, MergingItem, Object> mergePolicy,
                              int batchSize) {
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
                entries = new LinkedList<>();
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
                               SplitBrainMergePolicy<V, MergingItem, Object> mergePolicy) {
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

        private void sendMergingData(String dataStructureName, SplitBrainMergePolicy<V, MergingItem, Object> mergePolicy,
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
     * This batch size can only be used with {@link SplitBrainMergePolicy}.
     *
     * @return batch size from {@link com.hazelcast.config.MergePolicyConfig}
     */
    protected abstract int getBatchSize(String dataStructureName);

    /**
     * @return a type of {@link SplitBrainMergePolicy}
     */
    protected abstract SplitBrainMergePolicy<V, MergingItem, Object> getMergePolicy(String dataStructureName);

    protected abstract String getDataStructureName(Store store);

    protected abstract int getPartitionId(Store store);

    /**
     * Returns an {@link OperationFactory} for {@link SplitBrainMergePolicy}.
     *
     * @return a new operation factory
     */
    protected abstract OperationFactory createMergeOperationFactory(String dataStructureName,
                                                                    SplitBrainMergePolicy<V, MergingItem, Object> mergePolicy,
                                                                    int[] partitions, List<MergingItem>[] entries);
}
