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

package com.hazelcast.spi.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Contains common functionality to implement a {@link SplitBrainHandlerService}
 *
 * @param <Store> store of a partition
 */
public abstract class AbstractSplitBrainHandlerService<Store> implements SplitBrainHandlerService {

    private final int partitionCount;
    private final NodeEngine nodeEngine;
    private final IPartitionService partitionService;

    protected AbstractSplitBrainHandlerService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
    }

    @Override
    public final Runnable prepareMergeRunnable() {
        onPrepareMergeRunnableStart();

        Map<String, Collection<Store>> collectedStores = new HashMap<String, Collection<Store>>();
        Map<String, Collection<Store>> collectedStoresWithLegacyPolicies = new HashMap<String, Collection<Store>>();
        List<Store> backupStores = new LinkedList<Store>();

        LinkedList<String> mergingDataStructureNames = new LinkedList<String>();

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            boolean partitionOwner = partitionService.isPartitionOwner(partitionId);
            Collection<Iterator<Store>> iterators = iteratorsOf(partitionId);
            for (Iterator<Store> iterator : iterators) {
                while (iterator.hasNext()) {
                    Store store = iterator.next();

                    if (partitionOwner) {
                        String dataStructureName = getDataStructureName(store);
                        Object mergePolicy = getMergePolicy(dataStructureName);

                        if (!isDiscardPolicy(mergePolicy)) {

                            if (mergePolicy instanceof SplitBrainMergePolicy) {
                                copyToCollectedStores(store, collectedStores);
                            } else {
                                copyToCollectedStores(store, collectedStoresWithLegacyPolicies);
                            }

                            mergingDataStructureNames.add(dataStructureName);
                        }
                    } else {
                        backupStores.add(store);
                    }

                    iterator.remove();
                }
            }
        }

        onPrepareMergeRunnableEnd(mergingDataStructureNames);

        return newMergeRunnable(collectedStores, collectedStoresWithLegacyPolicies, backupStores, nodeEngine);
    }

    protected void onPrepareMergeRunnableStart() {
        // NOP intentionally, implementations can override this method
    }

    protected void onPrepareMergeRunnableEnd(Collection<String> dataStructureNames) {
        // NOP intentionally, implementations can override this method
    }

    protected boolean isDiscardPolicy(Object mergePolicy) {
        return mergePolicy instanceof DiscardMergePolicy;
    }

    // overridden on ee
    public void destroyStores(Collection<Store> recordStores) {
        Iterator<Store> iterator = recordStores.iterator();
        while (iterator.hasNext()) {
            try {
                destroyStore(iterator.next());
            } finally {
                iterator.remove();
            }
        }
    }

    private void copyToCollectedStores(Store store, Map<String, Collection<Store>> collectedStores) {
        String dataStructureName = getDataStructureName(store);

        Collection<Store> stores = collectedStores.get(dataStructureName);
        if (stores == null) {
            stores = new LinkedList<Store>();
            collectedStores.put(dataStructureName, stores);
        }

        stores.add(store);
    }

    /**
     * Destroys provided store
     */
    protected abstract void destroyStore(Store store);

    /**
     * @return name of the data structure
     */
    protected abstract String getDataStructureName(Store store);

    /**
     * @return merge policy for the data structure
     */
    protected abstract Object getMergePolicy(String dataStructureName);

    /**
     * @return collection of iterators to scan data in the partition
     */
    protected abstract Collection<Iterator<Store>> iteratorsOf(int partitionId);

    /**
     * @return new merge runnable
     */
    protected abstract Runnable newMergeRunnable(Map<String, Collection<Store>> collectedStores,
                                                 Map<String, Collection<Store>> collectedStoresWithLegacyPolicies,
                                                 Collection<Store> backupStores,
                                                 NodeEngine nodeEngine);
}
