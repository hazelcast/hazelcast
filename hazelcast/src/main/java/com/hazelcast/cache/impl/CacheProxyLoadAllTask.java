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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.operation.CacheLoadAllOperation;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateResults;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Task for creating and invoking {@link CacheLoadAllOperation} operations on all partitions owners of all provided keys.
 * Optional {@link CompletionListener} can be provided for callback after all operations are finished.
 */
final class CacheProxyLoadAllTask implements Runnable {

    /**
     * Speculative factor to be used when initialising collections
     * of an approximate final size.
     */
    private static final double SIZING_FUDGE_FACTOR = 1.3;

    private final NodeEngine nodeEngine;
    private CompletionListener completionListener;
    private final CacheOperationProvider operationProvider;
    private final Set<Data> keysData;
    private final boolean replaceExistingValues;
    private final String serviceName;

    CacheProxyLoadAllTask(NodeEngine nodeEngine, CacheOperationProvider operationProvider, Set<Data> keysData,
                          boolean replaceExistingValues,
                          CompletionListener completionListener, String serviceName) {
        this.nodeEngine = nodeEngine;
        this.operationProvider = operationProvider;
        this.keysData = keysData;
        this.replaceExistingValues = replaceExistingValues;
        this.completionListener = completionListener;
        this.serviceName = serviceName;
    }

    @Override
    public void run() {
        try {
            completionListener = injectDependencies(completionListener);

            OperationService operationService = nodeEngine.getOperationService();
            OperationFactory operationFactory;

            IPartitionService partitionService = nodeEngine.getPartitionService();
            Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();
            int partitionCount = partitionService.getPartitionCount();
            Map<Integer, Object> results = createHashMap(partitionCount);

            for (Map.Entry<Address, List<Integer>> memberPartitions : memberPartitionsMap.entrySet()) {
                Set<Integer> partitions = new PartitionIdSet(partitionCount, memberPartitions.getValue());
                Set<Data> ownerKeys = filterOwnerKeys(partitionService, partitions);
                operationFactory = operationProvider.createLoadAllOperationFactory(ownerKeys, replaceExistingValues);
                Map<Integer, Object> memberResults;
                memberResults = operationService.invokeOnPartitions(serviceName, operationFactory, partitions);
                results.putAll(memberResults);
            }

            validateResults(results);
            if (completionListener != null) {
                completionListener.onCompletion();
            }
        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
        } catch (Throwable t) {
            if (t instanceof OutOfMemoryError) {
                throw rethrow(t);
            } else {
                if (completionListener != null) {
                    completionListener.onException(new CacheException(t));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T injectDependencies(Object obj) {
        ManagedContext managedContext = nodeEngine.getSerializationService().getManagedContext();
        return (T) managedContext.initialize(obj);
    }

    private Set<Data> filterOwnerKeys(IPartitionService partitionService, Set<Integer> partitions) {
        //assume that the key data is evenly distributed over the partition count, so multiply by number of partitions
        final int roughSize = (int) (keysData.size() * partitions.size() / (double) partitionService.getPartitionCount()
                * SIZING_FUDGE_FACTOR);
        Set<Data> ownerKeys = createHashSet(roughSize);
        for (Data key : keysData) {
            int keyPartitionId = partitionService.getPartitionId(key);
            if (partitions.contains(keyPartitionId)) {
                ownerKeys.add(key);
            }
        }
        return ownerKeys;
    }
}
