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

package com.hazelcast.multimap.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.locksupport.LockProxySupport;
import com.hazelcast.internal.locksupport.LockSupportServiceImpl;
import com.hazelcast.internal.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.multimap.impl.operations.CountOperation;
import com.hazelcast.multimap.impl.operations.DeleteOperation;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory.OperationFactoryType;
import com.hazelcast.multimap.impl.operations.MultiMapPutAllOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.multimap.impl.operations.PutOperation;
import com.hazelcast.multimap.impl.operations.RemoveAllOperation;
import com.hazelcast.multimap.impl.operations.RemoveOperation;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.CollectionUtil.asIntegerList;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.toIntSize;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.multimap.impl.MultiMapProxyImpl.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.multimap.impl.MultiMapProxyImpl.NULL_VALUE_IS_NOT_ALLOWED;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static java.util.Collections.singletonMap;

public abstract class MultiMapProxySupport extends AbstractDistributedObject<MultiMapService> {

    protected final MultiMapConfig config;
    protected final String name;
    protected final LockProxySupport lockSupport;
    protected final OperationService operationService;
    protected final IPartitionService partitionService;

    protected MultiMapProxySupport(MultiMapConfig config, MultiMapService service, NodeEngine nodeEngine, String name) {
        super(nodeEngine, service);
        this.config = config;
        this.name = name;

        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();

        lockSupport = new LockProxySupport(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name),
                LockSupportServiceImpl.getMaxLeaseTimeInMillis(nodeEngine.getProperties()));
    }

    @Override
    public String getName() {
        return name;
    }

    //NB: this method is copied from MapProxySupport#getPutAllInitialSize
    @SuppressWarnings("checkstyle:magicnumber")
    private int getPutAllInitialSize(int mapSize, int partitionCount) {
        if (mapSize == 1) {
            return 1;
        }
        return (mapSize / partitionCount) + 1;
    }

    //NB: this method is generally copied from MapProxySupport#putAllInternal
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:methodlength"})
    protected void putAllInternal(Map<Data, Data> map,
                                  @Nullable InternalCompletableFuture<Void> future) {

        //get partition to entries mapping
        try {
            int mapSize = map.size();
            if (mapSize == 0) {
                if (future != null) {
                    future.complete(null);
                }
                return;
            }

            int partitionCount = partitionService.getPartitionCount();
            int initialSize = getPutAllInitialSize(mapSize, partitionCount);

            //get node to partition mapping
            Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

            // fill entriesPerPartition
            MapEntries[] entriesPerPartition = new MapEntries[partitionCount];

            for (Map.Entry<Data, Data> entry : map.entrySet()) {
                checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
                checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

                Data keyData = entry.getKey();
                int partitionId = partitionService.getPartitionId(keyData);
                MapEntries entries = entriesPerPartition[partitionId];
                if (entries == null) {
                    entries = new MapEntries(initialSize);
                    entriesPerPartition[partitionId] = entries;
                }
                entries.add(keyData, entry.getValue());
            }
            // invoke operations for entriesPerPartition
            AtomicInteger counter = new AtomicInteger(memberPartitionsMap.size());
            InternalCompletableFuture<Void> resultFuture =
                    future != null ? future : new InternalCompletableFuture<>();
            BiConsumer<Void, Throwable> callback = (response, t) -> {
                if (t != null) {
                    resultFuture.completeExceptionally(t);
                }

                if (counter.decrementAndGet() == 0) {
                    if (!resultFuture.isDone()) {
                        resultFuture.complete(null);
                    }
                }
            };
            for (Map.Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
                invokePutAllOperation(entry.getKey(), entry.getValue(), entriesPerPartition).whenCompleteAsync(callback);
            }
            // if executing in sync mode, block for the responses
            if (future == null) {
                resultFuture.get();
            }
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    //NB: this method is generally copied from MapProxySupport#invokePutAllOperation
    private InternalCompletableFuture<Void> invokePutAllOperation(
            Address address,
            List<Integer> memberPartitions,
            MapEntries[] entriesPerPartition
    ) {
        int size = memberPartitions.size();
        int[] partitions = new int[size];
        int index = 0;
        for (Integer partitionId : memberPartitions) {
            if (entriesPerPartition[partitionId] != null) {
                partitions[index++] = partitionId;
            }
        }
        if (index == 0) {
            return newCompletedFuture(null);
        }
        // trim partition array to real size
        if (index < size) {
            partitions = Arrays.copyOf(partitions, index);
            size = index;
        }

        index = 0;
        MapEntries[] entries = new MapEntries[size];
        long totalSize = 0;
        for (int partitionId : partitions) {
            totalSize += entriesPerPartition[partitionId].size();
            entries[index++] = entriesPerPartition[partitionId];
            entriesPerPartition[partitionId] = null;
        }
        if (totalSize == 0) {
            return newCompletedFuture(null);
        }

        OperationFactory factory = new MultiMapPutAllOperationFactory(name, partitions, entries);
        long startTimeNanos = System.nanoTime();
        CompletableFuture<Map<Integer, Object>> future =
                operationService.invokeOnPartitionsAsync(MultiMapService.SERVICE_NAME, factory,
                        singletonMap(address, asIntegerList(partitions)));
        InternalCompletableFuture<Void> resultFuture = new InternalCompletableFuture<>();
        long finalTotalSize = totalSize;
        future.whenCompleteAsync((response, t) -> {
            if (t == null) {
                getService().getLocalMultiMapStatsImpl(name).incrementPutLatencyNanos(finalTotalSize,
                        System.nanoTime() - startTimeNanos);

                resultFuture.complete(null);
            } else {
                resultFuture.completeExceptionally(t);
            }
        }, CALLER_RUNS);
        return resultFuture;
    }

    protected Boolean putInternal(Data dataKey, Data dataValue, int index) {
        try {
            PutOperation operation = new PutOperation(name, dataKey, getThreadId(), dataValue, index);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected MultiMapResponse getAllInternal(Data dataKey) {
        try {
            GetAllOperation operation = new GetAllOperation(name, dataKey);
            operation.setThreadId(ThreadUtil.getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Boolean removeInternal(Data dataKey, Data dataValue) {
        try {
            RemoveOperation operation = new RemoveOperation(name, dataKey, getThreadId(), dataValue);
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected MultiMapResponse removeInternal(Data dataKey) {
        try {
            RemoveAllOperation operation = new RemoveAllOperation(name, dataKey, getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected void deleteInternal(Data dataKey) {
        try {
            DeleteOperation operation = new DeleteOperation(name, dataKey, getThreadId());
            invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Set<Data> localKeySetInternal() {
        return getService().localKeySet(name);
    }

    protected Set<Data> keySetInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {

            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.KEY_SET)
                    );
            Set<Data> keySet = new HashSet<>();
            for (Object result : results.values()) {
                if (result == null) {
                    continue;
                }
                MultiMapResponse response = nodeEngine.toObject(result);
                if (response.getCollection() != null) {
                    keySet.addAll(response.getCollection());
                }
            }
            return keySet;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Map valuesInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.VALUES)
                    );
            if (config.isStatisticsEnabled()) {
                getService().getLocalMultiMapStatsImpl(name).incrementOtherOperations();
            }
            return results;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected Map entrySetInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.ENTRY_SET)
                    );
            if (config.isStatisticsEnabled()) {
                getService().getLocalMultiMapStatsImpl(name).incrementOtherOperations();
            }
            return results;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected boolean containsInternal(Data key, Data value) {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.CONTAINS,
                                    key, value, ThreadUtil.getThreadId())
                    );
            if (config.isStatisticsEnabled()) {
                getService().getLocalMultiMapStatsImpl(name).incrementOtherOperations();
            }
            for (Object obj : results.values()) {
                if (obj == null) {
                    continue;
                }
                Boolean result = nodeEngine.toObject(obj);
                if (result) {
                    return true;
                }
            }
            return false;
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public int size() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            MultiMapService.SERVICE_NAME,
                            new MultiMapOperationFactory(name, OperationFactoryType.SIZE)
                    );
            if (config.isStatisticsEnabled()) {
                getService().getLocalMultiMapStatsImpl(name).incrementOtherOperations();
            }
            long size = 0;
            for (Object obj : results.values()) {
                if (obj == null) {
                    continue;
                }
                Integer result = nodeEngine.toObject(obj);
                size += result;
            }
            return toIntSize(size);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public void clear() {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> resultMap = nodeEngine.getOperationService().invokeOnAllPartitions(
                    MultiMapService.SERVICE_NAME, new MultiMapOperationFactory(name, OperationFactoryType.CLEAR)
            );
            if (config.isStatisticsEnabled()) {
                getService().getLocalMultiMapStatsImpl(name).incrementOtherOperations();
            }
            int numberOfAffectedEntries = 0;
            for (Object o : resultMap.values()) {
                numberOfAffectedEntries += (Integer) o;
            }
            publishMultiMapEvent(numberOfAffectedEntries, EntryEventType.CLEAR_ALL);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private void publishMultiMapEvent(int numberOfAffectedEntries, EntryEventType eventType) {
        getService().publishMultiMapEvent(name, eventType, numberOfAffectedEntries);
    }

    protected Integer countInternal(Data dataKey) {
        try {
            CountOperation operation = new CountOperation(name, dataKey);
            operation.setThreadId(ThreadUtil.getThreadId());
            return invoke(operation, dataKey);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    private <T> T invoke(Operation operation, Data dataKey) {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
            Object result;
            if (config.isStatisticsEnabled()) {
                long startTimeNanos = Timer.nanos();
                Future future;
                future = operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
                result = future.get();
                incrementOperationStats(startTimeNanos, name, operation);
            } else {
                Future future = operationService.invokeOnPartition(
                        MultiMapService.SERVICE_NAME,
                        operation,
                        partitionId
                );
                result = future.get();
            }
            return nodeEngine.toObject(result);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    private void incrementOperationStats(long startTimeNanos, String name, Operation operation) {
        LocalMultiMapStatsImpl localMultiMapStatsImpl = getService().getLocalMultiMapStatsImpl(name);
        final long durationNanos = Timer.nanosElapsed(startTimeNanos);
        if (operation instanceof PutOperation) {
            localMultiMapStatsImpl.incrementPutLatencyNanos(durationNanos);
        } else if (operation instanceof RemoveOperation || operation instanceof RemoveAllOperation
                || operation instanceof DeleteOperation) {
            localMultiMapStatsImpl.incrementRemoveLatencyNanos(durationNanos);
        } else if (operation instanceof GetAllOperation) {
            localMultiMapStatsImpl.incrementGetLatencyNanos(durationNanos);
        }
    }

    private long getThreadId() {
        return ThreadUtil.getThreadId();
    }

    @Override
    public String toString() {
        return "MultiMap{name=" + name + '}';
    }
}
