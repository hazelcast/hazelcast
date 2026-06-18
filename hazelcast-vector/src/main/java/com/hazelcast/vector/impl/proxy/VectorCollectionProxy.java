/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.proxy;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.VectorUtil;
import com.hazelcast.vector.impl.ops.ClearOperationsFactory;
import com.hazelcast.vector.impl.ops.DeleteOperation;
import com.hazelcast.vector.impl.ops.GetOperation;
import com.hazelcast.vector.impl.ops.OptimizeOperationsFactory;
import com.hazelcast.vector.impl.ops.PutAllOperationFactory;
import com.hazelcast.vector.impl.ops.PutIfAbsentOperation;
import com.hazelcast.vector.impl.ops.PutOperation;
import com.hazelcast.vector.impl.ops.RemoveOperation;
import com.hazelcast.vector.impl.ops.SetOperation;
import com.hazelcast.vector.impl.ops.SizeOperationsFactory;
import com.hazelcast.vector.impl.ops.VectorEntries;
import com.hazelcast.vector.impl.stats.LocalVectorCollectionStatsImpl;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.CollectionUtil.asIntegerList;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.vector.impl.VectorCollectionService.SERVICE_NAME;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.util.Collections.singletonMap;

public class VectorCollectionProxy<K, V> extends AbstractDistributedObject<VectorCollectionService>
        implements VectorCollection<K, V> {

    private final String name;
    private final IPartitionService partitionService;
    private final SerializationService serializationService;
    private final VectorCollectionConfig config;
    private final LocalVectorCollectionStatsImpl statistics;

    private int putAllBatchSize;

    public VectorCollectionProxy(NodeEngine nodeEngine, VectorCollectionService service,
                                 String name, VectorCollectionConfig config) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.serializationService = nodeEngine.getSerializationService();
        // when the object is destroyed but the old proxy is still used,
        // operation statistics will not be recorded
        this.statistics = service.getStatistics(name);
        this.config = config;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public CompletionStage<VectorDocument<V>> getAsync(@Nonnull K key) {
        Objects.requireNonNull(key, "key cannot be null");
        Data keyData = serializationService.toData(key);
        return invokeOnKeyOwnerAsyncAndDeserialize(new GetOperation(name, keyData), keyData,
                LocalVectorCollectionStatsImpl::incrementGetLatencyNanos);

    }

    @Override
    public CompletionStage<VectorDocument<V>> putAsync(@Nonnull K key, @Nonnull VectorDocument<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value.getValue());
        return invokeOnKeyOwnerAsyncAndDeserialize(new PutOperation(name, keyData, valueData, value.getVectors()), keyData,
                LocalVectorCollectionStatsImpl::incrementPutLatencyNanos);
    }

    @Override
    public CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull VectorDocument<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value.getValue());
        return invokeOnKeyOwnerAsyncAndWrap(new SetOperation(name, keyData, valueData, value.getVectors()), keyData,
                LocalVectorCollectionStatsImpl::incrementSetLatencyNanos);
    }

    @Override
    public CompletionStage<VectorDocument<V>> putIfAbsentAsync(@Nonnull K key, @Nonnull VectorDocument<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value.getValue());
        return invokeOnKeyOwnerAsyncAndDeserialize(
                new PutIfAbsentOperation(name, keyData, valueData, value.getVectors()), keyData,
                LocalVectorCollectionStatsImpl::incrementPutLatencyNanos);
    }

    @Override
    public CompletionStage<Void> putAllAsync(Map<? extends K, VectorDocument<V>> documents) {
        Objects.requireNonNull(documents, "Null documents map is not allowed");
        var startTimeNanos = Timer.nanos();
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        putAllInternalAsync(documents, future);
        return future.thenAcceptAsync(v ->
                statistics.incrementPutAllLatencyNanos(documents.size(), Timer.nanosElapsed(startTimeNanos)), CALLER_RUNS);
    }

    @Override
    public CompletionStage<VectorDocument<V>> removeAsync(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        Data keyData = serializationService.toData(key);
        return invokeOnKeyOwnerAsyncAndDeserialize(new RemoveOperation(name, keyData), keyData,
                LocalVectorCollectionStatsImpl::incrementRemoveLatencyNanos);
    }

    @Override
    public CompletionStage<Void> deleteAsync(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        Data keyData = serializationService.toData(key);
        return invokeOnKeyOwnerAsyncAndWrap(new DeleteOperation(name, keyData), keyData,
                LocalVectorCollectionStatsImpl::incrementDeleteLatencyNanos);
    }

    @Override
    public CompletionStage<Void> optimizeAsync(String indexName) {
        var startTimeNanos = Timer.nanos();
        var factory = new OptimizeOperationsFactory(name, indexName);
        return getOperationService()
                .invokeOnAllPartitionsAsync(VectorCollectionService.SERVICE_NAME, factory)
                .thenAcceptAsync((res) -> statistics.incrementOptimizeLatencyNanos(startTimeNanos), CALLER_RUNS);
    }

    @Override
    public CompletionStage<Void> clearAsync() {
        var startTimeNanos = Timer.nanos();
        var factory = new ClearOperationsFactory(name);
        return getOperationService()
                .invokeOnAllPartitionsAsync(VectorCollectionService.SERVICE_NAME, factory)
                .thenAcceptAsync((res) -> statistics.incrementClearLatencyNanos(startTimeNanos), CALLER_RUNS);
    }

    @Override
    public long size() {
        try {
            var startTimeNanos = Timer.nanos();
            var results = getOperationService().invokeOnAllPartitions(SERVICE_NAME, new SizeOperationsFactory(name));
            long sum = results.values().stream()
                    .mapToLong(o -> (long) o)
                    .sum();
            statistics.incrementSizeLatencyNanos(startTimeNanos);
            return sum;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public CompletionStage<SearchResults<K, V>> searchAsync(VectorValues vectors, SearchOptions searchOptions) {
        Objects.requireNonNull(vectors, "vectors cannot be null");
        Objects.requireNonNull(searchOptions, "searchOptions cannot be null");

        var startTimeNanos = Timer.nanos();
        return getService().getSearcher(name, searchOptions)
                .search(name, vectors, searchOptions)
                // deserialization also materializes the query results and is single-pass over the merged iterator
                .thenApplyAsync(merged -> {
                            statistics.incrementSearchLatencyNanos(merged.size(), Timer.nanosElapsed(startTimeNanos));
                            return VectorUtil.deserialize(merged, serializationService);
                        },
                        CALLER_RUNS);
    }

    /**
     * Enables or disables {@link #putAllAsync} batching.
     */
    public void setPutAllBatchSize(int putAllBatchSize) {
        this.putAllBatchSize = putAllBatchSize;
    }

    /**
     * This method will group all entries per partition and send one operation
     * per member. If there are e.g. five keys for a single member, even if
     * they are from different partitions, there will only be a single remote
     * invocation instead of five.
     * <p>
     * There is also an optional support for batching to send smaller packages.
     * Takes care about {@code null} checks for keys and values.
     *
     * @param future execute asynchronously by completing this future.
     */
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    protected void putAllInternalAsync(Map<? extends K, ? extends VectorDocument<V>> map,
                                       @Nonnull InternalCompletableFuture<Void> future) {
        try {
            int mapSize = map.size();
            if (mapSize == 0) {
                future.complete(null);
                return;
            }

            boolean useBatching = isPutAllUseBatching(mapSize);
            int partitionCount = partitionService.getPartitionCount();
            int initialSize = getPutAllInitialSize(useBatching, mapSize, partitionCount);

            Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

            // init counters for batching
            MutableLong[] counterPerMember = null;
            Address[] addresses = null;
            if (useBatching) {
                counterPerMember = new MutableLong[partitionCount];
                addresses = new Address[partitionCount];
                for (Map.Entry<Address, List<Integer>> addressListEntry : memberPartitionsMap.entrySet()) {
                    MutableLong counter = new MutableLong();
                    Address address = addressListEntry.getKey();
                    for (int partitionId : addressListEntry.getValue()) {
                        counterPerMember[partitionId] = counter;
                        addresses[partitionId] = address;
                    }
                }
            }

            // Without batching there will be 1 invocation per member.
            // With batching add extra 1 so the future is not finished early as we are adding
            // new batches on the fly. This 1 will be removed after all requests are sent.
            AtomicInteger counter = new AtomicInteger(useBatching ? 1 : memberPartitionsMap.size());
            BiConsumer<Void, Throwable> callback = (response, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                }
                if (counter.decrementAndGet() == 0) {
                    if (!future.isDone()) {
                        future.complete(null);
                    }
                }
            };

            // fill entriesPerPartition
            VectorEntries[] entriesPerPartition = new VectorEntries[partitionCount];
            for (var entry : map.entrySet()) {
                checkNotNull(entry.getKey(), "key cannot be null");
                checkNotNull(entry.getValue(), "value cannot be null");

                Data keyData = toData(entry.getKey());
                int partitionId = partitionService.getPartitionId(keyData);
                VectorEntries entries = entriesPerPartition[partitionId];
                if (entries == null) {
                    entries = new VectorEntries(initialSize);
                    entriesPerPartition[partitionId] = entries;
                }

                entries.add(keyData, VectorUtil.serialize(entry.getValue(), serializationService));

                if (useBatching) {
                    long currentSize = ++counterPerMember[partitionId].value;
                    if (currentSize % putAllBatchSize == 0) {
                        List<Integer> partitions = memberPartitionsMap.get(addresses[partitionId]);
                        // TODO: implement backpressure or limit number of in-flight batches.
                        //  In this implementation the operations will be smaller, but there may be many of them
                        counter.incrementAndGet();
                        invokePutAllOperation(addresses[partitionId], partitions, entriesPerPartition, true)
                                .whenCompleteAsync(callback, ConcurrencyUtil.getDefaultAsyncExecutor());
                    }
                }
            }

            // invoke operations for entriesPerPartition
            // with batching it sends any remaining entries.
            for (Map.Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
                if (useBatching) {
                    counter.incrementAndGet();
                }
                invokePutAllOperation(entry.getKey(), entry.getValue(), entriesPerPartition, useBatching)
                        .whenCompleteAsync(callback, ConcurrencyUtil.getDefaultAsyncExecutor());
            }

            if (useBatching) {
                // check if we are already done (could happen if operations are fast)
                callback.accept(null, null);
            }
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private boolean isPutAllUseBatching(int mapSize) {
        // we check if the feature is enabled and if the map size is bigger than a single batch per member
        return (putAllBatchSize > 0 && mapSize > putAllBatchSize * getNodeEngine().getClusterService().getSize());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int getPutAllInitialSize(boolean useBatching, int mapSize, int partitionCount) {
        if (mapSize == 1) {
            return 1;
        }
        if (useBatching) {
            return putAllBatchSize;
        }
        // this is an educated guess for the initial size of the entries per partition, depending on the map size
        return (int) ceil(20f * mapSize / partitionCount / log10(mapSize));
    }

    @Nonnull
    private CompletableFuture<Void> invokePutAllOperation(
            Address address,
            List<Integer> memberPartitions,
            VectorEntries[] entriesPerPartition,
            boolean useBatching
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
        VectorEntries[] entries = new VectorEntries[size];
        long totalSize = 0;
        for (int partitionId : partitions) {
            int batchSize = entriesPerPartition[partitionId].size();
            assert !useBatching || putAllBatchSize == 0 || batchSize <= putAllBatchSize;
            entries[index++] = entriesPerPartition[partitionId];
            totalSize += batchSize;
            entriesPerPartition[partitionId] = null;
        }
        if (totalSize == 0) {
            return newCompletedFuture(null);
        }

        OperationFactory factory = new PutAllOperationFactory(name, partitions, entries);
        return getOperationService()
                .invokeOnPartitionsAsync(SERVICE_NAME, factory, singletonMap(address, asIntegerList(partitions)))
                .thenApplyAsync(v -> null, CALLER_RUNS);
    }

    private CompletionStage<VectorDocument<V>> invokeOnKeyOwnerAsyncAndDeserialize(
            Operation op, Data keyData, BiConsumer<LocalVectorCollectionStatsImpl, Long> statsUpdater) {
        var startTimeNanos = Timer.nanos();

        return this.<VectorDocument<Data>>invokeOnKeyOwnerAsync(op, keyData)
                .thenApplyAsync(
                        dataVectorDocument -> {
                            recordStats(startTimeNanos, statsUpdater);
                            return VectorUtil.deserialize(dataVectorDocument, serializationService);
                        },
                        CALLER_RUNS
                );
    }

    private <T> CompletionStage<T> invokeOnKeyOwnerAsyncAndWrap(Operation op, Data keyData,
                                                                BiConsumer<LocalVectorCollectionStatsImpl, Long> statsUpdater) {
        // Dummy thenApply is a workaround for InvocationFuture chaining problems (HZOLD-4709).
        var startTimeNanos = Timer.nanos();
        return this.<T>invokeOnKeyOwnerAsync(op, keyData)
                .thenApplyAsync(v -> {
                    recordStats(startTimeNanos, statsUpdater);
                    return v;
                }, CALLER_RUNS);
    }

    private void recordStats(long startTimeNanos, BiConsumer<LocalVectorCollectionStatsImpl, Long> statsUpdater) {
        statsUpdater.accept(statistics, Timer.nanosElapsed(startTimeNanos));
    }

    private <T> CompletionStage<T> invokeOnKeyOwnerAsync(Operation op, Data keyData) {
        return getOperationService().invokeOnPartitionAsync(SERVICE_NAME, op, getPartitionId(keyData));
    }
}
