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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.completeVoidFuture;
import static com.hazelcast.util.CollectionUtil.toIntArray;

/**
 * Utility for cooperative writes to an IMap.
 * Not thread-safe.
 */
public class AsyncMapWriter {

    //TODO: What number to put here?
    public static final int MAX_PARALLEL_ASYNC_OPS = 1000;

    // These magic values are copied from com.hazelcast.spi.impl.operationservice.impl.InvokeOnPartitions
    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private final IPartitionService partitionService;
    private final OperationService operationService;
    private final MapService mapService;
    private final SerializationService serializationService;

    private final MapEntries[] outputBuffers; // one buffer per partition
    private final AtomicInteger numConcurrentOps; // num concurrent ops across whole instance
    private final ExecutionService executionService;
    private final ILogger logger;

    private String mapName;
    private MapOperationProvider opProvider;

    public AsyncMapWriter(NodeEngine nodeEngine) {
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.outputBuffers = new MapEntries[partitionService.getPartitionCount()];
        this.serializationService = nodeEngine.getSerializationService();
        this.executionService = nodeEngine.getExecutionService();
        this.logger = nodeEngine.getLogger(AsyncMapWriter.class);
        JetService jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.numConcurrentOps = jetService.numConcurrentPutAllOps();
    }

    public void put(Map.Entry<Data, Data> entry) {
        int partitionId = partitionService.getPartitionId(entry.getKey());
        MapEntries entries = outputBuffers[partitionId];
        if (entries == null) {
            entries = outputBuffers[partitionId] = new MapEntries();
        }
        entries.add(entry.getKey(), entry.getValue());
    }

    public void put(Object key, Object value) {
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);
        put(entry(keyData, valueData));
    }

    /**
     * Set to new map name. No operations must be in flight when this is done, or it might cause
     * retries to go to another map
     */
    public void setMapName(String mapName) {
        this.mapName = mapName;
        this.opProvider = mapService.getMapServiceContext().getMapOperationProvider(mapName);
    }

    /**
     * @return false, if the parallel operation limit is exceeded. The call
     * should be retried later.
     */
    public boolean tryFlushAsync(CompletableFuture<Void> completionFuture) {
        Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();
        List<PartitionOpBuilder> ops = memberPartitionsMap.entrySet()
                                                          .stream()
                                                          .map(e -> opForMember(e.getKey(), e.getValue(), outputBuffers))
                                                          .filter(Objects::nonNull)
                                                          .collect(Collectors.toList());

        if (!invokeOnCluster(ops, completionFuture, true)) {
            return false;
        }
        resetBuffers();
        return true;
    }

    private boolean tryRetry(int[] partitions, MapEntries[] entriesPerPtion, CompletableFuture<Void> completionFuture) {
        assert partitions.length == entriesPerPtion.length;
        Map<Address, Entry<List<Integer>, List<MapEntries>>> addrToEntries = new HashMap<>();
        for (int index = 0; index < partitions.length; index++) {
            int partition = partitions[index];
            MapEntries entries = entriesPerPtion[index];
            Address owner = partitionService.getPartitionOwnerOrWait(partition);
            assert owner != null : "null owner was returned";
            Entry<List<Integer>, List<MapEntries>> ptionsAndEntries
                    = addrToEntries.computeIfAbsent(owner, a -> entry(new ArrayList<>(), new ArrayList<>()));
            ptionsAndEntries.getValue().add(entries);
            ptionsAndEntries.getKey().add(partition);
        }

        List<PartitionOpBuilder> retryOps = addrToEntries
                .entrySet()
                .stream()
                .map(e -> {
                    PartitionOpBuilder h = new PartitionOpBuilder(e.getKey());
                    List<MapEntries> entries = e.getValue().getValue();
                    h.entries = entries.toArray(new MapEntries[entries.size()]);
                    h.partitions = CollectionUtil.toIntArray(e.getValue().getKey());
                    return h;
                }).collect(Collectors.toList());

        return invokeOnCluster(retryOps, completionFuture, false);
    }

    private PartitionOpBuilder opForMember(Address member, List<Integer> partitions, MapEntries[] partitionToEntries) {
        PartitionOpBuilder builder = new PartitionOpBuilder(member);
        builder.entries = new MapEntries[partitions.size()];
        builder.partitions = new int[partitions.size()];
        int index = 0;
        for (Integer partition : partitions) {
            if (partitionToEntries[partition] != null) {
                builder.entries[index] = partitionToEntries[partition];
                builder.partitions[index] = partition;
                index++;
            }
        }
        if (index == 0) {
            // no entries for this member, skip the member
            return null;
        }

        // trim arrays to real sizes
        if (index < partitions.size()) {
            builder.entries = Arrays.copyOf(builder.entries, index);
            builder.partitions = Arrays.copyOf(builder.partitions, index);
        }
        return builder;
    }


    private void resetBuffers() {
        Arrays.fill(outputBuffers, null);
    }

    private boolean reserveOps(int count) {
        int prev;
        int next;
        do {
            prev = numConcurrentOps.get();
            next = prev + count;
            if (next > MAX_PARALLEL_ASYNC_OPS) {
                return false;
            }
        } while (!numConcurrentOps.compareAndSet(prev, next));
        return true;
    }

    private boolean invokeOnCluster(List<PartitionOpBuilder> opBuilders,
                                    CompletableFuture<Void> completionFuture,
                                    boolean shouldRetry) {
        if (opBuilders.isEmpty()) {
            completeVoidFuture(completionFuture);
            return true;
        }

        if (!reserveOps(opBuilders.size())) {
            return false;
        }
        AtomicInteger doneLatch = new AtomicInteger(opBuilders.size());
        for (PartitionOpBuilder builder : opBuilders) {
            ExecutionCallback<PartitionResponse> callback = callbackOf(r -> {
                numConcurrentOps.decrementAndGet();

                // try to cherry-pick partitions which failed in this operation
                List<Integer> failedPartitions = new ArrayList<>();
                List<MapEntries> failedEntries = new ArrayList<>();
                Throwable error = null;
                Object[] results = r.getResults();
                for (int idx = 0; idx < results.length; idx++) {
                    Object o = results[idx];
                    if (o instanceof Throwable) {
                        error = (Throwable) o;
                        if (error instanceof RetryableException) {
                            failedPartitions.add(builder.partitions[idx]);
                            failedEntries.add(builder.entries[idx]);
                        } else {
                            completionFuture.completeExceptionally((Throwable) o);
                            return;
                        }
                    }
                }
                if (error != null) {
                    if (!shouldRetry) {
                        completionFuture.completeExceptionally(error);
                        return;
                    }

                    // retry once
                    final MapEntries[] entries = failedEntries.toArray(new MapEntries[failedEntries.size()]);
                    final int[] partitions = toIntArray(failedPartitions);
                    final Throwable originalErr = error;
                    executionService.schedule(() -> {
                        try {
                            if (!tryRetry(partitions, entries, completionFuture)) {
                                completionFuture.completeExceptionally(originalErr);
                            }

                        } catch (Exception e) {
                            logger.severe("Exception during retry", e);
                            completionFuture.completeExceptionally(originalErr);
                        }
                    }, TRY_PAUSE_MILLIS, TimeUnit.MILLISECONDS);
                    return;
                }
                if (doneLatch.decrementAndGet() == 0) {
                    completionFuture.complete(null);
                }

            }, throwable -> {
                numConcurrentOps.decrementAndGet();
                if (throwable instanceof RetryableException) {
                    // the whole operation to the member failed, so we need to retry
                    // all of the partitions in the operation
                    if (!tryRetry(builder.partitions, builder.entries, completionFuture)) {
                        completionFuture.completeExceptionally(throwable);
                    }
                } else {
                    completionFuture.completeExceptionally(throwable);
                }
            });
            operationService
                    .createInvocationBuilder(MapService.SERVICE_NAME, builder.build(), builder.address)
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .setExecutionCallback((ExecutionCallback) callback)
                    .invoke();
        }
        return true;
    }

    private <T> ExecutionCallback<T> callbackOf(Consumer<T> onResponse, Consumer<Throwable> onError) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                onResponse.accept(o);
            }

            @Override
            public void onFailure(Throwable throwable) {
                onError.accept(throwable);
            }
        };
    }

    private class PartitionOpBuilder {
        private final Address address;

        // PartitionIteratingOp doesn't expose these, so we have to track them separately
        private MapEntries[] entries; //entries in the operation
        private int[] partitions; // partitions in the operation

        PartitionOpBuilder(Address address) {
            this.address = address;
        }

        private PartitionIteratingOperation build() {
            OperationFactory factory = opProvider.createPutAllOperationFactory(mapName,
                    partitions, entries);
            return new PartitionIteratingOperation(factory, partitions);
        }

        @Override
        public String toString() {
            return "PartitionOpBuilder{" +
                    "address=" + address +
                    ", entryCount=" + entries.length +
                    ", partitions=" + Arrays.toString(partitions) +
                    '}';
        }
    }
}

