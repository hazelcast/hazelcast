/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector;

import com.hazelcast.connector.map.Hz3MapAdapter;
import com.hazelcast.connector.map.Reader;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * Adapted from last Jet 3.x release
 * https://github.com/hazelcast/hazelcast-jet/tree/v3.2.2
 * from
 * hazelcast-jet-core/src/main/java/com/hazelcast/jet/impl/connector/ReadMapOrCacheP.java
 */
final class ReadMapOrCacheP<F extends CompletableFuture, B, R> extends AbstractProcessor {

    private final Reader<F, B, R> reader;
    private final int[] partitionIds;
    private final BooleanSupplier migrationWatcher;
    private final int[] readOffsets;

    private F[] readFutures;

    // currently emitted batch, its iterating position and partitionId
    private List<R> currentBatch = Collections.emptyList();
    private int currentBatchPosition;
    private int currentPartitionIndex = -1;
    private int numCompletedPartitions;

    private ReadMapOrCacheP(
            @Nonnull Reader<F, B, R> reader,
            @Nonnull int[] partitionIds,
            @Nonnull BooleanSupplier migrationWatcher

    ) {
        this.reader = reader;
        this.partitionIds = partitionIds;
        this.migrationWatcher = migrationWatcher;

        readOffsets = new int[partitionIds.length];
        Arrays.fill(readOffsets, Integer.MAX_VALUE);
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        while (emitResultSet()) {
            if (!tryGetNextResultSet()) {
                return numCompletedPartitions == partitionIds.length;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void initialRead() {
        readFutures = (F[]) new CompletableFuture[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = reader.readBatch(partitionIds[i], Integer.MAX_VALUE);
        }
    }

    private boolean emitResultSet() {
        checkMigration();
        for (; currentBatchPosition < currentBatch.size(); currentBatchPosition++) {
            Object result = reader.toObject(currentBatch.get(currentBatchPosition));
            if (result == null) {
                // element was filtered out by the predicate (?)
                continue;
            }
            if (!tryEmit(result)) {
                return false;
            }
        }
        // we're done with the current batch
        return true;
    }

    private boolean tryGetNextResultSet() {
        while (currentBatch.size() == currentBatchPosition && ++currentPartitionIndex < partitionIds.length) {
            if (readOffsets[currentPartitionIndex] < 0) {  // partition is completed
                assert readFutures[currentPartitionIndex] == null : "future not null";
                continue;
            }

            F future = readFutures[currentPartitionIndex];
            if (!future.isDone()) {  // data for partition not yet available
                continue;
            }

            B result = toBatchResult(future);

            int nextIndex = reader.toNextIndex(result);
            if (nextIndex < 0) {
                numCompletedPartitions++;
            } else {
                assert !currentBatch.isEmpty() : "empty but not terminal batch";
            }

            currentBatch = reader.toRecordSet(result);
            currentBatchPosition = 0;
            readOffsets[currentPartitionIndex] = nextIndex;
            // make another read on the same partition
            readFutures[currentPartitionIndex] = readOffsets[currentPartitionIndex] >= 0
                    ? reader.readBatch(partitionIds[currentPartitionIndex], readOffsets[currentPartitionIndex])
                    : null;
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            return false;
        }
        return true;
    }

    private B toBatchResult(F future) {
        B result;
        try {
            result = reader.toBatchResult(future);
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastSerializationException) {
                throw new JetException("Serialization error when reading the map: are the key, value, " +
                                       "predicate and projection classes visible to IMDG? You need to use User Code " +
                                       "Deployment, adding the classes to JetConfig isn't enough", e);
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
        return result;
    }

    private void checkMigration() {
        if (migrationWatcher.getAsBoolean()) {
            throw new RestartableException("Partition migration detected");
        }
    }

    static class RemoteProcessorSupplier<F extends CompletableFuture, B, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private final String clientXml;

        private transient Hz3MapAdapter hz3MapAdapter;

        private transient InternalSerializationService serializationService;
        private transient int totalParallelism;
        private transient int baseIndex;

        RemoteProcessorSupplier(
                String mapName, @Nonnull String clientXml
        ) {
            this.mapName = mapName;
            this.clientXml = clientXml;
        }

        @Override
        public void init(@Nonnull Context context) {

            hz3MapAdapter = Hz3Util.createMapAdapter(clientXml);

            HazelcastInstanceImpl hzInstance = (HazelcastInstanceImpl) context.hazelcastInstance();
            serializationService = hzInstance.node.getCompatibilitySerializationService();

            totalParallelism = context.totalParallelism();
            baseIndex = context.memberIndex() * context.localParallelism();
        }

        @Override
        public void close(Throwable error) {
            if (hz3MapAdapter != null) {
                hz3MapAdapter.shutdown();
            }
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            int remotePartitionCount = hz3MapAdapter.getPartitionCount();
            BooleanSupplier watcherInstance = hz3MapAdapter.createWatcher();

            return IntStream.range(0, count)
                    .mapToObj(i -> {
                        int[] partitionIds = Util.roundRobinPart(remotePartitionCount, totalParallelism, baseIndex + i);

                        return new ReadMapOrCacheP<>(
                                hz3MapAdapter.reader(mapName, toObjectFn()),
                                partitionIds,
                                watcherInstance
                        );
                    })
                    .collect(Collectors.toList());
        }

        private Function<Map.Entry<byte[], byte[]>, Object> toObjectFn() {
            return entry -> {
                HeapData keyData = new HeapData(entry.getKey());
                HeapData valueData = new HeapData(entry.getValue());
                return new LazyMapEntry<>(keyData, valueData, serializationService);
            };
        }
    }


}
