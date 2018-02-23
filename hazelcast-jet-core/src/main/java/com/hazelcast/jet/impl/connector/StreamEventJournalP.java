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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.jet.JournalInitialPosition;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * @see SourceProcessors#streamMapP
 */
public final class StreamEventJournalP<E, T> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 128;

    @Nonnull
    private final EventJournalReader<E> eventJournalReader;
    @Nonnull
    private final Predicate<E> predicate;
    @Nonnull
    private final Projection<E, T> projection;
    @Nonnull
    private final JournalInitialPosition initialPos;
    @Nonnull
    private final int[] partitionIds;
    @Nonnull
    private final WatermarkSourceUtil<T> watermarkSourceUtil;

    private final boolean isRemoteReader;

    // keep track of next offset to emit and read separately, as even when the
    // outbox is full we can still poll for new items.
    @Nonnull
    private final long[] emitOffsets;
    @Nonnull
    private final long[] readOffsets;

    private ICompletableFuture<ReadResultSet<T>>[] readFutures;

    // currently processed resultSet, it's partitionId and iterating position
    @Nullable
    private ReadResultSet<T> resultSet;
    private int currentPartitionIndex = -1;
    private int resultSetPosition;

    private Traverser<Entry<BroadcastKey<Integer>, long[]>> snapshotTraverser;
    private Traverser<Object> traverser = Traversers.empty();

    StreamEventJournalP(
            @Nonnull EventJournalReader<E> eventJournalReader,
            @Nonnull List<Integer> assignedPartitions,
            @Nonnull DistributedPredicate<E> predicateFn,
            @Nonnull DistributedFunction<E, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            boolean isRemoteReader,
            @Nonnull WatermarkGenerationParams<? super T> wmGenParams
    ) {
        this.eventJournalReader = eventJournalReader;
        this.predicate = (Serializable & Predicate<E>) predicateFn::test;
        this.projection = toProjection(projectionFn);
        this.initialPos = initialPos;
        this.isRemoteReader = isRemoteReader;

        partitionIds = assignedPartitions.stream().mapToInt(Integer::intValue).toArray();
        emitOffsets = new long[partitionIds.length];
        readOffsets = new long[partitionIds.length];

        watermarkSourceUtil = new WatermarkSourceUtil<>(wmGenParams);

        // Do not coalesce partition WMs because the number of partitions
        // is far larger than the number of consumers by default and it is
        // not configurable on a per journal basis. This creates
        // excessive latency when the number of events are relatively low
        // where we have to wait for all partitions to advance before
        // advancing the watermark.
        // The side effect of not coalescing is that when the job is
        // restarted and catching up, there might be dropped late events due to
        // several events being read from one partition before the rest and
        // the partition advancing ahead of others.
        // This might be changed in the future and/or made optional.
        watermarkSourceUtil.increasePartitionCount(1);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        ICompletableFuture<EventJournalInitialSubscriberState>[] futures = new ICompletableFuture[partitionIds.length];
        Arrays.setAll(futures, i -> eventJournalReader.subscribeToEventJournal(partitionIds[i]));
        for (int i = 0; i < futures.length; i++) {
            emitOffsets[i] = readOffsets[i] = getSequence(futures[i].get());
        }
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        if (!emitFromTraverser(traverser, this::afterEmit)) {
            return false;
        }
        do {
            tryGetNextResultSet();
            if (resultSet == null) {
                break;
            }
            emitResultSet();
        } while (resultSet == null);
        return false;
    }

    private void emitResultSet() {
        assert resultSet != null : "null resultSet";
        while (resultSetPosition < resultSet.size()) {
            T event = resultSet.get(resultSetPosition);
            if (event != null) {
                // Always use partition index of 0, treating all the partitions the
                // same for coalescing purposes.
                traverser = watermarkSourceUtil.handleEvent(event, 0);
                if (!emitFromTraverser(traverser, this::afterEmit)) {
                    return;
                }
            } else {
                afterEmit(null);
            }
        }
        // we're done with current resultSet
        resultSetPosition = 0;
        resultSet = null;
    }

    private void afterEmit(@Nullable Object object) {
        if (object instanceof Watermark) {
            return;
        }
        assert resultSet != null;
        emitOffsets[currentPartitionIndex] = resultSet.getSequence(resultSetPosition) + 1;
        resultSetPosition++;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseStream(IntStream.range(0, partitionIds.length)
                    .mapToObj(pIdx -> entry(
                            broadcastKey(partitionIds[pIdx]),
                            // Always use partition index of 0, treating all the partitions the
                            // same for coalescing purposes.
                            new long[] {emitOffsets[pIdx], watermarkSourceUtil.getWatermark(0)})));
        }
        boolean done = emitFromTraverserToSnapshot(snapshotTraverser);
        if (done) {
            logFinest(getLogger(), "Saved snapshot. Offsets: %s", emitOffsets);
            snapshotTraverser = null;
        }
        return done;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        int partitionId = ((BroadcastKey<Integer>) key).key();
        int partitionIndex = arrayIndexOf(partitionId, partitionIds);
        long offset = ((long[]) value)[0];
        long wm = ((long[]) value)[1];
        if (partitionIndex >= 0) {
            readOffsets[partitionIndex] = offset;
            emitOffsets[partitionIndex] = offset;
            // Always use partition index of 0, treating all the partitions the
            // same for coalescing purposes.
            watermarkSourceUtil.restoreWatermark(0, wm);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        logFinest(getLogger(), "Restored snapshot. Offsets: %s", readOffsets);
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void initialRead() {
        readFutures = new ICompletableFuture[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = readFromJournal(partitionIds[i], readOffsets[i]);
        }
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        return initialPos == START_FROM_CURRENT ? state.getNewestSequence() + 1 : state.getOldestSequence();
    }

    private void tryGetNextResultSet() {
        while (resultSet == null && ++currentPartitionIndex < partitionIds.length) {
            ICompletableFuture<ReadResultSet<T>> future = readFutures[currentPartitionIndex];
            if (!future.isDone()) {
                continue;
            }
            resultSet = toResultSet(currentPartitionIndex, future);
            if (resultSet != null) {
                assert resultSet.size() > 0 : "empty resultSet";
                readOffsets[currentPartitionIndex] += resultSet.readCount();
            }
            // make another read on the same partition
            readFutures[currentPartitionIndex] = readFromJournal(partitionIds[currentPartitionIndex],
                    readOffsets[currentPartitionIndex]);
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            traverser = watermarkSourceUtil.handleNoEvent();
        }
    }

    private ReadResultSet<T> toResultSet(int partitionIdx, ICompletableFuture<ReadResultSet<T>> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastInstanceNotActiveException && !isRemoteReader) {
                // This exception can be safely ignored - it means the instance was shutting down,
                // so we shouldn't unnecessarily throw an exception here.
                return null;
            } else if (ex instanceof StaleSequenceException) {
                long headSeq = ((StaleSequenceException) e.getCause()).getHeadSeq();
                // move both read and emitted offsets to the new head
                long oldOffset = readOffsets[partitionIdx];
                readOffsets[partitionIdx] = emitOffsets[partitionIdx] = headSeq;
                getLogger().warning("Events lost for partition " + partitionIds[partitionIdx]
                        + " due to journal overflow when reading from event journal. Increase journal size to " +
                        "avoid this error. Requested was: " + oldOffset + ", current head is: " + headSeq);
                return null;
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private ICompletableFuture<ReadResultSet<T>> readFromJournal(int partition, long offset) {
        logFinest(getLogger(), "Reading from partition %d and offset %d", partition, offset);
        return eventJournalReader.readFromEventJournal(offset,
                1, MAX_FETCH_SIZE, partition, predicate, projection);
    }

    private static <E, T> Projection<E, T> toProjection(Function<E, T> projectionFn) {
        return new Projection<E, T>() {
            @Override
            public T transform(E input) {
                return projectionFn.apply(input);
            }
        };
    }

    private static class ClusterMetaSupplier<E, T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig serializableConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final DistributedPredicate<E> predicate;
        private final DistributedFunction<E, T> projection;
        private final JournalInitialPosition initialPos;
        private final WatermarkGenerationParams<? super T> wmGenParams;

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                @Nullable ClientConfig clientConfig,
                @Nonnull DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                @Nonnull DistributedPredicate<E> predicate,
                @Nonnull DistributedFunction<E, T> projection,
                @Nonnull JournalInitialPosition initialPos,
                @Nonnull WatermarkGenerationParams<? super T> wmGenParams
        ) {
            this.serializableConfig = clientConfig == null ? null : new SerializableClientConfig(clientConfig);
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
            this.wmGenParams = wmGenParams;
        }

        @Override
        public int preferredLocalParallelism() {
            return serializableConfig != null ? 1 : 2;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (serializableConfig != null) {
                initRemote();
            } else {
                initLocal(context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions());
            }
        }

        private void initRemote() {
            HazelcastInstance client = newHazelcastClient(serializableConfig.asClientConfig());
            try {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
                remotePartitionCount = clientProxy.client.getClientPartitionService().getPartitionCount();
            } finally {
                client.shutdown();
            }
        }

        private void initLocal(Set<Partition> partitions) {
            addrToPartitions = partitions.stream()
                                         .collect(groupingBy(p -> p.getOwner().getAddress(),
                                                 mapping(Partition::getPartitionId, toList())));
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (addrToPartitions == null) {
                // assign each remote partition to a member
                addrToPartitions = range(0, remotePartitionCount)
                        .boxed()
                        .collect(groupingBy(partition -> addresses.get(partition % addresses.size())));
            }

            return address -> new ClusterProcessorSupplier<>(addrToPartitions.get(address),
                    serializableConfig, eventJournalReaderSupplier, predicate, projection, initialPos,
                    wmGenParams);
        }

    }

    private static class ClusterProcessorSupplier<E, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        @Nonnull
        private final List<Integer> ownedPartitions;
        @Nullable
        private final SerializableClientConfig serializableClientConfig;
        @Nonnull
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        @Nonnull
        private final DistributedPredicate<E> predicate;
        @Nonnull
        private final DistributedFunction<E, T> projection;
        @Nonnull
        private final JournalInitialPosition initialPos;
        @Nonnull
        private final WatermarkGenerationParams<? super T> wmGenParams;

        private transient HazelcastInstance client;
        private transient EventJournalReader<E> eventJournalReader;

        ClusterProcessorSupplier(
                @Nonnull List<Integer> ownedPartitions,
                @Nullable SerializableClientConfig serializableClientConfig,
                @Nonnull DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                @Nonnull DistributedPredicate<E> predicate,
                @Nonnull DistributedFunction<E, T> projection,
                @Nonnull JournalInitialPosition initialPos,
                @Nonnull WatermarkGenerationParams<? super T> wmGenParams
        ) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
            this.wmGenParams = wmGenParams;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance = context.jetInstance().getHazelcastInstance();
            if (serializableClientConfig != null) {
                client = newHazelcastClient(serializableClientConfig.asClientConfig());
                instance = client;
            }
            eventJournalReader = eventJournalReaderSupplier.apply(instance);
        }

        @Override
        public void complete(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return processorToPartitions(count, ownedPartitions)
                    .values().stream()
                    .map(this::processorForPartitions)
                    .collect(toList());
        }

        private Processor processorForPartitions(List<Integer> partitions) {
            return partitions.isEmpty()
                    ? Processors.noopP().get()
                    : new StreamEventJournalP<>(eventJournalReader, partitions, predicate, projection,
                    initialPos, client != null, wmGenParams);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            WatermarkGenerationParams<? super T> wmGenParams) {
        return new ClusterMetaSupplier<>(null,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, initialPos, wmGenParams);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<T> wmGenParams) {
        return new ClusterMetaSupplier<>(clientConfig,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, initialPos, wmGenParams);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<T> wmGenParams) {
        return new ClusterMetaSupplier<>(null,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, initialPos, wmGenParams);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<T> wmGenParams) {
        return new ClusterMetaSupplier<>(clientConfig,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, initialPos, wmGenParams);
    }
}
