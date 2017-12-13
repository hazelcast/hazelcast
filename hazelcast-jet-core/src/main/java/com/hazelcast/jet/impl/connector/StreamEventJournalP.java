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
import com.hazelcast.jet.JournalInitialPosition;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.util.function.Predicate;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

/**
 * @see SourceProcessors#streamMapP(String, DistributedPredicate, DistributedFunction, JournalInitialPosition)
 */
public final class StreamEventJournalP<E, T> extends AbstractProcessor {

    private static final int MAX_FETCH_SIZE = 128;

    private final EventJournalReader<E> eventJournalReader;
    private final Set<Integer> assignedPartitions;
    private final Predicate<E> predicate;
    private final Projection<E, T> projection;
    private final JournalInitialPosition initialPos;
    private final boolean isRemoteReader;

    // keep track of next offset to emit and read separately, as even when the
    // outbox is full we can still poll for new items.
    private final Map<Integer, Long> emitOffsets = new HashMap<>();
    private final Map<Integer, Long> readOffsets = new HashMap<>();

    private final Map<Integer, ICompletableFuture<ReadResultSet<T>>> readFutures = new HashMap<>();

    private Traverser<T> eventTraverser;
    private Traverser<Entry<BroadcastKey<Integer>, Long>> snapshotTraverser;

    // keep track of pendingItem's offset and partition
    private long pendingItemOffset;
    private int pendingItemPartition;

    // callback which will update the currently pending offset only after the item is emitted
    private Consumer<T> updateOffsetFn = e -> emitOffsets.put(pendingItemPartition, pendingItemOffset + 1);
    private Iterator<Entry<Integer, ICompletableFuture<ReadResultSet<T>>>> iterator;

    StreamEventJournalP(@Nonnull EventJournalReader<E> eventJournalReader,
                        @Nonnull List<Integer> assignedPartitions,
                        @Nonnull DistributedPredicate<E> predicateFn,
                        @Nonnull DistributedFunction<E, T> projectionFn,
                        @Nonnull JournalInitialPosition initialPos,
                        boolean isRemoteReader) {
        this.eventJournalReader = eventJournalReader;
        this.assignedPartitions = new HashSet<>(assignedPartitions);
        this.predicate = (Serializable & Predicate<E>) predicateFn::test;
        this.projection = toProjection(projectionFn);
        this.initialPos = initialPos;
        this.isRemoteReader = isRemoteReader;
    }

    @Override
    protected void init(@Nonnull Context context) {
        Map<Integer, ICompletableFuture<EventJournalInitialSubscriberState>> futures = assignedPartitions.stream()
            .map(partition -> entry(partition, eventJournalReader.subscribeToEventJournal(partition)))
            .collect(toMap(Entry::getKey, Entry::getValue));
        futures.forEach((partition, future) -> uncheckRun(() -> readOffsets.put(partition, getSequence(future.get()))));
        emitOffsets.putAll(readOffsets);
    }

    @Override
    public boolean complete() {
        if (readFutures.isEmpty()) {
            initialRead();
        }
        if (eventTraverser == null) {
            Traverser<T> t = nextTraverser();
            if (t != null) {
                eventTraverser = t.onFirstNull(() -> eventTraverser = null);
            }
        }

        if (eventTraverser != null) {
            emitFromTraverser(eventTraverser, updateOffsetFn);
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(emitOffsets.entrySet())
                    .map(e -> entry(broadcastKey(e.getKey()), e.getValue()))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        boolean done = emitFromTraverserToSnapshot(snapshotTraverser);
        if (done) {
            logFinest(getLogger(), "Saved snapshot. Offsets: %s", emitOffsets);
        }
        return done;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        int partition = ((BroadcastKey<Integer>) key).key();
        long offset = (Long) value;
        if (assignedPartitions.contains(partition)) {
            readOffsets.put(partition, offset);
            emitOffsets.put(partition, offset);
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
        readOffsets.forEach((partition, offset) ->
                readFutures.put(partition, readFromJournal(partition, offset)));
        iterator = readFutures.entrySet().iterator();
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        return initialPos == START_FROM_CURRENT ? state.getNewestSequence() + 1 : state.getOldestSequence();
    }

    private Traverser<T> nextTraverser() {
        ReadResultSet<T> resultSet = nextResultSet();
        if (resultSet == null) {
            return null;
        }
        Traverser<T> traverser = traverseIterable(resultSet);
        return peekIndex(traverser, i -> pendingItemOffset = resultSet.getSequence(i));
    }

    private ReadResultSet<T> nextResultSet() {
        while (iterator.hasNext()) {
            Entry<Integer, ICompletableFuture<ReadResultSet<T>>> entry = iterator.next();
            int partition = entry.getKey();
            if (!entry.getValue().isDone()) {
                continue;
            }
            ReadResultSet<T> resultSet = toResultSet(partition, entry.getValue());
            if (resultSet == null || resultSet.size() == 0) {
                // we got stale sequence or empty response, make another read
                entry.setValue(readFromJournal(partition, readOffsets.get(partition)));
                continue;
            }
            pendingItemPartition = partition;
            long newOffset = readOffsets.merge(partition, (long) resultSet.readCount(), Long::sum);
            // make another read on the same partition
            entry.setValue(readFromJournal(partition, newOffset));
            return resultSet;
        }
        iterator = readFutures.entrySet().iterator();
        return null;
    }

    private ReadResultSet<T> toResultSet(int partition, ICompletableFuture<ReadResultSet<T>> future) {
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
                long oldOffset = readOffsets.put(partition, headSeq);
                emitOffsets.put(partition, headSeq);
                getLogger().warning("Events lost for partition " + partition + " due to journal overflow " +
                        "when reading from event journal. Increase journal size to avoid this error. " +
                        "Requested was: " + oldOffset + ", current head is: " + headSeq);
                return null;
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private ICompletableFuture<ReadResultSet<T>> readFromJournal(int partition, long offset) {
        logFine(getLogger(), "Reading from partition %s and offset %s", partition, offset);
        return eventJournalReader.readFromEventJournal(offset,
                1, MAX_FETCH_SIZE, partition, predicate, projection);
    }

    /**
     * Returns a new traverser, that will return same items as supplied {@code
     * traverser} and before each item is returned a zero-based index is sent
     * to the {@code action}.
     */
    private static <T> Traverser<T> peekIndex(Traverser<T> traverser, IntConsumer action) {
        int[] count = {0};
        return () -> {
            T t = traverser.next();
            if (t != null) {
                action.accept(count[0]++);
            }
            return t;
        };
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

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                ClientConfig clientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                JournalInitialPosition initialPos
        ) {
            this.serializableConfig = clientConfig == null ? null : new SerializableClientConfig(clientConfig);
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
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
                    serializableConfig, eventJournalReaderSupplier, predicate, projection, initialPos);
        }

    }

    private static class ClusterProcessorSupplier<E, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<Integer> ownedPartitions;
        private final SerializableClientConfig serializableClientConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final DistributedPredicate<E> predicate;
        private final DistributedFunction<E, T> projection;
        private final JournalInitialPosition initialPos;

        private transient HazelcastInstance client;
        private transient EventJournalReader<E> eventJournalReader;

        ClusterProcessorSupplier(
                List<Integer> ownedPartitions,
                SerializableClientConfig serializableClientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                JournalInitialPosition initialPos) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
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
                    initialPos, client != null);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return new ClusterMetaSupplier<>(null,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, initialPos);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return new ClusterMetaSupplier<>(clientConfig,
                instance -> (EventJournalReader<EventJournalMapEvent<K, V>>) instance.getMap(mapName),
                predicate, projection, initialPos);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return new ClusterMetaSupplier<>(null,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, initialPos);
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return new ClusterMetaSupplier<>(clientConfig,
                inst -> (EventJournalReader<EventJournalCacheEvent<K, V>>) inst.getCacheManager().getCache(cacheName),
                predicate, projection, initialPos);
    }
}
