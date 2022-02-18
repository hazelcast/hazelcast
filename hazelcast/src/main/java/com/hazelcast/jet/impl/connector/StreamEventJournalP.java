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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.Partition;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.security.PermissionsUtil;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.MapPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static com.hazelcast.jet.impl.util.ImdgUtil.maybeUnwrapImdgFunction;
import static com.hazelcast.jet.impl.util.ImdgUtil.maybeUnwrapImdgPredicate;
import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
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
    private final EventJournalReader<? extends E> eventJournalReader;
    @Nonnull
    private final Predicate<? super E> predicate;
    @Nonnull
    private final Function<? super E, ? extends T> projection;
    @Nonnull
    private final JournalInitialPosition initialPos;
    @Nonnull
    private final int[] partitionIds;
    @Nonnull
    private final EventTimeMapper<? super T> eventTimeMapper;

    private final boolean isRemoteReader;

    // keep track of next offset to emit and read separately, as even when the
    // outbox is full we can still poll for new items.
    @Nonnull
    private final long[] emitOffsets;

    @Nonnull
    private final long[] readOffsets;

    private CompletableFuture<? extends ReadResultSet<? extends T>>[] readFutures;

    // currently processed resultSet, it's partitionId and iterating position
    @Nullable
    private ReadResultSet<? extends T> resultSet;
    private int currentPartitionIndex = -1;
    private int resultSetPosition;

    private Traverser<Entry<BroadcastKey<Integer>, long[]>> snapshotTraverser;
    private Traverser<Object> traverser = Traversers.empty();

    StreamEventJournalP(
            @Nonnull EventJournalReader<? extends E> eventJournalReader,
            @Nonnull List<Integer> assignedPartitions,
            @Nonnull PredicateEx<? super E> predicateFn,
            @Nonnull FunctionEx<? super E, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            boolean isRemoteReader,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.eventJournalReader = eventJournalReader;
        this.predicate = maybeUnwrapImdgPredicate(predicateFn);
        this.projection = maybeUnwrapImdgFunction(projectionFn);
        this.initialPos = initialPos;
        this.isRemoteReader = isRemoteReader;

        partitionIds = assignedPartitions.stream().mapToInt(Integer::intValue).toArray();
        emitOffsets = new long[partitionIds.length];
        readOffsets = new long[partitionIds.length];

        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);

        // Do not coalesce partition WMs because the number of partitions is far
        // larger than the number of consumers by default and it is not
        // configurable on a per journal basis. This creates excessive latency
        // when the number of events are relatively low and we have to wait for
        // all partitions to advance before advancing the watermark. The side
        // effect of not coalescing is that when the job is restarted and catching
        // up, there might be dropped late events due to several events being read
        // from one partition before the rest and the partition advancing ahead of
        // others. This might be changed in the future and/or made optional.
        assert partitionIds.length > 0 : "no partitions assigned";
        eventTimeMapper.addPartitions(1);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        @SuppressWarnings("unchecked")
        CompletableFuture<EventJournalInitialSubscriberState>[] futures = new CompletableFuture[partitionIds.length];
        Arrays.setAll(futures, i -> eventJournalReader.subscribeToEventJournal(partitionIds[i]));
        for (int i = 0; i < futures.length; i++) {
            emitOffsets[i] = readOffsets[i] = getSequence(futures[i].get());
        }

        if (!isRemoteReader) {
            // try to serde projection/predicate to fail fast if they aren't known to IMDG
            HazelcastInstanceImpl hzInstance = Util.getHazelcastInstanceImpl(context.hazelcastInstance());
            InternalSerializationService ss = hzInstance.getSerializationService();
            try {
                deserializeWithCustomClassLoader(ss, hzInstance.getClass().getClassLoader(), ss.toData(predicate));
                deserializeWithCustomClassLoader(ss, hzInstance.getClass().getClassLoader(), ss.toData(projection));
            } catch (HazelcastSerializationException e) {
                throw new JetException("The projection or predicate classes are not known to IMDG. It's not enough to " +
                        "add them to the job class path, they must be deployed using User code deployment: " + e, e);
            }
        }
    }

    @Override
    public boolean complete() {
        if (readFutures == null) {
            initialRead();
        }
        if (!emitFromTraverser(traverser)) {
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
            emitOffsets[currentPartitionIndex] = resultSet.getSequence(resultSetPosition) + 1;
            resultSetPosition++;
            if (event != null) {
                // Always use partition index of 0, treating all the partitions the
                // same for coalescing purposes.
                traverser = eventTimeMapper.flatMapEvent(event, 0, EventTimeMapper.NO_NATIVE_TIME);
                if (!emitFromTraverser(traverser)) {
                    return;
                }
            }
        }
        // we're done with current resultSet
        resultSetPosition = 0;
        resultSet = null;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            snapshotTraverser = traverseStream(IntStream
                    .range(0, partitionIds.length)
                    .mapToObj(pIdx -> entry(
                            broadcastKey(partitionIds[pIdx]),
                            // Always use partition index of 0, treating all the partitions the
                            // same for coalescing purposes.
                            new long[] {emitOffsets[pIdx], eventTimeMapper.getWatermark(0)})));
        }
        boolean done = emitFromTraverserToSnapshot(snapshotTraverser);
        if (done) {
            logFinest(getLogger(), "Saved snapshot. partitions=%s, offsets=%s, watermark=%d",
                    Arrays.toString(partitionIds), Arrays.toString(emitOffsets), eventTimeMapper.getWatermark(0));
            snapshotTraverser = null;
        }
        return done;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        @SuppressWarnings("unchecked")
        int partitionId = ((BroadcastKey<Integer>) key).key();
        int partitionIndex = arrayIndexOf(partitionId, partitionIds);
        long offset = ((long[]) value)[0];
        long wm = ((long[]) value)[1];
        if (partitionIndex >= 0) {
            readOffsets[partitionIndex] = offset;
            emitOffsets[partitionIndex] = offset;
            // Always use partition index of 0, treating all the partitions the
            // same for coalescing purposes.
            eventTimeMapper.restoreWatermark(0, wm);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        logFinest(getLogger(), "Restored snapshot. partitions=%s, offsets=%s",
                Arrays.toString(partitionIds), Arrays.toString(readOffsets));
        return true;
    }

    @SuppressWarnings("unchecked")
    private void initialRead() {
        readFutures = new CompletableFuture[partitionIds.length];
        for (int i = 0; i < readFutures.length; i++) {
            readFutures[i] = readFromJournal(partitionIds[i], readOffsets[i]);
        }
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        return initialPos == START_FROM_CURRENT ? state.getNewestSequence() + 1 : state.getOldestSequence();
    }

    private void tryGetNextResultSet() {
        while (resultSet == null && ++currentPartitionIndex < partitionIds.length) {
            CompletableFuture<? extends ReadResultSet<? extends T>> future = readFutures[currentPartitionIndex];
            if (!future.isDone()) {
                continue;
            }
            resultSet = toResultSet(future);
            int partitionId = partitionIds[currentPartitionIndex];
            if (resultSet != null) {
                assert resultSet.size() > 0 : "empty resultSet";
                long prevSequence = readOffsets[currentPartitionIndex];
                long lostCount = resultSet.getNextSequenceToReadFrom() - resultSet.readCount() - prevSequence;
                if (lostCount > 0) {
                    getLogger().warning(lostCount + " events lost for partition "
                            + partitionId + " due to journal overflow when reading from event journal."
                            + " Increase journal size to avoid this error. nextSequenceToReadFrom="
                            + resultSet.getNextSequenceToReadFrom() + ", readCount=" + resultSet.readCount()
                            + ", prevSeq=" + prevSequence);
                }
                readOffsets[currentPartitionIndex] = resultSet.getNextSequenceToReadFrom();
            }
            // make another read on the same partition
            readFutures[currentPartitionIndex] = readFromJournal(partitionId, readOffsets[currentPartitionIndex]);
        }

        if (currentPartitionIndex == partitionIds.length) {
            currentPartitionIndex = -1;
            traverser = eventTimeMapper.flatMapIdle();
        }
    }

    private ReadResultSet<? extends T> toResultSet(CompletableFuture<? extends ReadResultSet<? extends T>> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable ex = peel(e);
            if (ex instanceof HazelcastInstanceNotActiveException && !isRemoteReader) {
                // This exception can be safely ignored - it means the instance was shutting down,
                // so we shouldn't unnecessarily throw an exception here.
                return null;
            } else if (ex instanceof HazelcastSerializationException) {
                throw new JetException("Serialization error when reading the journal: are the key, value, " +
                        "predicate and projection classes visible to IMDG? You need to use User Code " +
                        "Deployment, adding the classes to JetConfig isn't enough", e);
            } else {
                throw rethrow(ex);
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private CompletableFuture<? extends ReadResultSet<? extends T>> readFromJournal(int partition, long offset) {
        return eventJournalReader.readFromEventJournal(offset, 1, MAX_FETCH_SIZE, partition, predicate, projection)
                                 .toCompletableFuture();
    }

    private static class ClusterMetaSupplier<E, T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String clientXml;
        private final FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>>
                eventJournalReaderSupplier;
        private final PredicateEx<? super E> predicate;
        private final FunctionEx<? super E, ? extends T> projection;
        private final JournalInitialPosition initialPos;
        private final EventTimePolicy<? super T> eventTimePolicy;
        private final SupplierEx<Permission> permissionFn;

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                @Nullable String clientXml,
                @Nonnull FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>>
                        eventJournalReaderSupplier,
                @Nonnull PredicateEx<? super E> predicate,
                @Nonnull FunctionEx<? super E, ? extends T> projection,
                @Nonnull JournalInitialPosition initialPos,
                @Nonnull EventTimePolicy<? super T> eventTimePolicy,
                @Nonnull SupplierEx<Permission> permissionFn
                ) {
            this.clientXml = clientXml;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
            this.eventTimePolicy = eventTimePolicy;
            this.permissionFn = permissionFn;
        }

        @Override
        public int preferredLocalParallelism() {
            return clientXml != null ? 1 : 2;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (clientXml != null) {
                initRemote();
            } else {
                PermissionsUtil.checkPermission(eventJournalReaderSupplier, context);
                initLocal(context.hazelcastInstance().getPartitionService().getPartitions());
            }
        }

        private void initRemote() {
            HazelcastInstance client = newHazelcastClient(asClientConfig(clientXml));
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

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (addrToPartitions == null) {
                // assign each remote partition to a member
                addrToPartitions = range(0, remotePartitionCount)
                        .boxed()
                        .collect(groupingBy(partition -> addresses.get(partition % addresses.size())));
            }

            return address -> new ClusterProcessorSupplier<>(addrToPartitions.get(address),
                    clientXml, eventJournalReaderSupplier, predicate, projection, initialPos,
                    eventTimePolicy);
        }

        @Override
        public Permission getRequiredPermission() {
            if (clientXml != null) {
                return null;
            }
            return permissionFn.get();
        }
    }

    private static class ClusterProcessorSupplier<E, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        @Nonnull
        private final List<Integer> ownedPartitions;
        @Nullable
        private final String clientXml;
        @Nonnull
        private final FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>>
                eventJournalReaderSupplier;
        @Nonnull
        private final PredicateEx<? super E> predicate;
        @Nonnull
        private final FunctionEx<? super E, ? extends T> projection;
        @Nonnull
        private final JournalInitialPosition initialPos;
        @Nonnull
        private final EventTimePolicy<? super T> eventTimePolicy;

        private transient HazelcastInstance client;
        private transient EventJournalReader<E> eventJournalReader;

        ClusterProcessorSupplier(
                @Nonnull List<Integer> ownedPartitions,
                @Nullable String clientXml,
                @Nonnull FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>>
                        eventJournalReaderSupplier,
                @Nonnull PredicateEx<? super E> predicate,
                @Nonnull FunctionEx<? super E, ? extends T> projection,
                @Nonnull JournalInitialPosition initialPos,
                @Nonnull EventTimePolicy<? super T> eventTimePolicy
        ) {
            this.ownedPartitions = ownedPartitions;
            this.clientXml = clientXml;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.initialPos = initialPos;
            this.eventTimePolicy = eventTimePolicy;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance = context.hazelcastInstance();
            if (clientXml != null) {
                client = newHazelcastClient(asClientConfig(clientXml));
                instance = client;
            }
            eventJournalReader = eventJournalReaderSupplier.apply(instance);
        }

        @Override
        public void close(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            return distributeObjects(count, ownedPartitions)
                    .values().stream()
                    .map(this::processorForPartitions)
                    .collect(toList());
        }

        private Processor processorForPartitions(List<Integer> partitions) {
            return partitions.isEmpty()
                    ? Processors.noopP().get()
                    : new StreamEventJournalP<>(eventJournalReader, partitions, predicate, projection,
                    initialPos, client != null, eventTimePolicy);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamMapSupplier(
            @Nonnull String mapName,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicate,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new ClusterMetaSupplier<>(null,
                SecuredFunctions.mapEventJournalReaderFn(mapName),
                predicate, projection, initialPos, eventTimePolicy,
                () -> new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull String clientXml,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicate,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new ClusterMetaSupplier<>(clientXml,
                SecuredFunctions.mapEventJournalReaderFn(mapName),
                predicate, projection, initialPos, eventTimePolicy,
                () -> new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamCacheSupplier(
            @Nonnull String cacheName,
            @Nonnull PredicateEx<? super EventJournalCacheEvent<K, V>> predicate,
            @Nonnull FunctionEx<? super EventJournalCacheEvent<K, V>, ? extends T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new ClusterMetaSupplier<>(null,
                SecuredFunctions.cacheEventJournalReaderFn(cacheName),
                predicate, projection, initialPos, eventTimePolicy,
                () -> new CachePermission(cacheName, ACTION_CREATE, ACTION_READ));
    }

    @SuppressWarnings("unchecked")
    public static <K, V, T> ProcessorMetaSupplier streamRemoteCacheSupplier(
            @Nonnull String cacheName,
            @Nonnull String clientXml,
            @Nonnull PredicateEx<? super EventJournalCacheEvent<K, V>> predicate,
            @Nonnull FunctionEx<? super EventJournalCacheEvent<K, V>, ? extends T> projection,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new ClusterMetaSupplier<>(clientXml,
                SecuredFunctions.cacheEventJournalReaderFn(cacheName),
                predicate, projection, initialPos, eventTimePolicy,
                () -> new CachePermission(cacheName, ACTION_CREATE, ACTION_READ));
    }
}
