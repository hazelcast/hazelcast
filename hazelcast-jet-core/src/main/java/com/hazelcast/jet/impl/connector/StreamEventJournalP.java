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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.StaleSequenceException;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

/**
 * @see SourceProcessors#streamMapP(String, DistributedPredicate, DistributedFunction, boolean)
 */
public final class StreamEventJournalP<E, T> extends AbstractProcessor {

    private static final int MIN_FETCH_SIZE = 0;
    private static final int MAX_FETCH_SIZE = 100;

    private final EventJournalReader<E> eventJournalReader;
    private final List<Integer> partitions;
    private final Map<Integer, Long> offsetMap = new HashMap<>();
    private final SerializablePredicate<E> predicate;
    private final Projection<E, T> projection;
    private final boolean startFromLatestSequence;
    private Map<Integer, ICompletableFuture<ReadResultSet<T>>> futureMap = emptyMap();
    private Traverser<T> traverser;

    private StreamEventJournalP(EventJournalReader<E> eventJournalReader,
                                List<Integer> partitions,
                                Predicate<E> predicate,
                                Function<E, T> projectionFn,
                                boolean startFromLatestSequence) {
        this.eventJournalReader = eventJournalReader;
        this.partitions = partitions;
        this.predicate = predicate != null ? predicate::test : null;
        this.projection = createProjection(projectionFn);
        this.startFromLatestSequence = startFromLatestSequence;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        List<ICompletableFuture<EventJournalInitialSubscriberState>> futures = new ArrayList<>(partitions.size());
        for (Integer partitionId : partitions) {
            futures.add(eventJournalReader.subscribeToEventJournal(partitionId));
        }
        for (int i = 0; i < futures.size(); i++) {
            ICompletableFuture<EventJournalInitialSubscriberState> future = futures.get(i);
            offsetMap.put(partitions.get(i), getSequence(future.get()));
        }
    }

    private long getSequence(EventJournalInitialSubscriberState state) {
        if (startFromLatestSequence) {
            return state.getNewestSequence() == -1 ? 0 : state.getNewestSequence();
        }
        return state.getOldestSequence();
    }

    @Override
    public boolean complete() {
        pollIfNeeded();
        nextTraverserIfNeeded();
        if (traverser != null) {
            emitFromTraverser(traverser);
        }
        return false;
    }

    private void nextTraverserIfNeeded() {
        if (traverser != null) {
            return;
        }
        ReadResultSet<T> resultSet = null;
        Iterator<Entry<Integer, ICompletableFuture<ReadResultSet<T>>>> iterator = futureMap.entrySet().iterator();
        while (iterator.hasNext() && resultSet == null) {
            Entry<Integer, ICompletableFuture<ReadResultSet<T>>> entry = iterator.next();
            if (!entry.getValue().isDone()) {
                continue;
            }
            iterator.remove(); // remove the entry from futureMap
            try {
                resultSet = entry.getValue().get();
            } catch (ExecutionException e) {
                // this happens if the ringbuffer storing the journal overflows
                if (e.getCause() instanceof StaleSequenceException) {
                    long headSeq = ((StaleSequenceException) e.getCause()).getHeadSeq();
                    long oldOffset = offsetMap.put(entry.getKey(), headSeq);
                    getLogger().severe("Stale sequence, requested: " + oldOffset + ", currentHead: " + headSeq);
                    continue;
                }
                throw ExceptionUtil.rethrow(e);
            } catch (InterruptedException e) {
                throw ExceptionUtil.rethrow(e);
            }
            offsetMap.merge(entry.getKey(), (long) resultSet.readCount(), Long::sum);
            if (resultSet.size() == 0) {
                resultSet = null;
            }
        }
        if (resultSet != null) {
            traverser = traverseIterable(resultSet)
                .onFirstNull(() -> traverser = null);
        }
    }

    private void pollIfNeeded() {
        if (!futureMap.isEmpty()) {
            return;
        }
        futureMap = offsetMap.entrySet().stream().collect(toMap(Map.Entry::getKey,
                        e -> eventJournalReader.readFromEventJournal(e.getValue(),
                                MIN_FETCH_SIZE, MAX_FETCH_SIZE, e.getKey(), predicate, projection)
                ));
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private static <E, T> Projection<E, T> createProjection(Function<E, T> projectionFn) {
        if (projectionFn == null) {
            return null;
        }
        return new Projection<E, T>() {
            @Override public T transform(E input) {
                return projectionFn.apply(input);
            }
        };
    }

    private static <E, T> List<Processor> getProcessors(int count, List<Integer> ownedPartitions,
                                                        EventJournalReader<E> eventJournalReader,
                                                        Predicate<E> predicate,
                                                        Function<E, T> projection,
                                                        boolean startFromLatestSequence) {

        return processorToPartitions(count, ownedPartitions)
                .values().stream()
                .map(partitions -> !partitions.isEmpty()
                        ? new StreamEventJournalP<>(eventJournalReader, partitions, predicate,
                        projection, startFromLatestSequence)
                        : Processors.noopP().get()
                )
                .collect(toList());
    }

    interface SerializablePredicate<E> extends com.hazelcast.util.function.Predicate<E>, Serializable {
    }

    private static class ClusterMetaSupplier<E, T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig serializableConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final DistributedPredicate<E> predicate;
        private final DistributedFunction<E, T> projection;
        private final boolean startFromLatestSequence;

        private transient int remotePartitionCount;
        private transient Map<Address, List<Integer>> addrToPartitions;

        ClusterMetaSupplier(
                ClientConfig clientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                DistributedPredicate<E> predicate,
                DistributedFunction<E, T> projection,
                boolean startFromLatestSequence) {
            this.serializableConfig = clientConfig == null ? null : new SerializableClientConfig(clientConfig);
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.startFromLatestSequence = startFromLatestSequence;
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
                    serializableConfig, eventJournalReaderSupplier, predicate, projection, startFromLatestSequence);
        }

    }

    private static class ClusterProcessorSupplier<E, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<Integer> ownedPartitions;
        private final SerializableClientConfig serializableClientConfig;
        private final DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier;
        private final Predicate<E> predicate;
        private final Function<E, T> projection;
        private final boolean startFromLatestSequence;

        private transient HazelcastInstance client;
        private transient EventJournalReader<E> eventJournalReader;

        ClusterProcessorSupplier(
                List<Integer> ownedPartitions,
                SerializableClientConfig serializableClientConfig,
                DistributedFunction<HazelcastInstance, EventJournalReader<E>> eventJournalReaderSupplier,
                Predicate<E> predicate,
                Function<E, T> projection,
                boolean startFromLatestSequence) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.eventJournalReaderSupplier = eventJournalReaderSupplier;
            this.predicate = predicate;
            this.projection = projection;
            this.startFromLatestSequence = startFromLatestSequence;
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
            return getProcessors(count, ownedPartitions, eventJournalReader, predicate,
                    projection, startFromLatestSequence);
        }

    }

    public static <T> ProcessorMetaSupplier streamMap(String mapName,
                                                      DistributedPredicate<EventJournalMapEvent> predicate,
                                                      DistributedFunction<EventJournalMapEvent, T> projection,
                                                      boolean startFromLatestSequence) {
        return new ClusterMetaSupplier<>(null,
                instance -> (EventJournalReader<EventJournalMapEvent>) instance.getMap(mapName),
                predicate, projection, startFromLatestSequence);
    }

    public static <T> ProcessorMetaSupplier streamMap(String mapName,
                                                      ClientConfig clientConfig,
                                                      DistributedPredicate<EventJournalMapEvent> predicate,
                                                      DistributedFunction<EventJournalMapEvent, T> projection,
                                                      boolean startFromLatestSequence) {
        return new ClusterMetaSupplier<>(clientConfig,
                instance -> (EventJournalReader<EventJournalMapEvent>) instance.getMap(mapName),
                predicate, projection, startFromLatestSequence);
    }

    public static <T> ProcessorMetaSupplier streamCache(String cacheName,
                                                        DistributedPredicate<EventJournalCacheEvent> predicate,
                                                        DistributedFunction<EventJournalCacheEvent, T> projection,
                                                        boolean startFromLatestSequence) {
        return new ClusterMetaSupplier<>(null,
                instance -> (EventJournalReader<EventJournalCacheEvent>) instance.getCacheManager().getCache(cacheName),
                predicate, projection, startFromLatestSequence);
    }

    public static <T> ProcessorMetaSupplier streamCache(String cacheName,
                                                        ClientConfig clientConfig,
                                                        DistributedPredicate<EventJournalCacheEvent> predicate,
                                                        DistributedFunction<EventJournalCacheEvent, T> projection,
                                                        boolean startFromLatestSequence) {
        return new ClusterMetaSupplier<>(clientConfig,
                instance -> (EventJournalReader<EventJournalCacheEvent>) instance.getCacheManager().getCache(cacheName),
                predicate, projection, startFromLatestSequence);
    }
}
