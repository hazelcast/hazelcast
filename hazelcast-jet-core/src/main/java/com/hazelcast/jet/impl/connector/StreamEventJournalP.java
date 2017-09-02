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
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.journal.EventJournalReader;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.impl.util.Util.processorToPartitions;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

/**
 * @see Sources#streamMap(String, DistributedPredicate, DistributedFunction, boolean)
 */
public final class StreamEventJournalP<E, T> extends AbstractProcessor {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(100));
    private static final int MIN_FETCH_SIZE = 0;
    private static final int MAX_FETCH_SIZE = 100;

    private final EventJournalReader<E> eventJournalReader;
    private final List<Integer> partitions;
    private final Map<Integer, Long> offsetMap = new HashMap<>();
    private final SerializablePredicate<E> predicate;
    private final Projection<E, T> projection;
    private final boolean startFromLatestSequence;

    private CompletableFuture<Void> jobFuture;
    private boolean fetched;

    private StreamEventJournalP(EventJournalReader<E> eventJournalReader,
                                List<Integer> partitions,
                                Predicate<E> predicate,
                                Function<E, T> projectionF,
                                boolean startFromLatestSequence) {
        this.eventJournalReader = eventJournalReader;
        this.partitions = partitions;
        this.predicate = predicate != null ? predicate::test : null;
        this.projection = createProjection(projectionF);
        this.startFromLatestSequence = startFromLatestSequence;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        jobFuture = context.jobFuture();

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
        long idleCount = 0;
        while (!jobFuture.isDone()) {
            boolean fetched = poll();
            if (fetched) {
                idleCount = 0;
            } else {
                IDLER.idle(++idleCount);
            }
        }
        return true;
    }

    private boolean poll() {
        Map<Integer, ICompletableFuture<ReadResultSet<T>>> futureMap =
                offsetMap.entrySet().stream().collect(toMap(Map.Entry::getKey,
                        e -> eventJournalReader.readFromEventJournal(e.getValue(),
                                MIN_FETCH_SIZE, MAX_FETCH_SIZE, e.getKey(), predicate, projection)
                ));
        fetched = false;
        futureMap.forEach((key, future) -> {
                    try {
                        ReadResultSet<T> resultSet = future.get();
                        resultSet.forEach(this::emitEvent);
                        offsetMap.compute(key, (k, v) -> v + resultSet.readCount());
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof StaleSequenceException) {
                            long headSeq = ((StaleSequenceException) e.getCause()).getHeadSeq();
                            long oldOffset = offsetMap.put(key, headSeq);
                            getLogger().severe("Stale sequence, requested: " + oldOffset + ", currentHead: " + headSeq);
                            return;
                        }
                        throw ExceptionUtil.rethrow(e);
                    } catch (InterruptedException e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                }
        );
        return fetched;
    }

    private void emitEvent(T event) {
        emit(event);
        fetched = true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private static <E, T> Projection<E, T> createProjection(Function<E, T> projectionF) {
        if (projectionF == null) {
            return null;
        }
        return new Projection<E, T>() {
            @Override public T transform(E input) {
                return projectionF.apply(input);
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
                        : Processors.noop().get()
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
