/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.function.Predicate;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Base class for implementing data-structure specific event journal test where the journal is expiring.
 *
 * @param <EJ_TYPE> the type of the event journal event
 */
public abstract class AbstractEventJournalExpiringTest<EJ_TYPE> extends HazelcastTestSupport {
    private static final Random RANDOM = new Random();

    protected HazelcastInstance[] instances;

    private int partitionId;
    private TruePredicate<EJ_TYPE> TRUE_PREDICATE = new TruePredicate<EJ_TYPE>();
    private Projection<EJ_TYPE, EJ_TYPE> IDENTITY_PROJECTION = Projections.identity();

    private void init() {
        instances = createInstances();
        partitionId = 1;
        warmUpPartitions(instances);
    }

    @Override
    protected Config getConfig() {
        int defaultPartitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setMapName("default")
                .setCacheName("default")
                .setTimeToLiveSeconds(1)
                .setCapacity(500 * defaultPartitionCount);

        return smallInstanceConfig()
                .addEventJournalConfig(eventJournalConfig);
    }

    @Test
    public void skipsEventsWhenExpired() throws Throwable {
        init();

        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        String key = randomPartitionKey();
        final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();

        readFromJournal(context, exception, 0);

        for (int i = 0; i < 100000; i++) {
            context.dataAdapter.put(key, i);
            if (exception.get() != null) {
                throw exception.get();
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    private void readFromJournal(final EventJournalTestContext<String, Integer, EJ_TYPE> context,
                                 final AtomicReference<Throwable> exception,
                                 long seq) {
        readFromEventJournal(context.dataAdapter, seq, 128, partitionId, TRUE_PREDICATE,
                IDENTITY_PROJECTION).andThen(new ExecutionCallback<ReadResultSet<EJ_TYPE>>() {
            @Override
            public void onResponse(ReadResultSet<EJ_TYPE> response) {
                readFromJournal(context, exception, response.getNextSequenceToReadFrom());
                // ignore response
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            }

            @Override
            public void onFailure(Throwable t) {
                exception.set(t);
            }
        });
    }


    /**
     * Returns a random key belonging to the partition with ID {@link #partitionId}.
     */
    private String randomPartitionKey() {
        return generateKeyForPartition(instances[0], partitionId);
    }

    /**
     * Creates hazelcast instances used to run the tests.
     *
     * @return the array of hazelcast instances
     */
    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(2).newInstances(getConfig());
    }

    /**
     * Reads from the event journal a set of events.
     *
     * @param adapter       the adapter for a specific data structure
     * @param startSequence the sequence of the first item to read
     * @param maxSize       the maximum number of items to read
     * @param partitionId   the partition ID of the entries in the journal
     * @param predicate     the predicate which the events must pass to be included in the response.
     *                      May be {@code null} in which case all events pass the predicate
     * @param projection    the projection which is applied to the events before returning.
     *                      May be {@code null} in which case the event is returned without being
     *                      projected
     * @param <K>           the data structure entry key type
     * @param <V>the        data structure entry value type
     * @param <PROJ_TYPE>   the return type of the projection. It is equal to the journal event type
     *                      if the projection is {@code null} or it is the identity projection
     * @return the future with the filtered and projected journal items
     */
    private <K, V, PROJ_TYPE> ICompletableFuture<ReadResultSet<PROJ_TYPE>> readFromEventJournal(
            EventJournalDataStructureAdapter<K, V, EJ_TYPE> adapter,
            long startSequence,
            int maxSize,
            int partitionId,
            Predicate<EJ_TYPE> predicate,
            Projection<EJ_TYPE, PROJ_TYPE> projection) {
        return adapter.readFromEventJournal(startSequence, 1, maxSize, partitionId, predicate, projection);
    }

    /**
     * Returns a random hazelcast instance
     */
    protected HazelcastInstance getRandomInstance() {
        return instances[RANDOM.nextInt(instances.length)];
    }

    /**
     * Creates the data structure specific {@link EventJournalTestContext} used
     * by the event journal tests.
     *
     * @param <K> key type of the created {@link EventJournalTestContext}
     * @param <V> value type of the created {@link EventJournalTestContext}
     * @return a {@link EventJournalTestContext} used by the event journal tests
     */
    protected abstract <K, V> EventJournalTestContext<K, V, EJ_TYPE> createContext();
}
