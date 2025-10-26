/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.spi.properties.ClusterProperty.EVENT_JOURNAL_CLEANUP_THRESHOLD;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Base class for implementing data-structure specific event journal test where the journal is expiring.
 *
 * @param <EJ_TYPE> the type of the event journal event
 */
@RunWith(HazelcastSerialClassRunner.class)
public abstract class AbstractEventJournalExpiringTest<EJ_TYPE> extends HazelcastTestSupport {
    private static final Random RANDOM = new Random();
    private static final int JOURNAL_CAPACITY_PER_PARTITION = 1_000;
    private static final int PARTITION_COUNT = 11;
    private static final float TEST_MAP_JOURNAL_CLEANUP_THRESHOLD = 0.5f;
    private static final int JOURNAL_TTL_SECOND = 1;

    protected HazelcastInstance[] instances;

    private int partitionId;
    private final TruePredicate<EJ_TYPE> TRUE_PREDICATE = new TruePredicate<>();
    private final Function<EJ_TYPE, EJ_TYPE> IDENTITY_FUNCTION = new IdentityFunction<>();

    private void init() {
        instances = createInstances();
        partitionId = 1;
        warmUpPartitions(instances);
    }

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.setProperty(EVENT_JOURNAL_CLEANUP_THRESHOLD.getName(), String.valueOf(TEST_MAP_JOURNAL_CLEANUP_THRESHOLD));
        return config;
    }

    protected final EventJournalConfig getEventJournalConfig() {
        return new EventJournalConfig()
                .setEnabled(true)
                .setTimeToLiveSeconds(JOURNAL_TTL_SECOND)
                .setCapacity(JOURNAL_CAPACITY_PER_PARTITION * PARTITION_COUNT);
    }

    @Test
    public void skipsEventsWhenExpired() throws Throwable {
        init();

        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        String key = randomPartitionKey();
        final AtomicReference<Throwable> exception = new AtomicReference<>();

        readFromJournal(context, exception, 0);

        for (int i = 0; i < 100000; i++) {
            context.dataAdapter.put(key, i);
            if (exception.get() != null) {
                throw exception.get();
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    @Test
    public void backupCleanupWhenExpired() throws Throwable {
        init();
        final EventJournalTestContext<String, String, EJ_TYPE> context = createContext();

        String key = generateKeyForPartition(instances[0], partitionId);
        String val = "value";
        context.dataAdapter.put(key, val);

        final AtomicReference<Throwable> exception = new AtomicReference<>();
        readFromJournal(context, exception, 0);

        var cleanupThresholdStart = (int) (JOURNAL_CAPACITY_PER_PARTITION * (1 - TEST_MAP_JOURNAL_CLEANUP_THRESHOLD));
        for (int i = 0; i < cleanupThresholdStart + 1; i++) {
            context.dataAdapter.put(key, val);
            if (exception.get() != null) {
                throw exception.get();
            }
        }

        // wait values expire and trigger cleanup by adding one more item
        sleepSeconds(JOURNAL_TTL_SECOND * 2);
        context.dataAdapter.put(key, val);

        // one of these two journal is the owner, the other is the backup
        var journalNode1 = getRingBufferContainer(getName(), partitionId, instances[0]);
        var journalNode2 = getRingBufferContainer(getName(), partitionId, instances[1]);

        // value added as trigger of cleanup + 1 remaining value
        var expectedCapacity = JOURNAL_CAPACITY_PER_PARTITION - 1;
        assertThat(journalNode1.remainingCapacity()).isGreaterThanOrEqualTo(expectedCapacity);
        assertThat(journalNode2.remainingCapacity()).isGreaterThanOrEqualTo(expectedCapacity);
    }

    private <K, V> void readFromJournal(final EventJournalTestContext<K, V, EJ_TYPE> context,
                                 final AtomicReference<Throwable> exception,
                                 long seq) {
        readFromEventJournal(context.dataAdapter, seq, 128, partitionId, TRUE_PREDICATE,
                IDENTITY_FUNCTION).whenCompleteAsync((response, t) -> {
            if (t == null) {
                readFromJournal(context, exception, response.getNextSequenceToReadFrom());
                // ignore response
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            } else {
                exception.set(t);
            }
        });
    }

    protected abstract <K, V> RingbufferContainer<K, V> getRingBufferContainer(String name, int partitionId, HazelcastInstance instance);

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
     * @param <V>           the data structure entry value type
     * @param <PROJ_TYPE>   the return type of the projection. It is equal to the journal event type
     *                      if the projection is {@code null} or it is the identity projection
     * @return the future with the filtered and projected journal items
     */
    private <K, V, PROJ_TYPE> CompletionStage<ReadResultSet<PROJ_TYPE>> readFromEventJournal(
            EventJournalDataStructureAdapter<K, V, EJ_TYPE> adapter,
            long startSequence,
            int maxSize,
            int partitionId,
            Predicate<EJ_TYPE> predicate,
            Function<EJ_TYPE, PROJ_TYPE> projection) {
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

    protected abstract String getName();
}
