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

package com.hazelcast.internal.util.scheduler;

import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SecondsBasedEntryTaskSchedulerStressTest {

    private static final int NUMBER_OF_THREADS = 4;
    private static final int NUMBER_OF_EVENTS_PER_THREAD = 10000;

    // scheduler is single threaded
    private TaskScheduler executorService;
    private ScheduledExecutorService scheduledExecutorService;

    @Before
    public void setUp() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        executorService = new DelegatingTaskScheduler(scheduledExecutorService, Executors.newSingleThreadExecutor());
    }

    @After
    public void tearDown() {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    public void test_forEach() {
        final EventCountingEntryProcessor processor = new EventCountingEntryProcessor();
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler
                = new SecondsBasedEntryTaskScheduler<>(executorService, processor, ScheduleType.FOR_EACH);

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final Thread thread = new Thread() {
                final Random random = new Random();

                @Override
                public void run() {
                    for (int j = 0; j < NUMBER_OF_EVENTS_PER_THREAD; j++) {
                        scheduler.schedule(getDelayMillis(), j, null);
                    }
                }

                private int getDelayMillis() {
                    return random.nextInt(5000) + 1;
                }
            };
            thread.start();
        }

        final long numberOfExpectedEvents = NUMBER_OF_THREADS * NUMBER_OF_EVENTS_PER_THREAD;
        assertTrueEventually(() -> {
            assertTrue(scheduler.isEmpty());
            assertEquals(numberOfExpectedEvents, processor.getNumberOfEvents());
        });
    }

    @Test
    public void test_postpone() {
        final EntryStoringProcessor processor = new EntryStoringProcessor();
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler
                = new SecondsBasedEntryTaskScheduler<>(executorService, processor, ScheduleType.POSTPONE);

        final int numberOfKeys = NUMBER_OF_THREADS;
        final Object[] locks = new Object[numberOfKeys];
        Arrays.fill(locks, new Object());

        final Map<Integer, Integer> latestValues = new ConcurrentHashMap<>();

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final Thread thread = new Thread() {
                final Random random = new Random();

                @Override
                public void run() {
                    for (int j = 0; j < NUMBER_OF_EVENTS_PER_THREAD; j++) {
                        int key = random.nextInt(numberOfKeys);

                        synchronized (locks[key]) {
                            if (scheduler.schedule(getDelayMillis(), key, j)) {
                                latestValues.put(key, j);
                            }
                        }
                    }
                }

                private int getDelayMillis() {
                    return random.nextInt(5000) + 1;
                }
            };
            thread.start();
        }

        assertTrueEventually(() -> {
            assertTrue(scheduler.isEmpty());
            assertEquals(latestValues.size(), processor.values.size());

            for (int key = 0; key < numberOfKeys; key++) {
                Integer expected = latestValues.get(key);
                Integer actual = processor.get(key);

                if (expected == null) {
                    assertNull(actual);
                } else {
                    assertEquals(expected, actual);
                }
            }
        });
    }

    private static class EventCountingEntryProcessor implements ScheduledEntryProcessor<Integer, Integer> {
        final AtomicInteger numberOfEvents = new AtomicInteger();

        @Override
        public void process(EntryTaskScheduler<Integer, Integer> scheduler,
                            Collection<ScheduledEntry<Integer, Integer>> entries) {

            numberOfEvents.addAndGet(entries.size());
        }

        long getNumberOfEvents() {
            return numberOfEvents.get();
        }
    }

    private static class EntryStoringProcessor implements ScheduledEntryProcessor<Integer, Integer> {
        final Map<Integer, Integer> values = new ConcurrentHashMap<>();

        @Override
        public void process(EntryTaskScheduler<Integer, Integer> scheduler,
                            Collection<ScheduledEntry<Integer, Integer>> entries) {

            // scheduler is single threaded
            for (ScheduledEntry<Integer, Integer> entry : entries) {
                values.put(entry.getKey(), entry.getValue());
            }
        }

        Integer get(int key) {
            return values.get(key);
        }
    }
}
