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

package com.hazelcast.internal.management;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.internal.management.dto.MCEventDTO;
import com.hazelcast.internal.metrics.managementcenter.ConcurrentArrayRingbuffer;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static com.hazelcast.internal.management.ManagementCenterService.MCEventStore.MC_EVENTS_WINDOW_NANOS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MCEventStoreTest {

    private static class FakeClock
            implements LongSupplier {

        long now = 42;

        @Override
        public long getAsLong() {
            return now;
        }
    }

    static final UUID MC_1_UUID = UUID.fromString("6c31a5a6-91b2-4a6c-bc0d-3592c5a3d29f");

    static final UUID MC_2_UUID = UUID.fromString("31a7baf0-11a2-4cad-8943-fa5d8a48e62b");

    static final UUID MC_3_UUID = UUID.fromString("ec83e345-7e4a-4a46-b049-0c34dd201b18");

    private ConcurrentArrayRingbuffer<MCEventDTO> queue;

    private ManagementCenterService.MCEventStore eventStore;

    private FakeClock clock;

    private void assertPolledEventCount(int expectedEventCount, UUID mcRemoteAddress) {
        assertEquals(expectedEventCount, eventStore.pollMCEvents(mcRemoteAddress).size());
    }

    void inNextMilli(Runnable r) {
        clock.now += 1_000_000;
        r.run();
    }

    private void logEvent() {
        eventStore.log(new ManagementCenterServiceIntegrationTest.TestEvent(clock.now));
    }

    @Before
    public void before() {
        clock = new FakeClock();
        queue = new ConcurrentArrayRingbuffer<>(1000);
        eventStore = new ManagementCenterService.MCEventStore(clock, queue, new NoLogFactory().getLogger(""));
    }

    @Test
    public void multipleMCs_canPollSeparately() {
        assertPolledEventCount(0, MC_2_UUID);
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        inNextMilli(() -> {
            assertPolledEventCount(2, MC_1_UUID);
            assertPolledEventCount(2, MC_2_UUID);
            assertPolledEventCount(0, MC_1_UUID);
            assertPolledEventCount(0, MC_2_UUID);
        });
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_1_UUID);
        });
        inNextMilli(() -> {
            logEvent();
            assertPolledEventCount(1, MC_1_UUID);
            assertPolledEventCount(2, MC_2_UUID);
        });
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        clock.now += MC_EVENTS_WINDOW_NANOS;
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_UUID);
            assertPolledEventCount(0, MC_2_UUID);
        });
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_UUID);
        });
        inNextMilli(() -> {
            logEvent();
            assertPolledEventCount(1, MC_1_UUID);
            assertPolledEventCount(1, MC_2_UUID);
        });
    }

    @Test
    public void sameMilliEvent_reportedInNextPoll() {
        assertPolledEventCount(0, MC_1_UUID);
        logEvent();
        logEvent();
        inNextMilli(() -> {
            logEvent();
            logEvent();
            assertPolledEventCount(4, MC_1_UUID);
            logEvent();
            logEvent();
            logEvent();
        });
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        inNextMilli(() -> {
            // 3 of these were reported in the same MS as the previous poll, 2 of them later
            assertPolledEventCount(5, MC_1_UUID);
        });
    }

    @Test
    public void eventCollectionStops_whenNoPollHappens_in30sec() {
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_UUID);
            assertPolledEventCount(0, MC_2_UUID);
            assertPolledEventCount(0, MC_3_UUID);
        });
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_3_UUID);
        });
        clock.now += TimeUnit.SECONDS.toNanos(31);
        logEvent();
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_UUID);
            assertPolledEventCount(0, MC_3_UUID);
        });
    }

    @Test
    public void disconnectRecognized_after120secInactivity() {
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_UUID);
            assertPolledEventCount(0, MC_2_UUID);
            assertPolledEventCount(0, MC_3_UUID);
        });
        logEvent();
        assertPolledEventCount(1, MC_3_UUID); // reading if previous event noted, nextSequence incremented
        clock.now += TimeUnit.SECONDS.toNanos(30);
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_1_UUID);
            assertPolledEventCount(1, MC_2_UUID);
        });
        logEvent();
        clock.now += TimeUnit.SECONDS.toNanos(30);
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_1_UUID);
            assertPolledEventCount(1, MC_2_UUID);
        });
        clock.now += TimeUnit.SECONDS.toNanos(30);
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_UUID);
            assertPolledEventCount(0, MC_2_UUID);
        });
        clock.now += TimeUnit.SECONDS.toNanos(30);
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_1_UUID);
            assertPolledEventCount(1, MC_2_UUID);
            // 120sec passed since last read, previous nextSequence forgotten, therefore all 3 events are logged
            assertPolledEventCount(3, MC_3_UUID);
        });
    }

    /**
     * Runs 50 threads in parallel, each thread performs 1000 tasks. Each task is one of:
     * - logging 800 events
     * - or polling as MC_1
     * - or polling as MC_2
     * <p>
     * The test fails if any of the threads throws {@link java.util.ConcurrentModificationException} (or any other exception).
     */
    @Test
    @Category(NightlyTest.class)
    public void stressTest()
            throws InterruptedException {
        Runnable[] tasks = new Runnable[]{
                () -> {
                    for (int i = 0; i < 800; ++i) {
                        inNextMilli(this::logEvent);
                    }
                },
                () -> inNextMilli(() -> eventStore.pollMCEvents(MC_1_UUID)),
                () -> inNextMilli(() -> eventStore.pollMCEvents(MC_2_UUID))
        };
        Random random = new Random();

        Set<Throwable> thrownByThreads = new ConcurrentSkipListSet<>();

        ThreadFactory tf = new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, exc) -> {
                    exc.printStackTrace();
                    thrownByThreads.add(exc);
                })
                .build();

        int threadCount = 50;
        int taskCount = 1000;
        List<Thread> threads = IntStream.range(0, threadCount)
                .mapToObj(i -> IntStream.range(0, taskCount)
                        .mapToObj(j -> tasks[Math.abs(random.nextInt()) % tasks.length])
                        .collect(toList()))
                .map(tasksForThread -> tf.newThread(() -> tasksForThread.forEach(Runnable::run)))
                .collect(toList());
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        if (!thrownByThreads.isEmpty()) {
            thrownByThreads.forEach(Throwable::printStackTrace);
            throw new AssertionError("at least one thread threw an exception");
        }
    }
}
