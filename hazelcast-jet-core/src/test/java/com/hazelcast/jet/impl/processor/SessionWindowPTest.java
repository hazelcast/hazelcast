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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.Session;
import com.hazelcast.jet.StreamingTestSupport;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SessionWindowPTest extends StreamingTestSupport {

    private static final int SESSION_TIMEOUT = 10;
    private SessionWindowP<Entry<String, Long>, String, LongAccumulator, Long> processor;

    @Before
    public void before() {
        processor = new SessionWindowP<>(
                SESSION_TIMEOUT,
                Entry::getValue,
                entryKey(),
                AggregateOperations.counting());
        processor.init(outbox, mock(Processor.Context.class));
    }

    @Test
    public void when_orderedEventsWithOneKey() {
        List<Entry<String, Long>> events = eventsWithKey("a");
        assertCorrectness(events);
    }

    @Test
    public void when_disorderedEventsWithOneKey() {
        List<Entry<String, Long>> events = eventsWithKey("a");
        shuffle(events);
        assertCorrectness(events);
    }

    @Test
    public void when_orderedEventsWithThreeKeys() {
        List<Entry<String, Long>> events = new ArrayList<>();
        events.addAll(eventsWithKey("a"));
        events.addAll(eventsWithKey("b"));
        events.addAll(eventsWithKey("c"));
        assertCorrectness(events);
    }

    @Test
    public void when_disorderedEVentsWithThreeKeys() {
        List<Entry<String, Long>> events = new ArrayList<>();
        events.addAll(eventsWithKey("a"));
        events.addAll(eventsWithKey("b"));
        events.addAll(eventsWithKey("c"));
        shuffle(events);
        assertCorrectness(events);
    }

    @Test
    public void when_batchProcessing_then_flushEverything() {
        // Given
        inbox.addAll(eventsWithKey("a"));
        // this watermark will cause the first session to be emitted, but not the second
        inbox.add(new Watermark(25));

        // When
        processor.process(0, inbox);

        // Then
        List<Session<String, Long>> expectedSessions = expectedSessions("a").collect(toList());
        assertEquals(expectedSessions.get(0), pollOutbox());
        assertNull(pollOutbox());

        // When
        // this will cause the second session to be emitted
        long start = System.nanoTime();
        processor.complete();
        long processTime = System.nanoTime() - start;
        // this is to test that there is no iteration from current watermark up to Long.MAX_VALUE, which
        // will take too long.

        // Then
        assertTrue("process took too long: " + processTime, processTime < MILLISECONDS.toNanos(100));
        assertEquals(expectedSessions.get(1), pollOutbox());
        assertNull(pollOutbox());
    }

    private void assertCorrectness(List<Entry<String, Long>> events) {
        // Given
        Set<String> keys = new HashSet<>();
        for (Entry<String, Long> ev : events) {
            inbox.add(ev);
            keys.add(ev.getKey());
        }
        Set<Session> expectedSessions = keys.stream()
                                            .flatMap(SessionWindowPTest::expectedSessions)
                                            .collect(toSet());
        inbox.add(new Watermark(100));

        // When
        processor.process(0, inbox);
        Set<Object> actualSessions = range(0, expectedSessions.size())
                .mapToObj(x -> pollOutbox())
                .collect(toSet());

        // Then
        try {
            assertEquals(expectedSessions, actualSessions);
            assertNull(pollOutbox());
            // Check against memory leaks
            assertTrue("keyToWindows not empty", processor.keyToWindows.isEmpty());
            assertTrue("deadlineToKeys not empty", processor.deadlineToKeys.isEmpty());
        } catch (AssertionError e) {
            System.err.println("Tested with events: " + events);
            throw e;
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            SessionWindowPTest test = new SessionWindowPTest();
            test.before();
            test.runBench();
        }
    }

    @SuppressWarnings("checkstyle:emptystatement")
    private void runBench() {
        Random rnd = ThreadLocalRandom.current();
        long start = System.nanoTime();
        long eventCount = 40_000_000;
        long keyCount = 2000;
        long eventsPerKey = eventCount / keyCount;
        int spread = 4000;
        int timestampStep = 20;
        int wmLag = 2000;
        long wmInterval = 100;
        System.out.format("keyCount %,d eventsPerKey %,d wmInterval %,d%n", keyCount, eventsPerKey, wmInterval);
        for (long idx = 0; idx < eventsPerKey; idx++) {
            long timestampBase = idx * timestampStep;
            for (long key = (timestampBase / SESSION_TIMEOUT) % 2; key < keyCount; key += 2) {
                while (!processor.tryProcess0(entry(key, timestampBase + rnd.nextInt(spread)))) { }
                while (!processor.tryProcess0(entry(key, timestampBase + rnd.nextInt(spread)))) { }
            }
            if (idx % wmInterval == 0) {
                Watermark wm = new Watermark(timestampBase - wmLag);
                int winCount = 0;
                while (!processor.tryProcessWm0(wm)) {
                    while (pollOutbox() != null) {
                        winCount++;
                    }
                }
                while (pollOutbox() != null) {
                    winCount++;
                }
            }
        }
        long took = System.nanoTime() - start;
        System.out.format("%nThroughput %,3d events/second%n", SECONDS.toNanos(1) * eventCount / took);
    }

    private static List<Entry<String, Long>> eventsWithKey(String key) {
        return asList(
                // session 1: [12..22]
                entry(key, 1L),
                entry(key, 6L),
                entry(key, 12L),

                // session 2: [30..50]
                entry(key, 30L),
                entry(key, 35L),
                entry(key, 40L)
        );
    }

    private static Stream<Session<String, Long>> expectedSessions(String key) {
        return Stream.of(
                new Session<>(key, 1, 22, 3L),
                new Session<>(key, 30, 50, 3L)
        );
    }
}
