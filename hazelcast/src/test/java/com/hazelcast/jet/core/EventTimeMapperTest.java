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

package com.hazelcast.jet.core;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.EventTimeMapper.NO_NATIVE_TIME;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EventTimeMapperTest {

    private static final long LAG = 3;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void smokeTest() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), 1, 0, 5)
        );
        eventTimeMapper.addPartitions(0L, 2);

        // all partitions are active initially
        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), null, 0, NO_NATIVE_TIME));
        // now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
        assertTraverser(eventTimeMapper.flatMapEvent(ns(5), null, 0, NO_NATIVE_TIME), IDLE_MESSAGE);
        // still all partitions are idle, but IDLE_MESSAGE should not be emitted for the second time
        assertTraverser(eventTimeMapper.flatMapEvent(ns(5), null, 0, NO_NATIVE_TIME));
        // now we observe event on partition0, watermark should be immediately forwarded because the other queue is idle
        assertTraverser(eventTimeMapper.flatMapEvent(ns(5), 100L, 0, NO_NATIVE_TIME), wm(100 - LAG), 100L);
        // observe another event on the same partition. No WM is emitted because the event is older
        assertTraverser(eventTimeMapper.flatMapEvent(ns(5), 90L, 0, NO_NATIVE_TIME), 90L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(5), 101L, 0, NO_NATIVE_TIME), wm(101 - LAG), 101L);
    }

    @Test
    public void smokeTest_disabledIdleTimeout() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), 1, 0, 0)
        );
        eventTimeMapper.addPartitions(2);

        // all partitions are active initially
        assertTraverser(eventTimeMapper.flatMapIdle());
        // let's have events only in partition0. No WM is output because we wait for the other partition indefinitely
        assertTraverser(eventTimeMapper.flatMapEvent(10L, 0, NO_NATIVE_TIME), 10L);
        assertTraverser(eventTimeMapper.flatMapEvent(11L, 0, NO_NATIVE_TIME), 11L);
        // now have some events in the other partition, wms will be output
        assertTraverser(eventTimeMapper.flatMapEvent(10L, 1, NO_NATIVE_TIME), wm(10 - LAG), 10L);
        assertTraverser(eventTimeMapper.flatMapEvent(11L, 1, NO_NATIVE_TIME), wm(11 - LAG), 11L);
        // now partition1 will get ahead of partition0 -> no WM
        assertTraverser(eventTimeMapper.flatMapEvent(12L, 1, NO_NATIVE_TIME), 12L);
        // another event in partition0, we'll get the wm
        assertTraverser(eventTimeMapper.flatMapEvent(13L, 0, NO_NATIVE_TIME), wm(12 - LAG), 13L);
    }

    @Test
    public void test_zeroPartitions() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), 1, 0, 0)
        );

        // it should immediately emit the idle message, even though the idle timeout is -1
        assertTraverser(eventTimeMapper.flatMapIdle(), IDLE_MESSAGE);
        assertTraverser(eventTimeMapper.flatMapIdle());

        // after adding a partition and observing an event, WM should be emitted
        eventTimeMapper.addPartitions(1);
        assertTraverser(eventTimeMapper.flatMapIdle()); // can't send WM here, we don't know what its value would be
        assertTraverser(eventTimeMapper.flatMapEvent(10L, 0, NO_NATIVE_TIME), wm(10 - LAG), 10L);
    }

    @Test
    public void when_idle_event_idle_then_twoIdleMessagesSent() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), 1, 0, 10)
        );
        eventTimeMapper.addPartitions(1);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME), wm(10 - LAG), 10L);

        // When - become idle
        assertTraverser(eventTimeMapper.flatMapEvent(ns(10), null, 0, NO_NATIVE_TIME), IDLE_MESSAGE);
        // When - another event, but no new WM
        assertTraverser(eventTimeMapper.flatMapEvent(ns(10), 10L, 0, NO_NATIVE_TIME), 10L);
        // When - become idle again
        assertTraverser(eventTimeMapper.flatMapEvent(ns(10), null, 0, NO_NATIVE_TIME));
        assertTraverser(eventTimeMapper.flatMapEvent(ns(20), null, 0, NO_NATIVE_TIME), IDLE_MESSAGE);
    }

    @Test
    public void when_eventInOneOfTwoPartitions_then_wmAndIdleMessageForwardedAfterTimeout() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), 1, 0, 10)
        );
        eventTimeMapper.addPartitions(ns(0), 2);

        // When
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME), 10L);

        // Then
        assertTraverser(eventTimeMapper.flatMapEvent(ns(10), null, 0, NO_NATIVE_TIME),
                wm(10 - LAG),
                IDLE_MESSAGE);
    }

    @Test
    public void when_noTimestampFnAndNoNativeTime_then_throw() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(null, limitingLag(LAG), 1, 0, 10)
        );
        eventTimeMapper.addPartitions(ns(0), 1);

        exception.expectMessage("Neither timestampFn nor nativeEventTime specified");
        eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME);
    }

    @Test
    public void when_noTimestampFn_then_useNativeTime() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(null, limitingLag(LAG), 1, 0, 5)
        );
        eventTimeMapper.addPartitions(0L, 1);

        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), 10L, 0, 11L), wm(11L - LAG), 10L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), 11L, 0, 12L), wm(12L - LAG), 11L);
    }

    @Test
    public void when_throttlingToMaxFrame_then_noWatermarksOutput() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), 0, 0, 5)
        );
        eventTimeMapper.addPartitions(0L, 1);

        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), -10L, 0, 11L), -10L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), 10L, 0, 12L), 10L);
    }

    @Test
    public void when_restoredState_then_wmDoesNotGoBack() {
        EventTimePolicy<Long> eventTimePolicy = eventTimePolicy(Long::longValue, limitingLag(0), 1, 0, 5);
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(0L, 1);

        // When
        eventTimeMapper.restoreWatermark(0, 10);

        // Then
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 9L, 0, NO_NATIVE_TIME), 9L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME), 10L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 11L, 0, NO_NATIVE_TIME), wm(11), 11L);
    }

    @Test
    public void when_twoActiveQueues_theLaggingOneRemoved_then_wmForwarded() {
        EventTimePolicy<Long> eventTimePolicy = eventTimePolicy(Long::longValue, limitingLag(0), 1, 0, 5);
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(0L, 2);

        // When
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME), 10L);

        // Then
        assertTraverser(eventTimeMapper.removePartition(ns(0), 1), wm(10));
    }

    @Test
    public void when_twoActiveQueues_theAheadOneRemoved_then_noWmForwarded() {
        EventTimePolicy<Long> eventTimePolicy = eventTimePolicy(Long::longValue, limitingLag(0), 1, 0, 5);
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(0L, 2);

        // When
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME), 10L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 11L, 1, NO_NATIVE_TIME), wm(10), 11L);

        // Then
        assertTraverser(eventTimeMapper.removePartition(ns(0), 1));
    }

    @Test
    public void when_threePartitions_laggingOneRemoved_secondLaggingOneIdle_then_noWmForwarded() {
        EventTimePolicy<Long> eventTimePolicy = eventTimePolicy(Long::longValue, limitingLag(0), 1, 0, 5);
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(0L, 3);

        // When
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 10L, 0, NO_NATIVE_TIME), 10L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), 11L, 1, NO_NATIVE_TIME), 11L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(1), 12L, 2, NO_NATIVE_TIME), wm(10), 12L);

        // Then
        // in this call partition0 will turn idle and partition1 is be removed -> wm(12) is forwarded
        assertTraverser(eventTimeMapper.removePartition(ns(5), 1), wm(12));
    }

    @Test
    public void when_currentWmBeyondReportedEventTimestamp_then_eventNotLate() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, constantWmPolicy(42L), 1, 0, 5));
        eventTimeMapper.addPartitions(0L, 1);

        assertTraverser(eventTimeMapper.flatMapEvent(41L, 0, NO_NATIVE_TIME), wm(41), 41L);
    }

    @Test
    public void when_currentWmBeyondReportedEventTimestamp_and_eventLate_then_wmDoesNotGoBack() {
        EventTimeMapper<Long> eventTimeMapper = new EventTimeMapper<>(
                eventTimePolicy(Long::longValue, constantWmPolicy(42L), 1, 0, 5));
        eventTimeMapper.addPartitions(0L, 1);

        assertTraverser(eventTimeMapper.flatMapEvent(41L, 0, NO_NATIVE_TIME), wm(41), 41L);
        assertTraverser(eventTimeMapper.flatMapEvent(ns(0), 40L, 0, NO_NATIVE_TIME), 40L);
    }

    private static SupplierEx<WatermarkPolicy> constantWmPolicy(long value) {
        return () -> new WatermarkPolicy() {
            @Override
            public void reportEvent(long timestamp) { }

            @Override
            public long getCurrentWatermark() {
                return value;
            }
        };
    }

    private <T> void assertTraverser(Traverser<T> actual, T ... expected) {
        for (T element : expected) {
            assertEquals(element, actual.next());
        }
        assertNull(actual.next());
    }

    private long ns(long ms) {
        return MILLISECONDS.toNanos(ms);
    }
}
