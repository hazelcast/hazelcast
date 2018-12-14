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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.noThrottling;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.WatermarkSourceUtil.NO_NATIVE_TIME;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WatermarkSourceUtilTest {

    private static final long LAG = 3;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void smokeTest() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), noThrottling(), 5)
        );
        wsu.increasePartitionCount(0L, 2);

        // all partitions are active initially
        assertTraverser(wsu.handleEvent(ns(1), null, 0, NO_NATIVE_TIME));
        // now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
        assertTraverser(wsu.handleEvent(ns(5), null, 0, NO_NATIVE_TIME), IDLE_MESSAGE);
        // still all partitions are idle, but IDLE_MESSAGE should not be emitted for the second time
        assertTraverser(wsu.handleEvent(ns(5), null, 0, NO_NATIVE_TIME));
        // now we observe event on partition0, watermark should be immediately forwarded because the other queue is idle
        assertTraverser(wsu.handleEvent(ns(5), 100L, 0, NO_NATIVE_TIME), wm(100 - LAG), 100L);
        // now we'll have a event on the other partition. No WM is emitted because it's older than already emitted one
        assertTraverser(wsu.handleEvent(ns(5), 90L, 0, NO_NATIVE_TIME), 90L);
        assertTraverser(wsu.handleEvent(ns(5), 101L, 0, NO_NATIVE_TIME), wm(101 - LAG), 101L);
    }

    @Test
    public void smokeTest_disabledIdleTimeout() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), noThrottling(), -1)
        );
        wsu.increasePartitionCount(2);

        // all partitions are active initially
        assertTraverser(wsu.handleNoEvent());
        // let's have events only in partition0. No WM is output because we wait for the other partition indefinitely
        assertTraverser(wsu.handleEvent(10L, 0, NO_NATIVE_TIME), 10L);
        assertTraverser(wsu.handleEvent(11L, 0, NO_NATIVE_TIME), 11L);
        // now have some events in the other partition, wms will be output
        assertTraverser(wsu.handleEvent(10L, 1, NO_NATIVE_TIME), wm(10 - LAG), 10L);
        assertTraverser(wsu.handleEvent(11L, 1, NO_NATIVE_TIME), wm(11 - LAG), 11L);
        // now partition1 will get ahead of partition0 -> no WM
        assertTraverser(wsu.handleEvent(12L, 1, NO_NATIVE_TIME), 12L);
        // another event in partition0, we'll get the wm
        assertTraverser(wsu.handleEvent(13L, 0, NO_NATIVE_TIME), wm(12 - LAG), 13L);
    }

    @Test
    public void test_zeroPartitions() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), noThrottling(), -1)
        );

        // it should immediately emit the idle message, even though the idle timeout is -1
        assertTraverser(wsu.handleNoEvent(), IDLE_MESSAGE);
        assertTraverser(wsu.handleNoEvent());

        // after adding a partition and observing an event, WM should be emitted
        wsu.increasePartitionCount(1);
        assertTraverser(wsu.handleNoEvent()); // can't send WM here, we don't know what its value would be
        assertTraverser(wsu.handleEvent(10L, 0, NO_NATIVE_TIME), wm(10 - LAG), 10L);
    }

    @Test
    public void when_idle_event_idle_then_twoIdleMessagesSent() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), noThrottling(), 10)
        );
        wsu.increasePartitionCount(1);
        assertTraverser(wsu.handleEvent(ns(0), 10L, 0, NO_NATIVE_TIME), wm(10 - LAG), 10L);

        // When - become idle
        assertTraverser(wsu.handleEvent(ns(10), null, 0, NO_NATIVE_TIME), IDLE_MESSAGE);
        // When - another event, but no new WM
        assertTraverser(wsu.handleEvent(ns(10), 10L, 0, NO_NATIVE_TIME), 10L);
        // When - become idle again
        assertTraverser(wsu.handleEvent(ns(10), null, 0, NO_NATIVE_TIME));
        assertTraverser(wsu.handleEvent(ns(20), null, 0, NO_NATIVE_TIME), IDLE_MESSAGE);
    }

    @Test
    public void when_eventInOneOfTwoPartitions_then_wmAndIdleMessageForwardedAfterTimeout() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(Long::longValue, limitingLag(LAG), noThrottling(), 10)
        );
        wsu.increasePartitionCount(ns(0), 2);

        // When
        assertTraverser(wsu.handleEvent(ns(0), 10L, 0, NO_NATIVE_TIME), 10L);

        // Then
        assertTraverser(wsu.handleEvent(ns(10), null, 0, NO_NATIVE_TIME),
                wm(10 - LAG),
                IDLE_MESSAGE);
    }

    @Test
    public void when_noTimestampFnAndNoNativeTime_then_throw() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(null, limitingLag(LAG), noThrottling(), 10)
        );
        wsu.increasePartitionCount(ns(0), 1);

        exception.expectMessage("Neither timestampFn nor nativeEventTime specified");
        wsu.handleEvent(ns(0), 10L, 0, NO_NATIVE_TIME);
    }

    @Test
    public void when_noTimestampFn_then_useNativeTime() {
        WatermarkSourceUtil<Long> wsu = new WatermarkSourceUtil<>(
                eventTimePolicy(null, limitingLag(LAG), noThrottling(), 5)
        );
        wsu.increasePartitionCount(0L, 1);

        assertTraverser(wsu.handleEvent(ns(1), 10L, 0, 11L), wm(11L - LAG), 10L);
        assertTraverser(wsu.handleEvent(ns(1), 11L, 0, 12L), wm(12L - LAG), 11L);
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

    public Watermark wm(long time) {
        return new Watermark(time);
    }
}
