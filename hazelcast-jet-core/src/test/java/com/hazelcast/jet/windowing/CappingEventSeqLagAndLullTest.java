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

package com.hazelcast.jet.windowing;

import org.junit.Test;

import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndLull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;

public class CappingEventSeqLagAndLullTest {

    private static final int MAX_LULL_MS = 3;
    private long currTime;
    private PunctuationPolicy p = cappingEventSeqLagAndLull(2, MAX_LULL_MS, () -> currTime);

    @Test
    public void when_outOfOrderEvents_then_monotonicPunct() {
        assertEquals(8, p.reportEvent(10));
        assertEquals(8, p.reportEvent(9));
        assertEquals(8, p.reportEvent(8));
        assertEquals(8, p.reportEvent(7)); // late event
        assertEquals(9, p.reportEvent(11));
    }

    @Test
    public void when_eventsStop_then_puncIncreases() {
        // Given - starting event
        assertEquals(10, p.reportEvent(12));
        long maxLullNanos = MILLISECONDS.toNanos(MAX_LULL_MS);

        // When
        for (; currTime < maxLullNanos; currTime += 1_000_000) {
            assertEquals(10, p.getCurrentPunctuation());
        }

        // Then - punc increases
        for (; currTime <= 10_000_000; currTime += 1_000_000) {
            assertEquals("at time=" + currTime,
                    10 + NANOSECONDS.toMillis(currTime - maxLullNanos),
                    p.getCurrentPunctuation());
        }
    }

    @Test
    public void when_noEventEver_then_increaseFromLongMinValue() {
        // Given
        assertEquals(Long.MIN_VALUE, p.getCurrentPunctuation()); // initializes maxLullAt
        long maxLullNanos = MILLISECONDS.toNanos(MAX_LULL_MS);

        // When
        for (; currTime < maxLullNanos; currTime += 1_000_000) {
            assertEquals(Long.MIN_VALUE, p.getCurrentPunctuation());
        }

        // Then - punct increases
        for (; currTime <= 10_000_000; currTime += 1_000_000) {
            assertEquals("at time=" + currTime,
                    Long.MIN_VALUE + NANOSECONDS.toMillis(currTime - maxLullNanos),
                    p.getCurrentPunctuation());
        }
    }
}
