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

import static org.junit.Assert.assertEquals;

public class CappingEventSeqLagAndRetentionTest {

    private static final int MAX_RETAIN_MS = 8;
    private static final int EVENT_SEQ_LAG = 8;

    private long time;
    private PunctuationPolicy p = PunctuationPolicies.cappingEventSeqLagAndRetention(EVENT_SEQ_LAG,
            MAX_RETAIN_MS, 8, () -> time);

    @Test
    public void when_outOfOrderEvents_then_monotonicPunct() {
        assertPunc(2, 10);
        assertPunc(2, 9);
        assertPunc(2, 8);
        assertPunc(2, 7);
        assertPunc(3, 11);
    }

    @Test
    public void when_clockIncreasing_then_eventuallyCatchUp() {
        assertPunc(2, 10);
        time = 1;
        assertPunc(3, 11);
        time = 2;
        assertPunc(4, 12);
        time = 3;
        assertPunc(5, 13);
        time = 4;
        assertEquals(5, p.getCurrentPunctuation());
        time = 8;
        assertEquals(10, p.getCurrentPunctuation());
        time = 9;
        assertEquals(11, p.getCurrentPunctuation());
        time = 10;
        assertEquals(12, p.getCurrentPunctuation());
        time = 11;
        assertEquals(13, p.getCurrentPunctuation());
    }

    private void assertPunc(long expected, long eventSeq) {
        assertEquals(expected, p.reportEvent(eventSeq));
        assertEquals(expected, p.getCurrentPunctuation());
    }
}
