/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorClockTest {

    @Test
    public void testEquals() {
        final VectorClock clock = vectorClock("A", 1, "B", 2);
        assertEquals(clock, vectorClock("A", 1, "B", 2));
        assertEquals(clock, new VectorClock(clock));
    }

    @Test
    public void testIsAfter() {
        assertFalse(vectorClock().isAfter(vectorClock()));
        assertTrue(vectorClock("A", 1).isAfter(vectorClock()));
        assertFalse(vectorClock("A", 1).isAfter(vectorClock("A", 1)));
        assertFalse(vectorClock("A", 1).isAfter(vectorClock("B", 1)));
        assertTrue(vectorClock("A", 1, "B", 1).isAfter(vectorClock("A", 1)));
        assertFalse(vectorClock("A", 1).isAfter(vectorClock("A", 1, "B", 1)));
        assertTrue(vectorClock("A", 2).isAfter(vectorClock("A", 1)));
        assertFalse(vectorClock("A", 2).isAfter(vectorClock("A", 1, "B", 1)));
        assertTrue(vectorClock("A", 2, "B", 1).isAfter(vectorClock("A", 1, "B", 1)));
    }

    @Test
    public void testMerge() {
        assertMerged(
                vectorClock("A", 1),
                vectorClock(),
                vectorClock("A", 1));
        assertMerged(
                vectorClock("A", 1),
                vectorClock("A", 2),
                vectorClock("A", 2));
        assertMerged(
                vectorClock("A", 2),
                vectorClock("A", 1),
                vectorClock("A", 2));
        assertMerged(
                vectorClock("A", 3, "B", 1),
                vectorClock("A", 1, "B", 2, "C", 3),
                vectorClock("A", 3, "B", 2, "C", 3));
    }

    @Test
    public void testIsEmpty() {
        assertTrue(vectorClock().isEmpty());
        assertFalse(vectorClock("A", 1).isEmpty());
    }

    private void assertMerged(VectorClock from, VectorClock to, VectorClock expected) {
        to.merge(from);
        assertEquals(to, expected);
    }

    private VectorClock vectorClock(Object... params) {
        final VectorClock clock = new VectorClock();
        for (int i = 0; i < params.length; ) {
            clock.setReplicaTimestamp((String) params[i++], (Integer) params[i++]);
        }
        return clock;
    }
}
