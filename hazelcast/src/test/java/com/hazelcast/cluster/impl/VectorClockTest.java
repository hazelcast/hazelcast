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

package com.hazelcast.cluster.impl;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorClockTest {

    private static UUID[] uuidParams;

    @BeforeClass
    public static void setUp() {
        uuidParams = new UUID[3];
        for (int i = 0; i < 3; i++) {
            uuidParams[i] = UuidUtil.newUnsecureUUID();
        }
    }

    @Test
    public void testEquals() {
        final VectorClock clock = vectorClock(uuidParams[0], 1, uuidParams[1], 2);
        assertEquals(clock, vectorClock(uuidParams[0], 1, uuidParams[1], 2));
        assertEquals(clock, new VectorClock(clock));
    }

    @Test
    public void testIsAfter() {
        assertFalse(vectorClock().isAfter(vectorClock()));
        assertTrue(vectorClock(uuidParams[0], 1).isAfter(vectorClock()));
        assertFalse(vectorClock(uuidParams[0], 1).isAfter(vectorClock(uuidParams[0], 1)));
        assertFalse(vectorClock(uuidParams[0], 1).isAfter(vectorClock(uuidParams[1], 1)));
        assertTrue(vectorClock(uuidParams[0], 1, uuidParams[1], 1).isAfter(vectorClock(uuidParams[0], 1)));
        assertFalse(vectorClock(uuidParams[0], 1).isAfter(vectorClock(uuidParams[0], 1, uuidParams[1], 1)));
        assertTrue(vectorClock(uuidParams[0], 2).isAfter(vectorClock(uuidParams[0], 1)));
        assertFalse(vectorClock(uuidParams[0], 2).isAfter(vectorClock(uuidParams[0], 1, uuidParams[1], 1)));
        assertTrue(vectorClock(uuidParams[0], 2, uuidParams[1], 1).isAfter(vectorClock(uuidParams[0], 1, uuidParams[1], 1)));
    }

    @Test
    public void testMerge() {
        assertMerged(
                vectorClock(uuidParams[0], 1),
                vectorClock(),
                vectorClock(uuidParams[0], 1));
        assertMerged(
                vectorClock(uuidParams[0], 1),
                vectorClock(uuidParams[0], 2),
                vectorClock(uuidParams[0], 2));
        assertMerged(
                vectorClock(uuidParams[0], 2),
                vectorClock(uuidParams[0], 1),
                vectorClock(uuidParams[0], 2));
        assertMerged(
                vectorClock(uuidParams[0], 3, uuidParams[1], 1),
                vectorClock(uuidParams[0], 1, uuidParams[1], 2, uuidParams[2], 3),
                vectorClock(uuidParams[0], 3, uuidParams[1], 2, uuidParams[2], 3));
    }

    @Test
    public void testIsEmpty() {
        assertTrue(vectorClock().isEmpty());
        assertFalse(vectorClock(uuidParams[0], 1).isEmpty());
    }

    private void assertMerged(VectorClock from, VectorClock to, VectorClock expected) {
        to.merge(from);
        assertEquals(to, expected);
    }

    private VectorClock vectorClock(Object... params) {
        final VectorClock clock = new VectorClock();
        for (int i = 0; i < params.length; ) {
            clock.setReplicaTimestamp((UUID) params[i++], (Integer) params[i++]);
        }
        return clock;
    }
}
