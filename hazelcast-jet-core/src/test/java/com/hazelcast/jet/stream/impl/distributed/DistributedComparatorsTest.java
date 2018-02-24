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

package com.hazelcast.jet.stream.impl.distributed;

import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.function.DistributedComparator.nullsFirst;
import static com.hazelcast.jet.function.DistributedComparator.nullsLast;
import static com.hazelcast.jet.stream.impl.distributed.DistributedComparators.NATURAL_ORDER;
import static com.hazelcast.jet.stream.impl.distributed.DistributedComparators.REVERSE_ORDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
public class DistributedComparatorsTest {

    @Test
    public void reverseComparator() {
        assertSame(REVERSE_ORDER, NATURAL_ORDER.reversed());
        assertSame(NATURAL_ORDER, REVERSE_ORDER.reversed());
    }

    @Test
    public void reverseOrderComparator() {
        DistributedComparator c = REVERSE_ORDER;
        assertEquals(1, c.compare(1, 2));
        assertEquals(-1, c.compare(2, 1));
    }

    @Test
    public void nullsFirstComparator() {
        DistributedComparator c = nullsFirst(NATURAL_ORDER);
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void nullsLastComparator() {
        DistributedComparator c = nullsLast(NATURAL_ORDER);
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void nullsFirst_withoutWrapped() {
        DistributedComparator c = nullsFirst(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void nullsLast_withoutWrapped() {
        DistributedComparator c = nullsLast(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }
}
