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

package com.hazelcast.jet.function;

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.function.DistributedComparator.nullsFirst;
import static com.hazelcast.jet.function.DistributedComparator.nullsLast;
import static com.hazelcast.jet.function.DistributedComparators.NATURAL_ORDER;
import static com.hazelcast.jet.function.DistributedComparators.REVERSE_ORDER;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
public class DistributedComparatorTest {

    @Test
    public void when_reverseComparator() {
        assertSame(REVERSE_ORDER, NATURAL_ORDER.reversed());
        assertSame(NATURAL_ORDER, REVERSE_ORDER.reversed());
    }

    @Test
    public void when_reverseOrderComparator() {
        DistributedComparator c = REVERSE_ORDER;
        assertEquals(1, c.compare(1, 2));
        assertEquals(-1, c.compare(2, 1));
    }

    @Test
    public void when_nullsFirstComparator() {
        DistributedComparator c = nullsFirst(NATURAL_ORDER);
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void when_nullsLastComparator() {
        DistributedComparator c = nullsLast(NATURAL_ORDER);
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void when_nullsFirst_withoutWrapped() {
        DistributedComparator c = nullsFirst(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void when_nullsLast_withoutWrapped() {
        DistributedComparator c = nullsLast(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void testSerializable_naturalOrder() {
        checkSerializable(DistributedComparator.naturalOrder(), null);
    }

    @Test
    public void testSerializable_reverseOrder() {
        checkSerializable(DistributedComparator.reverseOrder(), null);
    }

    @Test
    public void testSerializable_thenComparing_keyExtractor() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparing(Object::toString),
                null);
    }

    @Test
    public void testSerializable_thenComparing_otherComparator() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparing(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_thenComparing_keyExtractor_keyComparator() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparing(Object::toString, Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_thenComparingInt() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparingInt(Object::hashCode),
                null);
    }

    @Test
    public void testSerializable_thenComparingLong() {
        checkSerializable(
                DistributedComparator.<Long>naturalOrder()
                                      .thenComparingLong(Long::longValue),
                null);
    }

    @Test
    public void testSerializable_thenComparingDouble() {
        checkSerializable(
                DistributedComparator.<Double>naturalOrder()
                        .thenComparingDouble(Double::doubleValue),
                null);
    }

    @Test
    public void testSerializable_nullsFirst() {
        checkSerializable(
                DistributedComparator.<Comparable>nullsFirst(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_nullsLast() {
        checkSerializable(
                DistributedComparator.<Comparable>nullsLast(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_comparing_keyExtractor() {
        checkSerializable(DistributedComparator.comparing(Object::toString), null);
    }

    @Test
    public void testSerializable_comparing_keyExtractor_keyComparator() {
        checkSerializable(DistributedComparator.comparing(Object::toString, String::compareTo), null);
    }

    @Test
    public void testSerializable_comparingInt() {
        checkSerializable(DistributedComparator.comparingInt(Object::hashCode), null);
    }

    @Test
    public void testSerializable_comparingLong() {
        checkSerializable(DistributedComparator.comparingLong(Long::longValue), null);
    }

    @Test
    public void testSerializable_comparingDouble() {
        checkSerializable(DistributedComparator.comparingDouble(Double::doubleValue), null);
    }

}
