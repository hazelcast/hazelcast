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

package com.hazelcast.jet.function;

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

@RunWith(HazelcastParallelClassRunner.class)
public class DistributedComparatorTest {
    @Before
    public void setUp() {

    }

    @Test
    public void naturalOrder() {
        checkSerializable(DistributedComparator.naturalOrder(), null);
    }

    @Test
    public void reverseOrder() {
        checkSerializable(DistributedComparator.reverseOrder(), null);
    }

    @Test
    public void thenComparing_keyExtractor() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparing(Object::toString),
                null);
    }

    @Test
    public void thenComparing_otherComparator() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparing(Comparable::compareTo),
                null);
    }

    @Test
    public void thenComparing_keyExtractor_keyComparator() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparing(Object::toString, Comparable::compareTo),
                null);
    }

    @Test
    public void thenComparingInt() {
        checkSerializable(
                DistributedComparator.naturalOrder()
                                     .thenComparingInt(Object::hashCode),
                null);
    }

    @Test
    public void thenComparingLong() {
        checkSerializable(
                DistributedComparator.<Long>naturalOrder()
                                      .thenComparingLong(Long::longValue),
                null);
    }

    @Test
    public void thenComparingDouble() {
        checkSerializable(
                DistributedComparator.<Double>naturalOrder()
                        .thenComparingDouble(Double::doubleValue),
                null);
    }

    @Test
    public void nullsFirst() {
        checkSerializable(
                DistributedComparator.<Comparable>nullsFirst(Comparable::compareTo),
                null);
    }

    @Test
    public void nullsLast() {
        checkSerializable(
                DistributedComparator.<Comparable>nullsLast(Comparable::compareTo),
                null);
    }

    @Test
    public void comparing_keyExtractor() {
        checkSerializable(DistributedComparator.comparing(Object::toString), null);
    }

    @Test
    public void comparing_keyExtractor_keyComparator() {
        checkSerializable(DistributedComparator.comparing(Object::toString, String::compareTo), null);
    }

    @Test
    public void comparingInt() {
        checkSerializable(DistributedComparator.comparingInt(Object::hashCode), null);
    }

    @Test
    public void comparingLong() {
        checkSerializable(DistributedComparator.comparingLong(Long::longValue), null);
    }

    @Test
    public void comparingDouble() {
        checkSerializable(DistributedComparator.comparingDouble(Double::doubleValue), null);
    }

}
