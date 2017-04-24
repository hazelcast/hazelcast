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

package com.hazelcast.jet;

import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.stream.impl.StreamUtil.checkSerializable;

public class ComparatorTest {
    @Before
    public void setUp() {

    }

    @Test
    public void naturalOrder() {
        checkSerializable(Distributed.Comparator.naturalOrder(), null);
    }

    @Test
    public void reverseOrder() {
        checkSerializable(Distributed.Comparator.reverseOrder(), null);
    }

    @Test
    public void thenComparing_keyExtractor() {
        checkSerializable(
                Distributed.Comparator.naturalOrder()
                    .thenComparing(Object::toString),
                null);
    }

    @Test
    public void thenComparing_otherComparator() {
        checkSerializable(
                Distributed.Comparator.naturalOrder()
                                      .thenComparing(Comparable::compareTo),
                null);
    }

    @Test
    public void thenComparing_keyExtractor_keyComparator() {
        checkSerializable(
                Distributed.Comparator.naturalOrder()
                                      .thenComparing(Object::toString, Comparable::compareTo),
                null);
    }

    @Test
    public void thenComparingInt() {
        checkSerializable(
                Distributed.Comparator.naturalOrder()
                                      .thenComparingInt(Object::hashCode),
                null);
    }

    @Test
    public void thenComparingLong() {
        checkSerializable(
                Distributed.Comparator.<Long>naturalOrder()
                                      .thenComparingLong(Long::longValue),
                null);
    }

    @Test
    public void thenComparingDouble() {
        checkSerializable(
                Distributed.Comparator.<Double>naturalOrder()
                        .thenComparingDouble(Double::doubleValue),
                null);
    }

    @Test
    public void nullsFirst() {
        checkSerializable(
                Distributed.Comparator.<Comparable>nullsFirst(Comparable::compareTo),
                null);
    }

    @Test
    public void nullsLast() {
        checkSerializable(
                Distributed.Comparator.<Comparable>nullsLast(Comparable::compareTo),
                null);
    }

    @Test
    public void comparing_keyExtractor() {
        checkSerializable(Distributed.Comparator.comparing(Object::toString), null);
    }

    @Test
    public void comparing_keyExtractor_keyComparator() {
        checkSerializable(Distributed.Comparator.comparing(Object::toString, String::compareTo), null);
    }

    @Test
    public void comparingInt() {
        checkSerializable(Distributed.Comparator.comparingInt(Object::hashCode), null);
    }

    @Test
    public void comparingLong() {
        checkSerializable(Distributed.Comparator.comparingLong(Long::longValue), null);
    }

    @Test
    public void comparingDouble() {
        checkSerializable(Distributed.Comparator.comparingDouble(Double::doubleValue), null);
    }

}
