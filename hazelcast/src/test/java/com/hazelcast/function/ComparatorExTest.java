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

package com.hazelcast.function;

import com.hazelcast.query.impl.Comparables;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static com.hazelcast.function.ComparatorEx.nullsFirst;
import static com.hazelcast.function.ComparatorEx.nullsLast;
import static com.hazelcast.function.ComparatorsEx.NATURAL_ORDER;
import static com.hazelcast.function.ComparatorsEx.REVERSE_ORDER;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class ComparatorExTest {

    @Test
    public void when_reverseComparator() {
        assertSame(REVERSE_ORDER, NATURAL_ORDER.reversed());
        assertSame(NATURAL_ORDER, REVERSE_ORDER.reversed());
    }

    @Test
    public void when_reverseOrderComparator() {
        ComparatorEx c = REVERSE_ORDER;
        assertEquals(1, c.compare(1, 2));
        assertEquals(-1, c.compare(2, 1));
    }

    @Test
    public void when_nullsFirstComparator() {
        ComparatorEx c = nullsFirst(NATURAL_ORDER);
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void when_nullsLastComparator() {
        ComparatorEx c = nullsLast(NATURAL_ORDER);
        assertEquals(-1, c.compare(1, 2));
        assertEquals(1, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void when_nullsFirst_withoutWrapped() {
        ComparatorEx c = nullsFirst(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(1, c.compare(0, null));
        assertEquals(-1, c.compare(null, 0));
    }

    @Test
    public void when_nullsLast_withoutWrapped() {
        ComparatorEx c = nullsLast(null);
        assertEquals(0, c.compare(1, 2));
        assertEquals(0, c.compare(2, 1));
        assertEquals(-1, c.compare(0, null));
        assertEquals(1, c.compare(null, 0));
    }

    @Test
    public void testSerializable_naturalOrder() {
        checkSerializable(ComparatorEx.naturalOrder(), null);
    }

    @Test
    public void testSerializable_reverseOrder() {
        checkSerializable(ComparatorEx.reverseOrder(), null);
    }

    @Test
    public void testSerializable_thenComparing_keyExtractor() {
        checkSerializable(
                ComparatorEx.naturalOrder()
                            .thenComparing(Object::toString),
                null);
    }

    @Test
    public void testSerializable_thenComparing_otherComparator() {
        checkSerializable(
                ComparatorEx.naturalOrder()
                            .thenComparing(Comparables::compare),
                null);
    }

    @Test
    public void testSerializable_thenComparing_keyExtractor_keyComparator() {
        checkSerializable(
                ComparatorEx.naturalOrder()
                            .thenComparing(Object::toString, Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_thenComparingInt() {
        checkSerializable(
                ComparatorEx.naturalOrder()
                            .thenComparingInt(Object::hashCode),
                null);
    }

    @Test
    public void testSerializable_thenComparingLong() {
        checkSerializable(
                ComparatorEx.<Long>naturalOrder()
                        .thenComparingLong(Long::longValue),
                null);
    }

    @Test
    public void testSerializable_thenComparingDouble() {
        checkSerializable(
                ComparatorEx.<Double>naturalOrder()
                        .thenComparingDouble(Double::doubleValue),
                null);
    }

    @Test
    public void testSerializable_nullsFirst() {
        checkSerializable(
                ComparatorEx.<Comparable>nullsFirst(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_nullsLast() {
        checkSerializable(
                ComparatorEx.<Comparable>nullsLast(Comparable::compareTo),
                null);
    }

    @Test
    public void testSerializable_comparing_keyExtractor() {
        checkSerializable(ComparatorEx.comparing(Object::toString), null);
    }

    @Test
    public void testSerializable_comparing_keyExtractor_keyComparator() {
        checkSerializable(ComparatorEx.comparing(Object::toString, String::compareTo), null);
    }

    @Test
    public void testSerializable_comparingInt() {
        checkSerializable(ComparatorEx.comparingInt(Object::hashCode), null);
    }

    @Test
    public void testSerializable_comparingLong() {
        checkSerializable(ComparatorEx.comparingLong(Long::longValue), null);
    }

    @Test
    public void testSerializable_comparingDouble() {
        checkSerializable(ComparatorEx.comparingDouble(Double::doubleValue), null);
    }

    @Test
    public void testSerializableSingleton_naturalOrder()
            throws IOException, ClassNotFoundException {
        testSerializableSingletonIsSame(NATURAL_ORDER);
    }

    @Test
    public void testSerializableSingleton_reverseOrder()
            throws IOException, ClassNotFoundException {
        testSerializableSingletonIsSame(REVERSE_ORDER);
    }

    private void testSerializableSingletonIsSame(Object singleton)
            throws IOException, ClassNotFoundException {
        byte[] serialized;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            objectOutputStream.writeObject(singleton);
            objectOutputStream.flush();
            serialized = outputStream.toByteArray();
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);
             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            Object deserialized = objectInputStream.readObject();
            assertSame(singleton, deserialized);
        }
    }
}
