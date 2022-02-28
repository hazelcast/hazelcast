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

package com.hazelcast.jet.impl.util;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.impl.util.Util.addClamped;
import static com.hazelcast.jet.impl.util.Util.addOrIncrementIndexInName;
import static com.hazelcast.jet.impl.util.Util.createFieldProjection;
import static com.hazelcast.jet.impl.util.Util.distinctBy;
import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.impl.util.Util.formatJobDuration;
import static com.hazelcast.jet.impl.util.Util.gcd;
import static com.hazelcast.jet.impl.util.Util.memoizeConcurrent;
import static com.hazelcast.jet.impl.util.Util.roundRobinPart;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UtilTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_addClamped_then_doesNotOverflow() {
        // no overflow
        assertEquals(0, addClamped(0, 0));
        assertEquals(1, addClamped(1, 0));
        assertEquals(-1, addClamped(-1, 0));
        assertEquals(-1, addClamped(Long.MAX_VALUE, Long.MIN_VALUE));
        assertEquals(-1, addClamped(Long.MIN_VALUE, Long.MAX_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, addClamped(Long.MAX_VALUE, Long.MAX_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, -1));
        assertEquals(Long.MIN_VALUE, addClamped(Long.MIN_VALUE, Long.MIN_VALUE));
    }

    @Test
    public void when_subtractClamped_then_doesNotOverflow() {
        // no overflow
        assertEquals(0, subtractClamped(0, 0));
        assertEquals(1, subtractClamped(1, 0));
        assertEquals(-1, subtractClamped(-1, 0));
        assertEquals(0, subtractClamped(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(0, subtractClamped(Long.MIN_VALUE, Long.MIN_VALUE));

        // overflow over MAX_VALUE
        assertEquals(Long.MAX_VALUE, subtractClamped(Long.MAX_VALUE, -1));
        assertEquals(Long.MAX_VALUE, subtractClamped(Long.MAX_VALUE, Long.MIN_VALUE));

        // overflow over MIN_VALUE
        assertEquals(Long.MIN_VALUE, subtractClamped(Long.MIN_VALUE, 1));
        assertEquals(Long.MIN_VALUE, subtractClamped(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Test
    public void when_memoizeConcurrent_then_threadSafe() {
        final Object obj = new Object();
        Supplier<Object> supplier = new Supplier<Object>() {
            boolean supplied;

            @Override
            public Object get() {
                if (supplied) {
                    throw new IllegalStateException("Supplier was already called once.");
                }
                supplied = true;
                return obj;
            }
        };

        // does not fail 100% with non-concurrent memoize, but about 50% of the time.
        List<Object> list = Stream.generate(memoizeConcurrent(supplier)).limit(4).parallel().collect(Collectors.toList());
        assertTrue("Not all objects matched expected", list.stream().allMatch(o -> o.equals(obj)));
    }

    @Test(expected = NullPointerException.class)
    public void when_memoizeConcurrentWithNullSupplier_then_exception() {
        Supplier<Object> supplier = () -> null;
        memoizeConcurrent(supplier).get();
    }

    @Test
    public void test_calculateGcd2() {
        assertEquals(2, gcd(0L, 2L));
        assertEquals(1, gcd(1L, 2L));
        assertEquals(2, gcd(2L, 4L));
        assertEquals(2, gcd(-2L, 4L));
    }

    @Test
    public void test_calculateGcdN() {
        assertEquals(0, gcd());
        assertEquals(4, gcd(4, 4, 4));
        assertEquals(4, gcd(4, 8, 12));
        assertEquals(1, gcd(4, 8, 13));
    }

    @Test
    public void test_addIndexToName() {
        assertEquals("a-2", addOrIncrementIndexInName("a"));
        assertEquals("a-3", addOrIncrementIndexInName("a-2"));
        assertEquals("a-26", addOrIncrementIndexInName("a-25"));
        assertEquals("a-25x-2", addOrIncrementIndexInName("a-25x"));
        assertEquals("a-1351318168168168168168-2", addOrIncrementIndexInName("a-1351318168168168168168"));
        assertEquals("a-" + Integer.MAX_VALUE + "-2", addOrIncrementIndexInName("a-" + Integer.MAX_VALUE));
        assertEquals("a-0-2", addOrIncrementIndexInName("a-0"));
        assertEquals("a-1-2", addOrIncrementIndexInName("a-1"));
        assertEquals("a-1-3", addOrIncrementIndexInName("a-1-2"));
        assertEquals("a--1-2", addOrIncrementIndexInName("a--1"));
    }

    @Test
    public void test_roundRobinPart() {
        assertArrayEquals(new int[] {},
                roundRobinPart(0, 2, 0));
        assertArrayEquals(new int[] {0},
                roundRobinPart(1, 1, 0));
        assertArrayEquals(new int[] {0},
                roundRobinPart(1, 2, 0));
        assertArrayEquals(new int[] {},
                roundRobinPart(1, 2, 1));
        assertArrayEquals(new int[] {0, 1},
                roundRobinPart(2, 1, 0));
        assertArrayEquals(new int[] {0},
                roundRobinPart(2, 2, 0));
        assertArrayEquals(new int[] {1},
                roundRobinPart(2, 2, 1));
        assertArrayEquals(new int[] {0, 2},
                roundRobinPart(3, 2, 0));
        assertArrayEquals(new int[] {1},
                roundRobinPart(3, 2, 1));
    }

    @Test
    public void test_minGuarantee() {
        assertEquals(NONE, Util.min(NONE, AT_LEAST_ONCE));
        assertEquals(AT_LEAST_ONCE, Util.min(AT_LEAST_ONCE, EXACTLY_ONCE));
        assertEquals(NONE, Util.min(NONE, EXACTLY_ONCE));
        assertEquals(NONE, Util.min(NONE, NONE));
    }

    @Test
    public void whenNullToCheckSerializable_thenReturnNull() {
        Object returned = Util.checkSerializable(null, "object");
        assertThat(returned).isNull();
    }

    @Test
    public void whenSerializableObjectToCheckSerializable_thenReturnObject() {
        Object o = "o";
        Object returned = Util.checkSerializable(o, "object");
        assertThat(returned).isSameAs(o);
    }

    @Test
    public void whenNonSerializableObjectToCheckSerializable_thenThrowException() {
        assertThatThrownBy(() -> Util.checkSerializable(new HashMap<>().entrySet(), "object"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("\"object\" must implement Serializable");
    }

    @Test
    public void whenNullToCheckNonNullAndSerializable_thenThrowException() {
        assertThatThrownBy(() -> Util.checkNonNullAndSerializable(null, "object"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("\"object\" must not be null");
    }

    @Test
    public void whenNonSerializableToCheckNonNullAndSerializable_thenThrowException() {
        assertThatThrownBy(() -> Util.checkNonNullAndSerializable(new HashMap<>().entrySet(), "object"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("\"object\" must implement Serializable");
    }

    @Test
    public void whenObjectToCheckNonNullAndSerializable_thenReturnObject() {
        String s = "s";
        String returned = Util.checkNonNullAndSerializable(s, "s");
        assertThat(returned).isSameAs(s);
    }

    @Test
    public void test_formatJobDuration() {
        assertEquals("20:19:02.855", formatJobDuration(73_142_855));
        assertEquals("00:00:00.120", formatJobDuration(120));
        assertEquals("00:00:05.120", formatJobDuration(5120));
        assertEquals("13d 13:52:22.855", formatJobDuration(1173_142_855));
        assertEquals("2d 00:05:42.855", formatJobDuration(173_142_855));
        assertEquals("00:12:22.855", formatJobDuration(742_855));
        assertEquals("106751991167d 07:12:55.807", formatJobDuration(Long.MAX_VALUE));
        assertEquals("-9223372036854775808", formatJobDuration(Long.MIN_VALUE));
    }

    @Test
    public void test_assignPartitions() throws UnknownHostException {
        Map<Address, List<Integer>> partitionsByMember = Util.assignPartitions(
                new LinkedHashSet<>(asList(new Address("localhost", 5701), new Address("localhost", 5702))),
                ImmutableMap.of(
                        new Address("localhost", 5701), singletonList(1),
                        new Address("localhost", 5702), singletonList(2),
                        new Address("localhost", 5703), singletonList(3),
                        new Address("localhost", 5704), singletonList(4),
                        new Address("localhost", 5705), singletonList(5)
                )
        );

        assertEquals(2, partitionsByMember.size());
        assertEquals(asList(1, 3, 5), partitionsByMember.get(new Address("localhost", 5701)));
        assertEquals(asList(2, 4), partitionsByMember.get(new Address("localhost", 5702)));
    }

    @Test
    public void test_createFieldProjection() {
        Function<String[], String[]> fieldProjection =
                createFieldProjection(new String[]{"c", "a", "d", "a"}, asList("a", "b", "c"));
        assertArrayEquals(new String[]{"a", null, "c"}, fieldProjection.apply(new String[]{"c", "a", "d"}));
    }

    @Test
    public void test_distributeObjects() {
        // count == 1
        assertArrayEquals(
                new int[][]{
                        new int[]{}},
                distributeObjects(1, new int[]{}));
        assertArrayEquals(
                new int[][]{
                        new int[]{2}},
                distributeObjects(1, new int[]{2}));
        assertArrayEquals(
                new int[][]{
                        new int[]{2, 4}},
                distributeObjects(1, new int[]{2, 4}));

        // count == 3
        assertArrayEquals(
                new int[][]{
                        new int[]{2},
                        new int[]{},
                        new int[]{}},
                distributeObjects(3, new int[]{2}));
        assertArrayEquals(
                new int[][]{
                        new int[]{2},
                        new int[]{4},
                        new int[]{6}},
                distributeObjects(3, new int[]{2, 4, 6}));
        assertArrayEquals(
                new int[][]{
                        new int[]{2, 8},
                        new int[]{4},
                        new int[]{6}},
                distributeObjects(3, new int[]{2, 4, 6, 8}));
    }

    @Test
    public void test_distinctBy() {
        List<String> list = asList("alice", "adela", "ben");
        List<String> actual = list.stream()
                .filter(distinctBy(s -> s.charAt(0)))
                .collect(Collectors.toList());
        assertEquals(asList("alice", "ben"), actual);
    }
}
