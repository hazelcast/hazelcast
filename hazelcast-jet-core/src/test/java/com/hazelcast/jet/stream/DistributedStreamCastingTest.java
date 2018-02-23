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

package com.hazelcast.jet.stream;


import com.hazelcast.jet.IListJet;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class DistributedStreamCastingTest extends AbstractStreamTest {

    private Stream<Integer> stream;

    @Before
    public void setUp() {
        IListJet<Integer> list = getList();
        stream = DistributedStream.fromList(list);
    }

    @Test(expected = IllegalArgumentException.class)
    public void map_when_notSerializable_then_fail() {
        stream.map(m -> m);
    }

    @Test
    public void map_when_notDistributedButSerializable_then_proceed() {
        stream.map((Serializable & java.util.function.UnaryOperator<Integer>) m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMap_when_notSerializable_then_fail() {
        stream.flatMap(Stream::of);
    }

    @Test
    public void flatMap_when_notDistributedButSerializable_then_proceed() {
        stream.flatMap((Serializable & Function<Integer, Stream<? extends Integer>>) Stream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void collect_when_notSerializable_then_fail() {
        stream.collect(Collectors.counting());
    }

    @Test(expected = IllegalArgumentException.class)
    public void collect2_when_notSerializable_then_fail() {
        stream.collect(() -> new Integer[]{0},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test
    public void collect2_when_notDistributedButSerializable_then_proceed() {
        stream.collect((Serializable & java.util.function.Supplier<Integer[]>) () -> new Integer[]{0},
                (Serializable & java.util.function.BiConsumer<Integer[], Integer>) (r, e) -> r[0] += e,
                (Serializable & java.util.function.BiConsumer<Integer[], Integer[]>) (a, b) -> a[0] += b[0]);
    }

    public void forEach_when_notSerializable_then_proceed() {
        // here, non-serializable should be allowed
        java.util.function.Consumer action = System.out::println;
        stream.forEach(action);
    }

    public void forEachOrdered_when_notSerializable_then_proceed() {
        // here, non-serializable should be allowed
        java.util.function.Consumer action = System.out::println;
        stream.forEachOrdered(action);
    }

    @Test(expected = IllegalArgumentException.class)
    public void allMatch_when_notSerializable_then_fail() {
        stream.allMatch(m -> true);
    }

    @Test
    public void allMatch_when_notDistributedButSerializable_then_proceed() {
        stream.allMatch((Serializable & java.util.function.Predicate<Integer>) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void anyMatch_when_notSerializable_then_fail() {
        stream.anyMatch(m -> true);
    }

    @Test
    public void anyMatch_when_notDistributedButSerializable_then_proceed() {
        stream.anyMatch((Serializable & java.util.function.Predicate<Integer>) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noneMatch_when_notSerializable_then_fail() {
        stream.noneMatch(m -> true);
    }

    @Test
    public void noneMatch_when_notDistributedButSerializable_then_proceed() {
        stream.noneMatch((Serializable & java.util.function.Predicate<Integer>) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void filter_when_notSerializable_then_fail() {
        stream.filter(m -> true);
    }

    @Test
    public void filter_anyMatch_when_notDistributedButSerializable_then_proceed() {
        stream.filter((Serializable & java.util.function.Predicate<Integer>) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToInt_when_notSerializable_then_fail() {
        stream.mapToInt(m -> m);
    }

    @Test
    public void mapToInt_when_notDistributedButSerializable_then_proceed() {
        stream.mapToInt((Serializable & java.util.function.ToIntFunction<Integer>) m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToDouble_when_notSerializable_then_fail() {
        stream.mapToDouble(m -> (double) m);
    }

    @Test
    public void mapToDouble_when_notDistributedButSerializable_then_proceed() {
        stream.mapToDouble((Serializable & java.util.function.ToDoubleFunction<Integer>) m -> (double) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToLong_when_notSerializable_then_fail() {
        stream.mapToLong(m -> (long) m);
    }

    @Test
    public void mapToLong_when_notDistributedButSerializable_then_proceed() {
        stream.mapToLong((Serializable & java.util.function.ToLongFunction<Integer>) m -> (long) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMapToInt_when_notSerializable_then_fail() {
        stream.flatMapToInt(IntStream::of);
    }

    @Test
    public void flatMapToInt_when_notDistributedButSerializable_then_proceed() {
        stream.flatMapToInt((Serializable & Function<Integer, IntStream>) IntStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMapToDouble_when_notSerializable_then_fail() {
        stream.flatMapToDouble(DoubleStream::of);
    }

    @Test
    public void flatMapToDouble_when_notDistributedButSerializable_then_proceed() {
        stream.flatMapToDouble((Serializable & Function<Integer, DoubleStream>) DoubleStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMapToLong_when_notSerializable_then_fail() {
        stream.flatMapToLong(LongStream::of);
    }

    @Test
    public void flatMapToLong_when_notDistributedButSerializable_then_proceed() {
        stream.flatMapToLong((Serializable & Function<Integer, LongStream>) LongStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void max_when_notSerializable_then_fail() {
        java.util.Comparator<Integer> c = Integer::compareTo;
        stream.max(c);
    }

    @Test
    public void max_when_notDistributedButSerializable_then_proceed() {
        stream.max((Serializable & java.util.Comparator<Integer>) Integer::compareTo);
    }

    @Test(expected = IllegalArgumentException.class)
    public void min_when_notSerializable_then_fail() {
        java.util.Comparator<Integer> c = Integer::compareTo;
        stream.min(c);
    }

    @Test
    public void min_when_notDistributedButSerializable_then_proceed() {
        stream.min((Serializable & java.util.Comparator<Integer>) Integer::compareTo);
    }

    @Test(expected = IllegalArgumentException.class)
    public void peek_when_notSerializable_then_fail() {
        stream.peek(System.out::println);
    }

    @Test
    public void peek_when_notDistributedButSerializable_then_proceed() {
        stream.peek((Serializable & java.util.function.Consumer<Integer>) (x) -> System.out.println(x));
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce_when_notSerializable_then_fail() {
        stream.reduce((l, r) -> l + r);
    }

    @Test
    public void reduce_when_notDistributedButSerializable_then_proceed() {
        stream.reduce((Serializable & java.util.function.BinaryOperator<Integer>) (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce2_when_notSerializable_then_fail() {
        stream.reduce(0, (l, r) -> l + r);
    }

    @Test
    public void reduce2_when_notDistributedButSerializable_then_proceed() {
        stream.reduce(0, (Serializable & java.util.function.BinaryOperator<Integer>) (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce3_when_notSerializable_then_fail() {
        stream.reduce(0, (l, r) -> l + r, (l, r) -> l + r);
    }

    @Test
    public void reduce3_when_notDistributedButSerializable_then_proceed() {
        stream.reduce(0,
                (Serializable & java.util.function.BiFunction<Integer, Integer, Integer>) (l, r) -> l + r,
                (Serializable & java.util.function.BinaryOperator<Integer>) (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sorted_when_notSerializable_then_fail() {
        Comparator<Integer> c = Integer::compareTo;
        stream.sorted(c);
    }

    @Test
    public void sorted_when_notDistributedButSerializable_then_proceed() {
        stream.sorted((Serializable & Comparator<Integer>) Integer::compareTo);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sorted_when_serializableButNotReally_then_fail() {
        Comparator<Integer> comparator = Comparator.comparing(Object::toString);
        assertTrue(comparator instanceof Serializable);
        // Comparator.comparing returns serializable instance, however, its keyExtractor is not, so it should fail here
        stream.sorted(comparator);
    }
}
