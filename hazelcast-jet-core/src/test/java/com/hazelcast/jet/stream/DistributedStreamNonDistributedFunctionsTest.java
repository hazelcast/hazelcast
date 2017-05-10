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


import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class DistributedStreamNonDistributedFunctionsTest extends AbstractStreamTest {

    private Stream<Integer> stream;

    @Before
    public void setUp() {
        List<Integer> list = getList();
        stream = list.stream();
    }

    @Test
    public void map() {
        DistributedFunction<Integer, String> f = (DistributedFunction<Integer, String> & Serializable) Object::toString;
        stream.map(f);
    }

    @Test
    public void flatMap() {
        java.util.function.Function<Integer, Stream<? extends Integer>> f =
                (java.util.function.Function<Integer, Stream<? extends Integer>> & Serializable) Stream::of;
        stream.flatMap(f);
    }

    @Test
    public void collect2() {
        Supplier<Integer[]> supplier = (Supplier<Integer[]> & Serializable) () -> new Integer[]{0};
        BiConsumer<Integer[], Integer> accumulator = (BiConsumer<Integer[], Integer> & Serializable) (r, e) -> r[0] += e;
        BiConsumer<Integer[], Integer[]> combiner = (BiConsumer<Integer[], Integer[]> & Serializable) (a, b) -> a[0] += b[0];
        stream.collect(supplier,
                accumulator,
                combiner);
    }

    @Test
    public void forEach() {
        Consumer<Integer> action = (Consumer<Integer> & Serializable) v -> {};
        stream.forEach(action);
    }

    @Test
    public void forEachOrdered() {
        Consumer<Integer> action = (Consumer<Integer> & Serializable) v -> {};
        stream.forEachOrdered(action);
    }

    @Test
    public void allMatch() {
        Predicate<Integer> predicate = (Predicate<Integer> & Serializable) m -> true;
        stream.allMatch(predicate);
    }

    @Test
    public void anyMatch() {
        Predicate<Integer> predicate = (Predicate<Integer> & Serializable) m -> true;
        stream.anyMatch(predicate);
    }

    @Test
    public void noneMatch() {
        Predicate<Integer> predicate = (Predicate<Integer> & Serializable) m -> true;
        stream.noneMatch(predicate);
    }

    @Test
    public void filter() {
        Predicate<Integer> predicate = (Predicate<Integer> & Serializable) m -> true;
        stream.filter(predicate);
    }

    @Test
    public void mapToInt() {
        ToIntFunction<Integer> mapper = (ToIntFunction<Integer> & Serializable) m -> m;
        stream.mapToInt(mapper);
    }

    @Test
    public void mapToDouble() {
        DistributedToDoubleFunction<Integer> mapper = (DistributedToDoubleFunction<Integer> & Serializable) m -> m;
        stream.mapToDouble(mapper);
    }

    @Test
    public void mapToLong() {
        ToLongFunction<Integer> mapper = (ToLongFunction<Integer> & Serializable) m -> (long) m;
        stream.mapToLong(mapper);
    }

    @Test
    public void flatMapToInt() {
        java.util.function.Function<Integer, IntStream> function = (java.util.function.Function<Integer, IntStream> & Serializable) IntStream::of;
        stream.flatMapToInt(function);
    }

    @Test
    public void flatMapToDouble() {
        java.util.function.Function<Integer, DoubleStream> function = (java.util.function.Function<Integer, DoubleStream> & Serializable) DoubleStream::of;
        stream.flatMapToDouble(function);
    }

    @Test
    public void flatMapToLong() {
        java.util.function.Function<Integer, LongStream> function = (java.util.function.Function<Integer, LongStream> & Serializable) LongStream::of;
        stream.flatMapToLong(function);
    }

    @Test
    public void max() {
        Comparator<Integer> comparator = (Comparator<Integer> & Serializable) Integer::compareTo;
        stream.max(comparator);
    }

    @Test
    public void min() {
        Comparator<Integer> comparator = (Comparator<Integer> & Serializable) Integer::compareTo;
        stream.min(comparator);
    }

    @Test
    public void peek() {
        Consumer<Integer> action = (Consumer<Integer> & Serializable) v -> {};
        stream.peek(action);
    }

    @Test
    public void reduce() {
        BinaryOperator<Integer> accumulator = (BinaryOperator<Integer> & Serializable) (l, r) -> l + r;
        stream.reduce(accumulator);
    }

    @Test
    public void reduce2() {
        BinaryOperator<Integer> accumulator = (BinaryOperator<Integer> & Serializable) (l, r) -> l + r;
        stream.reduce(0, accumulator);
    }

    @Test
    public void reduce3() {
        BiFunction<Integer, Integer, Integer> accumulator = (BiFunction<Integer, Integer, Integer> & Serializable) (l, r) -> l + r;
        BinaryOperator<Integer> combiner = (BinaryOperator<Integer> & Serializable) (l, r) -> l + r;
        stream.reduce(0, accumulator, combiner);
    }

    @Test
    public void sorted() {
        Comparator<Integer> comparator = (Comparator<Integer> & Serializable) Integer::compareTo;
        stream.sorted(comparator);
    }
}
