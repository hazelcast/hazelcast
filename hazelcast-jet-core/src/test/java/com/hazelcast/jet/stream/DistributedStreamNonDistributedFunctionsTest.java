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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
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
        IListJet<Integer> list = getList();
        stream = DistributedStream.fromList(list);
    }

    @Test
    public void map() {
        stream.map((Function<Integer, String> & Serializable) Object::toString);
    }

    @Test
    public void flatMap() {
        stream.flatMap((Function<Integer, Stream<? extends Integer>> & Serializable) Stream::of);
    }

    @Test
    public void collect2() {
        stream.collect(
                (Supplier<Integer[]> & Serializable) () -> new Integer[]{0},
                (BiConsumer<Integer[], Integer> & Serializable) (r, e) -> r[0] += e,
                (BiConsumer<Integer[], Integer[]> & Serializable) (a, b) -> a[0] += b[0]);
    }

    @Test
    public void forEach() {
        stream.forEach((Consumer<Integer> & Serializable) v -> { });
    }

    @Test
    public void forEachOrdered() {
        stream.forEachOrdered((Consumer<Integer> & Serializable) v -> { });
    }

    @Test
    public void allMatch() {
        stream.allMatch((Predicate<Integer> & Serializable) m -> true);
    }

    @Test
    public void anyMatch() {
        stream.anyMatch((Predicate<Integer> & Serializable) m -> true);
    }

    @Test
    public void noneMatch() {
        stream.noneMatch((Predicate<Integer> & Serializable) m -> true);
    }

    @Test
    public void filter() {
        stream.filter((Predicate<Integer> & Serializable) m -> true);
    }

    @Test
    public void mapToInt() {
        stream.mapToInt((ToIntFunction<Integer> & Serializable) m -> m);
    }

    @Test
    public void mapToDouble() {
        stream.mapToDouble((ToDoubleFunction<Integer> & Serializable) m -> m);
    }

    @Test
    public void mapToLong() {
        stream.mapToLong((ToLongFunction<Integer> & Serializable) m -> (long) m);
    }

    @Test
    public void flatMapToInt() {
        stream.flatMapToInt((Function<Integer, IntStream> & Serializable) IntStream::of);
    }

    @Test
    public void flatMapToDouble() {
        stream.flatMapToDouble((Function<Integer, DoubleStream> & Serializable) DoubleStream::of);
    }

    @Test
    public void flatMapToLong() {
        stream.flatMapToLong((Function<Integer, LongStream> & Serializable) LongStream::of);
    }

    @Test
    public void max() {
        stream.max((Comparator<Integer> & Serializable) Integer::compareTo);
    }

    @Test
    public void min() {
        stream.min((Comparator<Integer> & Serializable) Integer::compareTo);
    }

    @Test
    public void peek() {
        stream.peek((Consumer<Integer> & Serializable) v -> { });
    }

    @Test
    public void reduce() {
        stream.reduce((BinaryOperator<Integer> & Serializable) (l, r) -> l + r);
    }

    @Test
    public void reduce2() {
        stream.reduce(0, (BinaryOperator<Integer> & Serializable) (l, r) -> l + r);
    }

    @Test
    public void reduce3() {
        stream.reduce(
                0,
                (BiFunction<Integer, Integer, Integer> & Serializable) (l, r) -> l + r,
                (BinaryOperator<Integer> & Serializable) (l1, r1) -> l1 + r1);
    }

    @Test
    public void sorted() {
        stream.sorted((Comparator<Integer> & Serializable) Integer::compareTo);
    }
}
