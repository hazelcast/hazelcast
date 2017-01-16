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


import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class DistributedStreamCastingTest extends AbstractStreamTest {

    private Stream<Integer> stream;

    @Before
    public void setUp() {
        List<Integer> list = getList();
        stream = list.stream();
    }

    @Test(expected = ClassCastException.class)
    public void map() {
        stream.map(Object::toString);
    }

    @Test(expected = ClassCastException.class)
    public void flatMap() {
        stream.flatMap(Stream::of);
    }

    @Test(expected = ClassCastException.class)
    public void collect() {
        stream.collect(Collectors.counting());
    }

    @Test(expected = ClassCastException.class)
    public void collect2() {
        stream.collect(() -> new Integer[]{0},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test(expected = ClassCastException.class)
    public void forEach() {
        stream.forEach(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void forEachOrdered() {
        stream.forEachOrdered(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void allMatch() {
        stream.allMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void anyMatch() {
        stream.anyMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void noneMatch() {
        stream.noneMatch(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void filter() {
        stream.filter(m -> true);
    }

    @Test(expected = ClassCastException.class)
    public void mapToInt() {
        stream.mapToInt(m -> m);
    }

    @Test(expected = ClassCastException.class)
    public void mapToDouble() {
        stream.mapToDouble(m -> (double) m);
    }

    @Test(expected = ClassCastException.class)
    public void mapToLong() {
        stream.mapToLong(m -> (long) m);
    }

    @Test(expected = ClassCastException.class)
    public void flatMapToInt() {
        stream.flatMapToInt(IntStream::of);
    }

    @Test(expected = ClassCastException.class)
    public void flatMapToDouble() {
        stream.flatMapToDouble(DoubleStream::of);
    }

    @Test(expected = ClassCastException.class)
    public void flatMapToLong() {
        stream.flatMapToLong(LongStream::of);
    }

    @Test(expected = ClassCastException.class)
    public void max() {
        stream.max(Comparator.naturalOrder());
    }

    @Test(expected = ClassCastException.class)
    public void min() {
        stream.min(Comparator.naturalOrder());
    }

    @Test(expected = ClassCastException.class)
    public void peek() {
        stream.peek(System.out::println);
    }

    @Test(expected = ClassCastException.class)
    public void reduce() {
        stream.reduce((l, r) -> l + r);
    }

    @Test(expected = ClassCastException.class)
    public void reduce2() {
        stream.reduce(0, (l, r) -> l + r);
    }

    @Test(expected = ClassCastException.class)
    public void reduce3() {
        stream.reduce(0, (l, r) -> l + r, (l, r) -> l + r);
    }

    @Test(expected = ClassCastException.class)
    public void sorted() {
        stream.sorted(Comparator.naturalOrder());
    }
}
