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

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
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
        List<Integer> list = getList();
        stream = list.stream();
    }

    @Test(expected = IllegalArgumentException.class)
    public void map() {
        stream.map(Object::toString);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMap() {
        stream.flatMap(Stream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void collect() {
        stream.collect(Collectors.counting());
    }

    @Test(expected = IllegalArgumentException.class)
    public void collect2() {
        stream.collect(() -> new Integer[]{0},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forEach() {
        stream.forEach(System.out::println);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forEachOrdered() {
        stream.forEachOrdered(System.out::println);
    }

    @Test(expected = IllegalArgumentException.class)
    public void allMatch() {
        stream.allMatch(m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void anyMatch() {
        stream.anyMatch(m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noneMatch() {
        stream.noneMatch(m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void filter() {
        stream.filter(m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToInt() {
        stream.mapToInt(m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToDouble() {
        stream.mapToDouble(m -> (double) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToLong() {
        stream.mapToLong(m -> (long) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMapToInt() {
        stream.flatMapToInt(IntStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMapToDouble() {
        stream.flatMapToDouble(DoubleStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMapToLong() {
        stream.flatMapToLong(LongStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void max() {
        Comparator<Integer> c = Integer::compareTo;
        stream.max(c);
    }

    @Test(expected = IllegalArgumentException.class)
    public void min() {
        Comparator<Integer> c = Integer::compareTo;
        stream.min(c);
    }

    @Test(expected = IllegalArgumentException.class)
    public void peek() {
        stream.peek(System.out::println);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce() {
        stream.reduce((l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce2() {
        stream.reduce(0, (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce3() {
        stream.reduce(0, (l, r) -> l + r, (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sorted() {
        Comparator<Integer> c = Integer::compareTo;
        stream.sorted(c);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sorted_when_serializableButNotReally_then_fail() {
        Comparator<Integer> comparator = Comparator.comparing(Object::toString);
        assertTrue(comparator instanceof Serializable);
        // Comparator.comparing returns serializable instance, however, its keyExtractor is not, so it should fail here
        stream.sorted(comparator);
    }
}
