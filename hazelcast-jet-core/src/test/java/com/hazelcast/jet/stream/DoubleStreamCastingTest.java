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
import java.util.stream.DoubleStream;

public class DoubleStreamCastingTest extends AbstractStreamTest {

    private DoubleStream stream;

    @Before
    public void setUp() {
        IStreamList<Integer> list = getList();
        stream = list.stream().mapToDouble(m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void map_when_notSerializable_then_fail() {
        stream.map(m -> m);
    }

    @Test
    public void map_notDistributedButSerializable_then_proceed() {
        stream.map((Serializable & java.util.function.DoubleUnaryOperator) m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMap_when_notSerializable_then_fail() {
        stream.flatMap(DoubleStream::of);
    }

    @Test
    public void flatMap_notDistributedButSerializable_then_proceed() {
        stream.flatMap((Serializable & java.util.function.DoubleFunction<DoubleStream>) DoubleStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void collect_when_notSerializable_then_fail() {
        stream.collect(() -> new Double[]{0.0},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test
    public void collect_notDistributedButSerializable_then_proceed() {
        stream.collect((Serializable & java.util.function.Supplier<Double[]>) () -> new Double[]{0.0},
                (Serializable & java.util.function.ObjDoubleConsumer<Double[]>) (r, e) -> r[0] += e,
                (Serializable & java.util.function.BiConsumer<Double[], Double[]>) (a, b) -> a[0] += b[0]);
    }

    public void forEach_when_notSerializable_then_proceed() {
        // here, non-serializable should be allowed
        java.util.function.DoubleConsumer action = System.out::println;
        stream.forEach(action);
    }

    public void forEachOrdered_when_notSerializable_then_proceed() {
        // here, non-serializable should be allowed
        java.util.function.DoubleConsumer action = System.out::println;
        stream.forEachOrdered(action);
    }

    @Test(expected = IllegalArgumentException.class)
    public void allMatch_when_notSerializable_then_fail() {
        stream.allMatch(m -> true);
    }

    @Test
    public void allMatch_notDistributedButSerializable_then_proceed() {
        stream.allMatch((Serializable & java.util.function.DoublePredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void anyMatch_when_notSerializable_then_fail() {
        stream.anyMatch(m -> true);
    }

    @Test
    public void anyMatch_notDistributedButSerializable_then_proceed() {
        stream.anyMatch((Serializable & java.util.function.DoublePredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noneMatch_when_notSerializable_then_fail() {
        stream.noneMatch(m -> true);
    }

    @Test
    public void noneMatch_notDistributedButSerializable_then_proceed() {
        stream.noneMatch((Serializable & java.util.function.DoublePredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void filter_when_notSerializable_then_fail() {
        stream.filter(m -> true);
    }

    @Test
    public void filter_anyMatch_notDistributedButSerializable_then_proceed() {
        stream.filter((Serializable & java.util.function.DoublePredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToObj_when_notSerializable_then_fail() {
        stream.mapToObj(m -> m);
    }

    @Test
    public void mapToObj_notDistributedButSerializable_then_proceed() {
        stream.mapToObj((Serializable & java.util.function.DoubleFunction<Double>) m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToInt_when_notSerializable_then_fail() {
        stream.mapToInt(m -> (int) m);
    }

    @Test
    public void mapToInt_notDistributedButSerializable_then_proceed() {
        stream.mapToInt((Serializable & java.util.function.DoubleToIntFunction) m -> (int) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToLong_when_notSerializable_then_fail() {
        stream.mapToLong(m -> (long) m);
    }

    @Test
    public void mapToLong_notDistributedButSerializable_then_proceed() {
        stream.mapToLong((Serializable & java.util.function.DoubleToLongFunction) m -> (long) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void peek_when_notSerializable_then_fail() {
        stream.peek(System.out::println);
    }

    @Test
    public void peek_notDistributedButSerializable_then_proceed() {
        stream.peek((Serializable & java.util.function.DoubleConsumer) (x) -> System.out.println(x));
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce_when_notSerializable_then_fail() {
        stream.reduce((l, r) -> l + r);
    }

    @Test
    public void reduce_notDistributedButSerializable_then_proceed() {
        stream.reduce((Serializable & java.util.function.DoubleBinaryOperator) (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce2_when_notSerializable_then_fail() {
        stream.reduce(0, (l, r) -> l + r);
    }

    @Test
    public void reduce2_notDistributedButSerializable_then_proceed() {
        stream.reduce(0, (Serializable & java.util.function.DoubleBinaryOperator) (l, r) -> l + r);
    }
}
