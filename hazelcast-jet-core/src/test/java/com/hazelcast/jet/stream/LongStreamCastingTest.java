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
import java.util.stream.LongStream;

public class LongStreamCastingTest extends AbstractStreamTest {

    private LongStream stream;

    @Before
    public void setUp() {
        IListJet<Integer> list = getList();
        stream = DistributedStream.fromList(list).mapToLong(m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void map_when_notSerializable_then_fail() {
        stream.map(m -> m);
    }

    @Test
    public void map_notDistributedButSerializable_then_proceed() {
        stream.map((Serializable & java.util.function.LongUnaryOperator) m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void flatMap_when_notSerializable_then_fail() {
        stream.flatMap(LongStream::of);
    }

    @Test
    public void flatMap_notDistributedButSerializable_then_proceed() {
        stream.flatMap((Serializable & java.util.function.LongFunction<LongStream>) LongStream::of);
    }

    @Test(expected = IllegalArgumentException.class)
    public void collect_when_notSerializable_then_fail() {
        stream.collect(() -> new Long[]{0L},
                (r, e) -> r[0] += e,
                (a, b) -> a[0] += b[0]);
    }

    @Test
    public void collect_notDistributedButSerializable_then_proceed() {
        stream.collect((Serializable & java.util.function.Supplier<Long[]>) () -> new Long[]{0L},
                (Serializable & java.util.function.ObjLongConsumer<Long[]>) (r, e) -> r[0] += e,
                (Serializable & java.util.function.BiConsumer<Long[], Long[]>) (a, b) -> a[0] += b[0]);
    }

    public void forEach_when_notSerializable_then_proceed() {
        // here, non-serializable should be allowed
        java.util.function.LongConsumer action = System.out::println;
        stream.forEach(action);
    }

    public void forEachOrdered_when_notSerializable_then_proceed() {
        // here, non-serializable should be allowed
        java.util.function.LongConsumer action = System.out::println;
        stream.forEachOrdered(action);
    }

    @Test(expected = IllegalArgumentException.class)
    public void allMatch_when_notSerializable_then_fail() {
        stream.allMatch(m -> true);
    }

    @Test
    public void allMatch_notDistributedButSerializable_then_proceed() {
        stream.allMatch((Serializable & java.util.function.LongPredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void anyMatch_when_notSerializable_then_fail() {
        stream.anyMatch(m -> true);
    }

    @Test
    public void anyMatch_notDistributedButSerializable_then_proceed() {
        stream.anyMatch((Serializable & java.util.function.LongPredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noneMatch_when_notSerializable_then_fail() {
        stream.noneMatch(m -> true);
    }

    @Test
    public void noneMatch_notDistributedButSerializable_then_proceed() {
        stream.noneMatch((Serializable & java.util.function.LongPredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void filter_when_notSerializable_then_fail() {
        stream.filter(m -> true);
    }

    @Test
    public void filter_anyMatch_notDistributedButSerializable_then_proceed() {
        stream.filter((Serializable & java.util.function.LongPredicate) m -> true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToObj_when_notSerializable_then_fail() {
        stream.mapToObj(m -> m);
    }

    @Test
    public void mapToObj_notDistributedButSerializable_then_proceed() {
        stream.mapToObj((Serializable & java.util.function.LongFunction<Long>) m -> m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToDouble_when_notSerializable_then_fail() {
        stream.mapToDouble(m -> (double) m);
    }

    @Test
    public void mapToDouble_notDistributedButSerializable_then_proceed() {
        stream.mapToDouble((Serializable & java.util.function.LongToDoubleFunction) m -> (double) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapToInt_when_notSerializable_then_fail() {
        stream.mapToInt(m -> (int) m);
    }

    @Test
    public void mapToInt_notDistributedButSerializable_then_proceed() {
        stream.mapToInt((Serializable & java.util.function.LongToIntFunction) m -> (int) m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void peek_when_notSerializable_then_fail() {
        stream.peek(System.out::println);
    }

    @Test
    public void peek_notDistributedButSerializable_then_proceed() {
        stream.peek((Serializable & java.util.function.LongConsumer) (x) -> System.out.println(x));
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce_when_notSerializable_then_fail() {
        stream.reduce((l, r) -> l + r);
    }

    @Test
    public void reduce_notDistributedButSerializable_then_proceed() {
        stream.reduce((Serializable & java.util.function.LongBinaryOperator) (l, r) -> l + r);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reduce2_when_notSerializable_then_fail() {
        stream.reduce(0, (l, r) -> l + r);
    }

    @Test
    public void reduce2_notDistributedButSerializable_then_proceed() {
        stream.reduce(0, (Serializable & java.util.function.LongBinaryOperator) (l, r) -> l + r);
    }

}
