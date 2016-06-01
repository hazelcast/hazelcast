/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public interface DistributedIntStream extends IntStream {

    DistributedIntStream filter(Distributed.IntPredicate predicate);

    DistributedIntStream map(Distributed.IntUnaryOperator mapper);

    <U> DistributedStream<U> mapToObj(Distributed.IntFunction<? extends U> mapper);

    DistributedLongStream mapToLong(Distributed.IntToLongFunction mapper);

    DistributedDoubleStream mapToDouble(Distributed.IntToDoubleFunction mapper);

    DistributedIntStream flatMap(Distributed.IntFunction<? extends IntStream> mapper);

    DistributedIntStream distinct();

    DistributedIntStream sorted();

    DistributedIntStream peek(Distributed.IntConsumer action);

    DistributedIntStream limit(long maxSize);

    DistributedIntStream skip(long n);

    void forEach(Distributed.IntConsumer action);

    void forEachOrdered(Distributed.IntConsumer action);

    @Override
    int[] toArray();

    int reduce(int identity, Distributed.IntBinaryOperator op);

    OptionalInt reduce(Distributed.IntBinaryOperator op);

    <R> R collect(Distributed.Supplier<R> supplier, Distributed.ObjIntConsumer<R> accumulator,
                  Distributed.BiConsumer<R, R> combiner);

    @Override
    int sum();

    @Override
    OptionalInt min();

    @Override
    OptionalInt max();

    @Override
    long count();

    @Override
    OptionalDouble average();

    @Override
    IntSummaryStatistics summaryStatistics();

    boolean anyMatch(Distributed.IntPredicate predicate);

    boolean allMatch(Distributed.IntPredicate predicate);

    boolean noneMatch(Distributed.IntPredicate predicate);

    @Override
    OptionalInt findFirst();

    @Override
    OptionalInt findAny();

    @Override
    DistributedLongStream asLongStream();

    @Override
    DistributedDoubleStream asDoubleStream();

    @Override
    DistributedStream<Integer> boxed();

    @Override
    DistributedIntStream sequential();

    @Override
    DistributedIntStream parallel();

    @Override
    PrimitiveIterator.OfInt iterator();

    @Override
    Spliterator.OfInt spliterator();

    @Override
    default IntStream filter(IntPredicate predicate) {
        return filter((Distributed.IntPredicate) predicate);
    }

    @Override
    default IntStream map(IntUnaryOperator mapper) {
        return map((Distributed.IntUnaryOperator) mapper);
    }

    @Override
    default <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
        return mapToObj((Distributed.IntFunction) mapper);
    }

    @Override
    default LongStream mapToLong(IntToLongFunction mapper) {
        return mapToLong((Distributed.IntToLongFunction) mapper);
    }

    @Override
    default DoubleStream mapToDouble(IntToDoubleFunction mapper) {
        return mapToDouble((Distributed.IntToDoubleFunction) mapper);
    }

    @Override
    default IntStream flatMap(IntFunction<? extends IntStream> mapper) {
        return flatMap((Distributed.IntFunction) mapper);
    }

    @Override
    default IntStream peek(IntConsumer action) {
        return peek((Distributed.IntConsumer) action);
    }

    @Override
    default void forEach(IntConsumer action) {
        forEach((Distributed.IntConsumer) action);
    }

    @Override
    default void forEachOrdered(IntConsumer action) {
        forEachOrdered((Distributed.IntConsumer) action);
    }

    @Override
    default int reduce(int identity, IntBinaryOperator op) {
        return reduce(identity, (Distributed.IntBinaryOperator) op);
    }

    @Override
    default OptionalInt reduce(IntBinaryOperator op) {
        return reduce((Distributed.IntBinaryOperator) op);
    }

    @Override
    default <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        return collect((Distributed.Supplier<R>) supplier,
                (Distributed.ObjIntConsumer<R>) accumulator,
                (Distributed.BiConsumer<R, R>) combiner);
    }

    @Override
    default boolean anyMatch(IntPredicate predicate) {
        return anyMatch((Distributed.IntPredicate) predicate);
    }

    @Override
    default boolean allMatch(IntPredicate predicate) {
        return allMatch((Distributed.IntPredicate) predicate);
    }

    @Override
    default boolean noneMatch(IntPredicate predicate) {
        return noneMatch((Distributed.IntPredicate) predicate);
    }
}
