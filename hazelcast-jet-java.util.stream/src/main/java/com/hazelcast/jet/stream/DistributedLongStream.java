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

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public interface DistributedLongStream extends LongStream {

    DistributedLongStream filter(Distributed.LongPredicate predicate);

    DistributedLongStream map(Distributed.LongUnaryOperator mapper);

    <U> DistributedStream<U> mapToObj(Distributed.LongFunction<? extends U> mapper);

    DistributedIntStream mapToInt(Distributed.LongToIntFunction mapper);

    DistributedDoubleStream mapToDouble(Distributed.LongToDoubleFunction mapper);

    DistributedLongStream flatMap(Distributed.LongFunction<? extends LongStream> mapper);

    DistributedLongStream distinct();

    DistributedLongStream sorted();

    DistributedLongStream peek(Distributed.LongConsumer action);

    DistributedLongStream limit(long maxSize);

    DistributedLongStream skip(long n);

    void forEach(Distributed.LongConsumer action);

    void forEachOrdered(Distributed.LongConsumer action);

    @Override
    long[] toArray();

    long reduce(long identity, Distributed.LongBinaryOperator op);

    OptionalLong reduce(Distributed.LongBinaryOperator op);

    <R> R collect(Distributed.Supplier<R> supplier, Distributed.ObjLongConsumer<R> accumulator,
                  Distributed.BiConsumer<R, R> combiner);

    @Override
    long sum();

    @Override
    OptionalLong min();

    @Override
    OptionalLong max();

    @Override
    long count();

    @Override
    OptionalDouble average();

    @Override
    LongSummaryStatistics summaryStatistics();

    boolean anyMatch(Distributed.LongPredicate predicate);

    boolean allMatch(Distributed.LongPredicate predicate);

    boolean noneMatch(Distributed.LongPredicate predicate);

    @Override
    OptionalLong findFirst();

    @Override
    OptionalLong findAny();

    @Override
    DistributedDoubleStream asDoubleStream();

    @Override
    DistributedStream<Long> boxed();

    @Override
    DistributedLongStream sequential();

    @Override
    DistributedLongStream parallel();

    @Override
    PrimitiveIterator.OfLong iterator();

    @Override
    Spliterator.OfLong spliterator();

    @Override
    default LongStream filter(LongPredicate predicate) {
        return filter((Distributed.LongPredicate) predicate);
    }

    @Override
    default LongStream map(LongUnaryOperator mapper) {
        return map((Distributed.LongUnaryOperator) mapper);
    }

    @Override
    default <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
        return mapToObj((Distributed.LongFunction) mapper);
    }

    @Override
    default IntStream mapToInt(LongToIntFunction mapper) {
        return mapToInt((Distributed.LongToIntFunction) mapper);
    }

    @Override
    default DoubleStream mapToDouble(LongToDoubleFunction mapper) {
        return mapToDouble((Distributed.LongToDoubleFunction) mapper);
    }

    @Override
    default LongStream flatMap(LongFunction<? extends LongStream> mapper) {
        return flatMap((Distributed.LongFunction) mapper);
    }

    @Override
    default LongStream peek(LongConsumer action) {
        return peek((Distributed.LongConsumer) action);
    }

    @Override
    default void forEach(LongConsumer action) {
        forEach((Distributed.LongConsumer) action);
    }

    @Override
    default void forEachOrdered(LongConsumer action) {
        forEachOrdered((Distributed.LongConsumer) action);
    }

    @Override
    default long reduce(long identity, LongBinaryOperator op) {
        return reduce(identity, (Distributed.LongBinaryOperator) op);
    }

    @Override
    default OptionalLong reduce(LongBinaryOperator op) {
        return reduce((Distributed.LongBinaryOperator) op);
    }

    @Override
    default <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        return collect((Distributed.Supplier<R>) supplier,
                (Distributed.ObjLongConsumer<R>) accumulator,
                (Distributed.BiConsumer<R, R>) combiner);
    }

    @Override
    default boolean anyMatch(LongPredicate predicate) {
        return anyMatch((Distributed.LongPredicate) predicate);
    }

    @Override
    default boolean allMatch(LongPredicate predicate) {
        return allMatch((Distributed.LongPredicate) predicate);
    }

    @Override
    default boolean noneMatch(LongPredicate predicate) {
        return noneMatch((Distributed.LongPredicate) predicate);
    }
}
