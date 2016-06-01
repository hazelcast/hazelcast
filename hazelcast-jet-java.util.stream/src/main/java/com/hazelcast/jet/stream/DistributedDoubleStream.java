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

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public interface DistributedDoubleStream extends DoubleStream {

    DistributedDoubleStream filter(Distributed.DoublePredicate predicate);

    DistributedDoubleStream map(Distributed.DoubleUnaryOperator mapper);

    <U> DistributedStream<U> mapToObj(Distributed.DoubleFunction<? extends U> mapper);

    DistributedLongStream mapToLong(Distributed.DoubleToLongFunction mapper);

    DistributedIntStream mapToInt(Distributed.DoubleToIntFunction mapper);

    DistributedDoubleStream flatMap(Distributed.DoubleFunction<? extends DoubleStream> mapper);

    DistributedDoubleStream distinct();

    DistributedDoubleStream sorted();

    DistributedDoubleStream peek(Distributed.DoubleConsumer action);

    DistributedDoubleStream limit(long maxSize);

    DistributedDoubleStream skip(long n);

    void forEach(Distributed.DoubleConsumer action);

    void forEachOrdered(Distributed.DoubleConsumer action);

    @Override
    double[] toArray();

    double reduce(double identity, Distributed.DoubleBinaryOperator op);

    OptionalDouble reduce(Distributed.DoubleBinaryOperator op);

    <R> R collect(Distributed.Supplier<R> supplier, Distributed.ObjDoubleConsumer<R> accumulator,
                  Distributed.BiConsumer<R, R> combiner);

    @Override
    double sum();

    @Override
    OptionalDouble min();

    @Override
    OptionalDouble max();

    @Override
    long count();

    @Override
    OptionalDouble average();

    @Override
    DoubleSummaryStatistics summaryStatistics();

    boolean anyMatch(Distributed.DoublePredicate predicate);

    boolean allMatch(Distributed.DoublePredicate predicate);

    boolean noneMatch(Distributed.DoublePredicate predicate);

    @Override
    OptionalDouble findFirst();

    @Override
    OptionalDouble findAny();

    @Override
    DistributedStream<Double> boxed();

    @Override
    DistributedDoubleStream sequential();

    @Override
    DistributedDoubleStream parallel();

    @Override
    PrimitiveIterator.OfDouble iterator();

    @Override
    Spliterator.OfDouble spliterator();

    @Override
    default DoubleStream filter(DoublePredicate predicate) {
        return filter((Distributed.DoublePredicate) predicate);
    }

    @Override
    default DoubleStream map(DoubleUnaryOperator mapper) {
        return map((Distributed.DoubleUnaryOperator) mapper);
    }

    @Override
    default <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
        return mapToObj((Distributed.DoubleFunction) mapper);
    }

    @Override
    default LongStream mapToLong(DoubleToLongFunction mapper) {
        return mapToLong((Distributed.DoubleToLongFunction) mapper);
    }

    @Override
    default IntStream mapToInt(DoubleToIntFunction mapper) {
        return mapToInt((Distributed.DoubleToIntFunction) mapper);
    }

    @Override
    default DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
        return flatMap((Distributed.DoubleFunction) mapper);
    }

    @Override
    default DoubleStream peek(DoubleConsumer action) {
        return peek((Distributed.DoubleConsumer) action);
    }

    @Override
    default void forEach(DoubleConsumer action) {
        forEach((Distributed.DoubleConsumer) action);
    }

    @Override
    default void forEachOrdered(DoubleConsumer action) {
        forEachOrdered((Distributed.DoubleConsumer) action);
    }

    @Override
    default double reduce(double identity, DoubleBinaryOperator op) {
        return reduce(identity, (Distributed.DoubleBinaryOperator) op);
    }

    @Override
    default OptionalDouble reduce(DoubleBinaryOperator op) {
        return reduce((Distributed.DoubleBinaryOperator) op);
    }

    @Override
    default <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        return collect((Distributed.Supplier<R>) supplier,
                (Distributed.ObjDoubleConsumer<R>) accumulator,
                (Distributed.BiConsumer<R, R>) combiner);
    }

    @Override
    default boolean anyMatch(DoublePredicate predicate) {
        return anyMatch((Distributed.DoublePredicate) predicate);
    }

    @Override
    default boolean allMatch(DoublePredicate predicate) {
        return allMatch((Distributed.DoublePredicate) predicate);
    }

    @Override
    default boolean noneMatch(DoublePredicate predicate) {
        return noneMatch((Distributed.DoublePredicate) predicate);
    }
}
