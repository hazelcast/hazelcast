/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedIntBinaryOperator;
import com.hazelcast.jet.function.DistributedIntConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.function.DistributedIntPredicate;
import com.hazelcast.jet.function.DistributedIntToDoubleFunction;
import com.hazelcast.jet.function.DistributedIntToLongFunction;
import com.hazelcast.jet.function.DistributedIntUnaryOperator;
import com.hazelcast.jet.function.DistributedObjIntConsumer;
import com.hazelcast.jet.function.DistributedSupplier;

import java.util.OptionalInt;
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
import java.util.stream.IntStream;

/**
 * An extension of {@link IntStream} that supports distributed stream
 * operations by replacing functional interfaces with their serializable
 * equivalents.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedIntStream extends IntStream {

    /**
     * {@code Serializable} variant of {@link
     * IntStream#filter(IntPredicate) java.util.stream.IntStream#filter(IntPredicate)}.
     */
    default DistributedIntStream filter(DistributedIntPredicate predicate) {
        return filter((IntPredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#map(IntUnaryOperator) java.util.stream.IntStream#map(IntUnaryOperator)}.
     */
    default DistributedIntStream map(DistributedIntUnaryOperator mapper) {
        return map((IntUnaryOperator) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#mapToObj(IntFunction) java.util.stream.IntStream#mapToObj(IntFunction)}.
     */
    default <U> DistributedStream<U> mapToObj(DistributedIntFunction<? extends U> mapper) {
        return mapToObj((IntFunction<? extends U>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#mapToLong(IntToLongFunction)
     * java.util.stream.IntStream#mapToLong(IntToLongFunction)}.
     */
    default DistributedLongStream mapToLong(DistributedIntToLongFunction mapper) {
        return mapToLong((IntToLongFunction) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#mapToDouble(IntToDoubleFunction)
     * java.util.stream.IntStream#mapToDouble(IntToDoubleFunction)}.
     */
    default DistributedDoubleStream mapToDouble(DistributedIntToDoubleFunction mapper) {
        return mapToDouble((IntToDoubleFunction) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#flatMap(IntFunction)
     * java.util.stream.IntStream#flatMap(IntFunction)}.
     */
    default DistributedIntStream flatMap(DistributedIntFunction<? extends IntStream> mapper) {
        return flatMap((IntFunction<? extends IntStream>) mapper);
    }

    @Override
    DistributedIntStream distinct();

    @Override
    DistributedIntStream sorted();

    /**
     * {@code Serializable} variant of
     * {@link IntStream#peek(IntConsumer)
     * java.util.stream.IntStream#peek(IntConsumer)}.
     */
    default DistributedIntStream peek(DistributedIntConsumer action) {
        return peek((IntConsumer) action);
    }

    @Override
    DistributedIntStream limit(long maxSize);

    @Override
    DistributedIntStream skip(long n);

    /**
     * {@code Serializable} variant of
     * {@link IntStream#reduce(int, IntBinaryOperator)
     * java.util.stream.IntStream#reduce(int, IntBinaryOperator)}.
     */
    default int reduce(int identity, DistributedIntBinaryOperator op) {
        return reduce(identity, (IntBinaryOperator) op);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#reduce(IntBinaryOperator)
     * java.util.stream.IntStream#reduce(IntBinaryOperator)}.
     */
    default OptionalInt reduce(DistributedIntBinaryOperator op) {
        return reduce((IntBinaryOperator) op);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#collect(Supplier, ObjIntConsumer, BiConsumer)
     * java.util.stream.IntStream#collect(Supplier, ObjIntConsumer, BiConsumer)}.
     */
    default <R> R collect(DistributedSupplier<R> supplier, DistributedObjIntConsumer<R> accumulator,
                          DistributedBiConsumer<R, R> combiner) {
        return collect((Supplier<R>) supplier, accumulator, combiner);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#anyMatch(IntPredicate)
     * java.util.stream.IntStream#anyMatch(IntPredicate)}.
     */
    default boolean anyMatch(DistributedIntPredicate predicate) {
        return anyMatch((IntPredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#allMatch(IntPredicate) java.util.stream.IntStream#allMatch(IntPredicate)}.
     */
    default boolean allMatch(DistributedIntPredicate predicate) {
        return allMatch((IntPredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link IntStream#noneMatch(IntPredicate)
     * java.util.stream.IntStream#noneMatch(IntPredicate)}.
     */
    default boolean noneMatch(DistributedIntPredicate predicate) {
        return noneMatch((IntPredicate) predicate);
    }

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
    DistributedIntStream filter(IntPredicate predicate);

    @Override
    DistributedIntStream map(IntUnaryOperator mapper);

    @Override
    <U> DistributedStream<U> mapToObj(IntFunction<? extends U> mapper);

    @Override
    DistributedLongStream mapToLong(IntToLongFunction mapper);

    @Override
    DistributedDoubleStream mapToDouble(IntToDoubleFunction mapper);

    @Override
    DistributedIntStream flatMap(IntFunction<? extends IntStream> mapper);

    @Override
    DistributedIntStream peek(IntConsumer action);

    @Override
    int reduce(int identity, IntBinaryOperator op);

    @Override
    OptionalInt reduce(IntBinaryOperator op);

    @Override
    <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner);

    @Override
    boolean anyMatch(IntPredicate predicate);

    @Override
    boolean allMatch(IntPredicate predicate);

    @Override
    boolean noneMatch(IntPredicate predicate);

    /**
     * @param jobConfig Job configuration which will be used while executing underlying DAG
     * @return the new stream
     */
    DistributedIntStream configure(JobConfig jobConfig);
}
