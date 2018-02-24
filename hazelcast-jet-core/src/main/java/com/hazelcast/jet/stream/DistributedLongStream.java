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
import com.hazelcast.jet.function.DistributedLongBinaryOperator;
import com.hazelcast.jet.function.DistributedLongConsumer;
import com.hazelcast.jet.function.DistributedLongFunction;
import com.hazelcast.jet.function.DistributedLongPredicate;
import com.hazelcast.jet.function.DistributedLongToDoubleFunction;
import com.hazelcast.jet.function.DistributedLongToIntFunction;
import com.hazelcast.jet.function.DistributedLongUnaryOperator;
import com.hazelcast.jet.function.DistributedObjLongConsumer;
import com.hazelcast.jet.function.DistributedSupplier;

import java.util.OptionalLong;
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
import java.util.stream.LongStream;

/**
 * An extension of {@link LongStream} that supports distributed stream
 * operations by replacing functional interfaces with their serializable
 * equivalents.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedLongStream extends LongStream {

    /**
     * {@code Serializable} variant of
     * {@link LongStream#filter(LongPredicate) java.util.stream.LongStream#filter(LongPredicate)}.
     */
    default DistributedLongStream filter(DistributedLongPredicate predicate) {
        return filter((LongPredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#map(LongUnaryOperator) java.util.stream.LongStream#map(LongUnaryOperator)}.
     */
    default DistributedLongStream map(DistributedLongUnaryOperator mapper) {
        return map((LongUnaryOperator) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#mapToObj(LongFunction) java.util.stream.LongStream#mapToObj(LongFunction)}.
     */
    default <U> DistributedStream<U> mapToObj(DistributedLongFunction<? extends U> mapper) {
        return mapToObj((LongFunction<? extends U>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#mapToInt(LongToIntFunction) java.util.stream.LongStream#mapToInt(LongToIntFunction)}.
     */
    default DistributedIntStream mapToInt(DistributedLongToIntFunction mapper) {
        return mapToInt((LongToIntFunction) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#mapToDouble(LongToDoubleFunction) java.util.stream.LongStream#mapToDouble(LongToDoubleFunction)}.
     */
    default DistributedDoubleStream mapToDouble(DistributedLongToDoubleFunction mapper) {
        return mapToDouble((LongToDoubleFunction) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#flatMap(LongFunction) java.util.stream.LongStream#flatMap(LongFunction)}.
     */
    default DistributedLongStream flatMap(DistributedLongFunction<? extends LongStream> mapper) {
        return flatMap((LongFunction<? extends LongStream>) mapper);
    }

    @Override
    DistributedLongStream distinct();

    @Override
    DistributedLongStream sorted();

    /**
     * {@code Serializable} variant of
     * {@link LongStream#peek(LongConsumer) java.util.stream.LongStream#peek(LongConsumer)}.
     */
    default DistributedLongStream peek(DistributedLongConsumer action) {
        return peek((LongConsumer) action);
    }

    @Override
    DistributedLongStream limit(long maxSize);

    @Override
    DistributedLongStream skip(long n);

    /**
     * {@code Serializable} variant of
     * {@link LongStream#reduce(long, LongBinaryOperator) java.util.stream.LongStream#reduce(long, LongBinaryOperator)}.
     */
    default long reduce(long identity, DistributedLongBinaryOperator op) {
        return reduce(identity, (LongBinaryOperator) op);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#reduce(LongBinaryOperator) java.util.stream.LongStream#reduce(LongBinaryOperator)}.
     */
    default OptionalLong reduce(DistributedLongBinaryOperator op) {
        return reduce((LongBinaryOperator) op);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#collect(Supplier, ObjLongConsumer, BiConsumer)
     * java.util.stream.LongStream#collect(Supplier, ObjLongConsumer, BiConsumer)}.
     */
    default <R> R collect(DistributedSupplier<R> supplier, DistributedObjLongConsumer<R> accumulator,
                          DistributedBiConsumer<R, R> combiner) {
        return collect((Supplier<R>) supplier, accumulator, combiner);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#anyMatch(LongPredicate) java.util.stream.LongStream#anyMatch(LongPredicate)}.
     */
    default boolean anyMatch(DistributedLongPredicate predicate) {
        return anyMatch((LongPredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#allMatch(LongPredicate) java.util.stream.LongStream#allMatch(LongPredicate)}.
     */
    default boolean allMatch(DistributedLongPredicate predicate) {
        return allMatch((LongPredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link LongStream#noneMatch(LongPredicate) java.util.stream.LongStream#noneMatch(LongPredicate)}.
     */
    default boolean noneMatch(DistributedLongPredicate predicate) {
        return noneMatch((LongPredicate) predicate);
    }

    @Override
    DistributedDoubleStream asDoubleStream();

    @Override
    DistributedStream<Long> boxed();

    @Override
    DistributedLongStream sequential();

    @Override
    DistributedLongStream parallel();

    @Override
    DistributedLongStream filter(LongPredicate predicate);

    @Override
    DistributedLongStream map(LongUnaryOperator mapper);

    @Override <U> DistributedStream<U> mapToObj(LongFunction<? extends U> mapper);

    @Override
    DistributedIntStream mapToInt(LongToIntFunction mapper);

    @Override
    DistributedDoubleStream mapToDouble(LongToDoubleFunction mapper);

    @Override
    DistributedLongStream flatMap(LongFunction<? extends LongStream> mapper);

    @Override
    DistributedLongStream peek(LongConsumer action);

    @Override
    long reduce(long identity, LongBinaryOperator op);

    @Override
    OptionalLong reduce(LongBinaryOperator op);

    @Override <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner);

    @Override
    boolean anyMatch(LongPredicate predicate);

    @Override
    boolean allMatch(LongPredicate predicate);

    @Override
    boolean noneMatch(LongPredicate predicate);

    /**
     * @param jobConfig Job configuration which will be used while executing underlying DAG
     * @return the new stream
     */
    DistributedLongStream configure(JobConfig jobConfig);
}
