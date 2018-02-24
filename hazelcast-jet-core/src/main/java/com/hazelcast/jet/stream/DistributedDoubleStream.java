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
import com.hazelcast.jet.function.DistributedDoubleBinaryOperator;
import com.hazelcast.jet.function.DistributedDoubleConsumer;
import com.hazelcast.jet.function.DistributedDoubleFunction;
import com.hazelcast.jet.function.DistributedDoublePredicate;
import com.hazelcast.jet.function.DistributedDoubleToIntFunction;
import com.hazelcast.jet.function.DistributedDoubleToLongFunction;
import com.hazelcast.jet.function.DistributedDoubleUnaryOperator;
import com.hazelcast.jet.function.DistributedObjDoubleConsumer;
import com.hazelcast.jet.function.DistributedSupplier;

import java.util.OptionalDouble;
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

/**
 * An extension of {@link DoubleStream} that supports
 * distributed stream operations by replacing functional interfaces with
 * their serializable equivalents.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedDoubleStream extends DoubleStream {

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#filter(DoublePredicate) java.util.stream.DoubleStream#filter(DoublePredicate)}.
     */
    default DistributedDoubleStream filter(DistributedDoublePredicate predicate) {
        return filter((DoublePredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#map(DoubleUnaryOperator) java.util.stream.DoubleStream#map(DoubleUnaryOperator)}.
     */
    default DistributedDoubleStream map(DistributedDoubleUnaryOperator mapper) {
        return map((DoubleUnaryOperator) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#mapToObj(DoubleFunction) java.util.stream.DoubleStream#mapToObj(DoubleFunction)}.
     */
    default <U> DistributedStream<U> mapToObj(DistributedDoubleFunction<? extends U> mapper) {
        return mapToObj((DoubleFunction<? extends U>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#mapToLong(DoubleToLongFunction) java.util.stream.DoubleStream#mapToLong(DoubleToLongFunction)}.
     */
    default DistributedLongStream mapToLong(DistributedDoubleToLongFunction mapper) {
        return mapToLong((DoubleToLongFunction) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#mapToInt(DoubleToIntFunction) java.util.stream.DoubleStream#mapToInt(DoubleToIntFunction)}.
     */
    default DistributedIntStream mapToInt(DistributedDoubleToIntFunction mapper) {
        return mapToInt((DoubleToIntFunction) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#flatMap(DoubleFunction) java.util.stream.DoubleStream#flatMap(DoubleFunction)}.
     */
    default DistributedDoubleStream flatMap(DistributedDoubleFunction<? extends DoubleStream> mapper) {
        return flatMap((DoubleFunction<? extends DoubleStream>) mapper);
    }

    @Override
    DistributedDoubleStream distinct();

    @Override
    DistributedDoubleStream sorted();

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#peek(DoubleConsumer) java.util.stream.DoubleStream#peek(DoubleConsumer)}.
     */
    default DistributedDoubleStream peek(DistributedDoubleConsumer action) {
        return peek((DoubleConsumer) action);
    }

    @Override
    DistributedDoubleStream limit(long maxSize);

    @Override
    DistributedDoubleStream skip(long n);

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#reduce(double, DoubleBinaryOperator)
     * java.util.stream.DoubleStream#reduce(double, DoubleBinaryOperator)}.
     */
    default double reduce(double identity, DistributedDoubleBinaryOperator op) {
        return reduce(identity, (DoubleBinaryOperator) op);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#reduce(DoubleBinaryOperator) java.util.stream.DoubleStream#reduce(DoubleBinaryOperator)}.
     */
    default OptionalDouble reduce(DistributedDoubleBinaryOperator op) {
        return reduce((DoubleBinaryOperator) op);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#collect(Supplier, ObjDoubleConsumer, BiConsumer)
     * java.util.stream.DoubleStream#collect(Supplier, ObjDoubleConsumer, BiConsumer)}.
     */
    default <R> R collect(DistributedSupplier<R> supplier, DistributedObjDoubleConsumer<R> accumulator,
                          DistributedBiConsumer<R, R> combiner) {
        return collect((Supplier<R>) supplier, accumulator, combiner);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#anyMatch(DoublePredicate) java.util.stream.DoubleStream#anyMatch(DoublePredicate)}.
     */
    default boolean anyMatch(DistributedDoublePredicate predicate) {
        return anyMatch((DoublePredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#allMatch(DoublePredicate) java.util.stream.DoubleStream#allMatch(DoublePredicate)}.
     */
    default boolean allMatch(DistributedDoublePredicate predicate) {
        return allMatch((DoublePredicate) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link DoubleStream#noneMatch(DoublePredicate) java.util.stream.DoubleStream#noneMatch(DoublePredicate)}.
     */
    default boolean noneMatch(DistributedDoublePredicate predicate) {
        return noneMatch((DoublePredicate) predicate);
    }

    @Override
    DistributedStream<Double> boxed();

    @Override
    DistributedDoubleStream sequential();

    @Override
    DistributedDoubleStream parallel();

    @Override
    DistributedDoubleStream filter(DoublePredicate predicate);

    @Override
    DistributedDoubleStream map(DoubleUnaryOperator mapper);

    @Override <U> DistributedStream<U> mapToObj(DoubleFunction<? extends U> mapper);

    @Override
    DistributedLongStream mapToLong(DoubleToLongFunction mapper);

    @Override
    DistributedIntStream mapToInt(DoubleToIntFunction mapper);

    @Override
    DistributedDoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper);

    @Override
    DistributedDoubleStream peek(DoubleConsumer action);

    @Override
    void forEach(DoubleConsumer action);

    @Override
    void forEachOrdered(DoubleConsumer action);

    @Override
    double reduce(double identity, DoubleBinaryOperator op);

    @Override
    OptionalDouble reduce(DoubleBinaryOperator op);

    @Override <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner);

    @Override
    boolean anyMatch(DoublePredicate predicate);

    @Override
    boolean allMatch(DoublePredicate predicate);

    @Override
    boolean noneMatch(DoublePredicate predicate);

    /**
     * @param jobConfig Job configuration which will be used while executing underlying DAG
     * @return the new stream
     */
    DistributedDoubleStream configure(JobConfig jobConfig);
}
