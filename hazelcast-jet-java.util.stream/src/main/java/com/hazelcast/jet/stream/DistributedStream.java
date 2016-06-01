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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public interface DistributedStream<T> extends Stream<T> {

    DistributedStream<T> filter(Distributed.Predicate<? super T> predicate);

    <R> DistributedStream<R> map(Distributed.Function<? super T, ? extends R> mapper);

    DistributedIntStream mapToInt(Distributed.ToIntFunction<? super T> mapper);

    DistributedLongStream mapToLong(Distributed.ToLongFunction<? super T> mapper);

    DistributedDoubleStream mapToDouble(Distributed.ToDoubleFunction<? super T> mapper);

    <R> DistributedStream<R> flatMap(Distributed.Function<? super T, ? extends Stream<? extends R>> mapper);

    DistributedIntStream flatMapToInt(Distributed.Function<? super T, ? extends IntStream> mapper);

    DistributedLongStream flatMapToLong(Distributed.Function<? super T, ? extends LongStream> mapper);

    DistributedDoubleStream flatMapToDouble(Distributed.Function<? super T, ? extends DoubleStream> mapper);

    DistributedStream<T> distinct();

    DistributedStream<T> sorted();

    DistributedStream<T> sorted(Distributed.Comparator<? super T> comparator);

    DistributedStream<T> peek(Distributed.Consumer<? super T> action);

    DistributedStream<T> limit(long maxSize);

    DistributedStream<T> skip(long n);

    void forEach(Distributed.Consumer<? super T> action);

    void forEachOrdered(Distributed.Consumer<? super T> action);

    Object[] toArray();

    T reduce(T identity, Distributed.BinaryOperator<T> accumulator);

    Optional<T> reduce(Distributed.BinaryOperator<T> accumulator);

    <U> U reduce(U identity,
                 Distributed.BiFunction<U, ? super T, U> accumulator,
                 Distributed.BinaryOperator<U> combiner);

    <R> R collect(Distributed.Supplier<R> supplier,
                  Distributed.BiConsumer<R, ? super T> accumulator,
                  Distributed.BiConsumer<R, R> combiner);

    <R, A> R collect(Distributed.Collector<? super T, A, R> collector);

    Optional<T> min(Distributed.Comparator<? super T> comparator);

    Optional<T> max(Distributed.Comparator<? super T> comparator);

    long count();

    boolean anyMatch(Distributed.Predicate<? super T> predicate);

    boolean allMatch(Distributed.Predicate<? super T> predicate);

    boolean noneMatch(Distributed.Predicate<? super T> predicate);

    Optional<T> findFirst();

    Optional<T> findAny();

    Iterator<T> iterator();

    @Override
    Spliterator<T> spliterator();

    @Override
    DistributedStream<T> sequential();

    @Override
    DistributedStream<T> parallel();

    @Override
    DistributedStream<T> unordered();

    @Override
    default DistributedStream<T> filter(Predicate<? super T> predicate) {
        return filter((Distributed.Predicate) predicate);
    }

    @Override
    default <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
        return map((Distributed.Function) mapper);
    }

    @Override
    default IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return mapToInt((Distributed.ToIntFunction) mapper);
    }

    @Override
    default LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return mapToLong((Distributed.ToLongFunction) mapper);
    }

    @Override
    default DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return mapToDouble((Distributed.ToDoubleFunction) mapper);
    }

    @Override
    default <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return flatMap((Distributed.Function) mapper);
    }

    @Override
    default IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return flatMapToInt((Distributed.Function) mapper);
    }

    @Override
    default LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return flatMapToLong((Distributed.Function) mapper);
    }

    @Override
    default DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return flatMapToDouble((Distributed.Function) mapper);
    }

    @Override
    default Stream<T> sorted(Comparator<? super T> comparator) {
        return sorted((Distributed.Comparator) comparator);
    }

    @Override
    default Stream<T> peek(Consumer<? super T> action) {
        return peek((Distributed.Consumer) action);
    }

    @Override
    default void forEach(Consumer<? super T> action) {
        forEach((Distributed.Consumer) action);
    }

    @Override
    default void forEachOrdered(Consumer<? super T> action) {
        forEachOrdered((Distributed.Consumer) action);
    }

    @Override
    default T reduce(T identity, BinaryOperator<T> accumulator) {
        return reduce(identity, (Distributed.BinaryOperator<T>) accumulator);
    }

    @Override
    default Optional<T> reduce(BinaryOperator<T> accumulator) {
        return reduce((Distributed.BinaryOperator<T>) accumulator);
    }

    @Override
    default <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return reduce(identity,
                (Distributed.BiFunction<U, ? super T, U>) accumulator,
                (Distributed.BinaryOperator<U>) combiner);
    }

    @Override
    default <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return collect((Distributed.Supplier<R>) supplier,
                (Distributed.BiConsumer<R, ? super T>) accumulator,
                (Distributed.BiConsumer<R, R>) combiner);
    }

    @Override
    default <R, A> R collect(Collector<? super T, A, R> collector) {
        return (R) collect((Distributed.Collector) collector);
    }

    @Override
    default Optional<T> min(Comparator<? super T> comparator) {
        return min((Distributed.Comparator) comparator);
    }

    @Override
    default Optional<T> max(Comparator<? super T> comparator) {
        return max((Distributed.Comparator) comparator);
    }

    @Override
    default boolean anyMatch(Predicate<? super T> predicate) {
        return anyMatch((Distributed.Predicate) predicate);
    }

    @Override
    default boolean allMatch(Predicate<? super T> predicate) {
        return allMatch((Distributed.Predicate) predicate);
    }

    @Override
    default boolean noneMatch(Predicate<? super T> predicate) {
        return noneMatch((Distributed.Predicate) predicate);
    }

}
