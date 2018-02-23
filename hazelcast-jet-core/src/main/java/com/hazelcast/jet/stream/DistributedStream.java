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
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.function.DistributedToIntFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.IListDecorator;
import com.hazelcast.jet.stream.impl.IMapDecorator;
import com.hazelcast.jet.stream.impl.pipeline.AbstractSourcePipe;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.reducers.CollectorReducer;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Optional;
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

/**
 * An extension of {@link Stream} that supports distributed stream
 * operations by replacing functional interfaces with their {@code
 * Serializable} equivalents.
 *
 * @param <T> the type of the stream elements
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedStream<T> extends Stream<T> {

    /**
     * Returns a distributed {@code Stream} with the given {@link BatchSource} as
     * the source.
     *
     * @param instance  the instance where the stream will be executed on
     * @param source    source of the stream
     * @param isOrdered whether the source should be treated as ordered or unordered. An ordered stream
     *                  will not have any parallelism.
     */
    static <T> DistributedStream<T> fromSource(JetInstance instance, BatchSource<T> source, boolean isOrdered) {
        return new AbstractSourcePipe<T>(new StreamContext(instance)) {
            @Override
            protected ProcessorMetaSupplier getSourceMetaSupplier() {
                return ((BatchSourceTransform) source).metaSupplier;
            }

            @Override
            protected String getName() {
                return source.name();
            }

            @Override
            public boolean isOrdered() {
                return isOrdered;
            }
        };
    }

    /**
     * Returns a {@link DistributedStream} with this map as its source.
     * <p>
     * If the underlying map is being concurrently modified, there are no
     * guarantees given with respect to missing or duplicate items in a
     * stream operation.
     */
    @Nonnull
    static <K, V> DistributedStream<Entry<K, V>> fromMap(@Nonnull IMapJet<K, V> map) {
        IMapDecorator decorator = (IMapDecorator) map;
        return fromSource(decorator.getInstance(), Sources.map(map.getName()), false);
    }

    /**
     * Returns a {@link DistributedStream} with this map as its source.
     * Entries will be filtered and mapped according to the given predicate
     * and projection.
     * <p>
     * If the underlying map is being concurrently modified, there are no
     * guarantees given with respect to missing or duplicate items in a
     * stream operation.
     * <p>
     * To create a {@code Predicate} instance you might prefer to use Jet's
     * {@link com.hazelcast.jet.GenericPredicates}.
     */
    @Nonnull
    static <K, V, T> DistributedStream<T> fromMap(
            @Nonnull IMapJet<K, V> map,
            @Nonnull com.hazelcast.query.Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn
    ) {
        IMapDecorator decorator = (IMapDecorator) map;
        return fromSource(decorator.getInstance(), Sources.map(map.getName(), predicate, projectionFn), false);
    }

    /**
     * Returns an ordered {@link DistributedStream} with this list as its
     * source.
     */
    @Nonnull
    static <T> DistributedStream<T> fromList(IListJet<T> list) {
        IListDecorator decorator = (IListDecorator<T>) list;
        return fromSource(decorator.getInstance(), Sources.list(list.getName()), true);
    }

    ;

    /**
     * {@code Serializable} variant of
     * {@link Stream#filter(Predicate) java.util.stream.Stream#filter(Predicate)}.
     */
    default DistributedStream<T> filter(DistributedPredicate<? super T> predicate) {
        return filter((Predicate<? super T>) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#map(Function) java.util.stream.Stream#map(Function)}.
     */
    default <R> DistributedStream<R> map(DistributedFunction<? super T, ? extends R> mapper) {
        return map((Function<? super T, ? extends R>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#mapToInt(ToIntFunction) java.util.stream.Stream#mapToInt(ToIntFunction)}.
     */
    default DistributedIntStream mapToInt(DistributedToIntFunction<? super T> mapper) {
        return mapToInt((ToIntFunction<? super T>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#mapToLong(ToLongFunction) java.util.stream.Stream#mapToLong(ToLongFunction)}.
     */
    default DistributedLongStream mapToLong(DistributedToLongFunction<? super T> mapper) {
        return mapToLong((ToLongFunction<? super T>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#mapToDouble(ToDoubleFunction) java.util.stream.Stream#mapToDouble(ToDoubleFunction)}.
     */
    default DistributedDoubleStream mapToDouble(DistributedToDoubleFunction<? super T> mapper) {
        return mapToDouble((ToDoubleFunction<? super T>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#flatMap(Function) java.util.stream.Stream#flatMap(Function)}.
     */
    default <R> DistributedStream<R> flatMap(DistributedFunction<? super T, ? extends Stream<? extends R>> mapper) {
        return flatMap((Function<? super T, ? extends Stream<? extends R>>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#flatMapToInt(Function) java.util.stream.Stream#flatMapToInt(Function)}.
     */
    default DistributedIntStream flatMapToInt(DistributedFunction<? super T, ? extends IntStream> mapper) {
        return flatMapToInt((Function<? super T, ? extends IntStream>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#flatMapToLong(Function) java.util.stream.Stream#flatMapToLong(Function)}.
     */
    default DistributedLongStream flatMapToLong(DistributedFunction<? super T, ? extends LongStream> mapper) {
        return flatMapToLong((Function<? super T, ? extends LongStream>) mapper);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#flatMapToDouble(Function) java.util.stream.Stream#flatMapToDouble(Function)}.
     */
    default DistributedDoubleStream flatMapToDouble(DistributedFunction<? super T, ? extends DoubleStream> mapper) {
        return flatMapToDouble((Function<? super T, ? extends DoubleStream>) mapper);
    }

    @Override
    DistributedStream<T> distinct();

    @Override
    DistributedStream<T> sorted();

    /**
     * {@code Serializable} variant of
     * {@link Stream#sorted(Comparator) java.util.stream.Stream#sorted(Comparator)}.
     */
    default DistributedStream<T> sorted(DistributedComparator<? super T> comparator) {
        return sorted((Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#peek(Consumer) java.util.stream.Stream#peek(Consumer)}.
     */
    default DistributedStream<T> peek(DistributedConsumer<? super T> action) {
        return peek((Consumer<? super T>) action);
    }

    @Override
    DistributedStream<T> limit(long maxSize);

    @Override
    DistributedStream<T> skip(long n);

    /**
     * {@code Serializable} variant of
     * {@link Stream#reduce(Object, BinaryOperator) java.util.stream.Stream#reduce(Object, BinaryOperator)}.
     */
    default T reduce(T identity, DistributedBinaryOperator<T> accumulator) {
        return reduce(identity, (BinaryOperator<T>) accumulator);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#reduce(BinaryOperator) java.util.stream.Stream#reduce(BinaryOperator)}.
     */
    default Optional<T> reduce(DistributedBinaryOperator<T> accumulator) {
        return reduce((BinaryOperator<T>) accumulator);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#reduce(Object, BiFunction, BinaryOperator)
     * java.util.stream.Stream#reduce(Object, BiFunction, BinaryOperator)}.
     */
    default <U> U reduce(U identity,
                         DistributedBiFunction<U, ? super T, U> accumulator,
                         DistributedBinaryOperator<U> combiner) {
        return reduce(identity, (BiFunction<U, ? super T, U>) accumulator, combiner);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#collect(Supplier, BiConsumer, BiConsumer)
     * java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)}.
     */
    default <R> R collect(DistributedSupplier<R> supplier,
                          DistributedBiConsumer<R, ? super T> accumulator,
                          DistributedBiConsumer<R, R> combiner) {
        return collect((Supplier<R>) supplier, accumulator, combiner);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#collect(Collector) java.util.stream.Stream#collect(Collector)}.
     */
    default <R, A> R collect(DistributedCollector<? super T, A, R> collector) {
        return collect(new CollectorReducer<>(collector.supplier(), collector.accumulator(),
                collector.combiner(), collector.finisher()));
    }

    /**
     * Terminate the stream using a reduction performed by the given {@link Reducer}
     * and return the resulting value.
     * <p>
     * A {@link Reducer} is specific to Jet, and is responsible for building
     * and executing the underlying DAG. It can't be used as a downstream collector in a
     * collector cascade.
     *
     * @param reducer the reducer
     * @param <R>     type of the return value
     * @return the result of the reduction operation
     */
    <R> R collect(Reducer<? super T, R> reducer);

    /**
     * {@code Serializable} variant of
     * {@link Stream#min(Comparator) java.util.stream.Stream#min(Comparator)}.
     */
    default Optional<T> min(DistributedComparator<? super T> comparator) {
        return min((Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#max(Comparator) java.util.stream.Stream#max(Comparator)}.
     */
    default Optional<T> max(DistributedComparator<? super T> comparator) {
        return max((Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#anyMatch(Predicate) java.util.stream.Stream#anyMatch(Predicate)}.
     */
    default boolean anyMatch(DistributedPredicate<? super T> predicate) {
        return anyMatch((Predicate<? super T>) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#allMatch(Predicate) java.util.stream.Stream#allMatch(Predicate)}.
     */
    default boolean allMatch(DistributedPredicate<? super T> predicate) {
        return allMatch((Predicate<? super T>) predicate);
    }

    /**
     * {@code Serializable} variant of
     * {@link Stream#noneMatch(Predicate) java.util.stream.Stream#noneMatch(Predicate)}.
     */
    default boolean noneMatch(DistributedPredicate<? super T> predicate) {
        return noneMatch((Predicate<? super T>) predicate);
    }

    @Override
    DistributedStream<T> sequential();

    @Override
    DistributedStream<T> parallel();

    @Override
    DistributedStream<T> unordered();

    @Override
    DistributedStream<T> filter(Predicate<? super T> predicate);

    @Override <R> DistributedStream<R> map(Function<? super T, ? extends R> mapper);

    @Override
    DistributedIntStream mapToInt(ToIntFunction<? super T> mapper);

    @Override
    DistributedLongStream mapToLong(ToLongFunction<? super T> mapper);

    @Override
    DistributedDoubleStream mapToDouble(ToDoubleFunction<? super T> mapper);

    @Override <R> DistributedStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper);

    @Override
    DistributedIntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper);

    @Override
    DistributedLongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper);

    @Override
    DistributedDoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper);

    @Override
    DistributedStream<T> sorted(Comparator<? super T> comparator);

    @Override
    DistributedStream<T> peek(Consumer<? super T> action);

    @Override
    T reduce(T identity, BinaryOperator<T> accumulator);

    @Override
    Optional<T> reduce(BinaryOperator<T> accumulator);

    @Override <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner);

    @Override <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner);

    @Override
    Optional<T> min(Comparator<? super T> comparator);

    @Override
    Optional<T> max(Comparator<? super T> comparator);

    @Override
    boolean anyMatch(Predicate<? super T> predicate);

    @Override
    boolean allMatch(Predicate<? super T> predicate);

    @Override
    boolean noneMatch(Predicate<? super T> predicate);

    /**
     * @param jobConfig Job configuration which will be used while executing underlying DAG
     * @return the new stream
     */
    DistributedStream<T> configure(JobConfig jobConfig);
}
