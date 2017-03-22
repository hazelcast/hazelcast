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

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.reducers.CollectorReducer;

import java.util.Comparator;
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
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * An extension of {@link java.util.stream.Stream} to support distributed stream operations by replacing
 * functional interfaces with their serializable equivalents.
 *
 * @param <T> the type of the stream elements
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedStream<T> extends Stream<T> {

    /**
     * Returns a stream consisting of the elements of this stream that match
     * the given predicate.
     *
     * <p>This is an intermediate operation.
     *
     * @param predicate a non-interfering, stateless predicate to apply to each element
     *                  to determine if it
     *                  should be included
     * @return the new stream
     */
    default DistributedStream<T> filter(Distributed.Predicate<? super T> predicate) {
        return filter((Predicate<? super T>) predicate);
    }

    /**
     * Returns a stream consisting of the results of applying the given
     * function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param <R>    The element type of the new stream
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    default <R> DistributedStream<R> map(Distributed.Function<? super T, ? extends R> mapper) {
        return map((Function<? super T, ? extends R>) mapper);
    }

    /**
     * Returns an {@code DistributedIntStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    default DistributedIntStream mapToInt(Distributed.ToIntFunction<? super T> mapper) {
        return mapToInt((ToIntFunction<? super T>) mapper);
    }

    /**
     * Returns a {@code DistributedLongStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    default DistributedLongStream mapToLong(Distributed.ToLongFunction<? super T> mapper) {
        return mapToLong((ToLongFunction<? super T>) mapper);
    }

    /**
     * Returns a {@code DistributedDoubleStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    default DistributedDoubleStream mapToDouble(Distributed.ToDoubleFunction<? super T> mapper) {
        return mapToDouble((ToDoubleFunction<? super T>) mapper);
    }

    /**
     * Returns a stream consisting of the results of replacing each element of
     * this stream with the contents of a mapped stream produced by applying
     * the provided mapping function to each element.  Each mapped stream is
     * {@link java.util.stream.BaseStream#close() closed} after its contents
     * have been placed into this stream.  (If a mapped stream is {@code null}
     * an empty stream is used, instead.)
     *
     * <p>This is an intermediate operation.
     *
     * @param <R>    The element type of the new stream
     * @param mapper a non-interfering, stateles function to apply to each element which produces
     *               a stream of new values
     * @return the new stream
     */
    default <R> DistributedStream<R> flatMap(Distributed.Function<? super T, ? extends Stream<? extends R>> mapper) {
        return flatMap((Function<? super T, ? extends Stream<? extends R>>) mapper);
    }

    /**
     * Returns an {@code IntStream} consisting of the results of replacing each
     * element of this stream with the contents of a mapped stream produced by
     * applying the provided mapping function to each element.  Each mapped
     * stream is {@link java.util.stream.BaseStream#close() closed} after its
     * contents have been placed into this stream.  (If a mapped stream is
     * {@code null} an empty stream is used, instead.)
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element which produces
     *               a stream of new values
     * @return the new stream
     * @see #flatMap(Function)
     */
    default DistributedIntStream flatMapToInt(Distributed.Function<? super T, ? extends IntStream> mapper) {
        return flatMapToInt((Function<? super T, ? extends IntStream>) mapper);
    }

    /**
     * Returns an {@code LongStream} consisting of the results of replacing each
     * element of this stream with the contents of a mapped stream produced by
     * applying the provided mapping function to each element.  Each mapped
     * stream is {@link java.util.stream.BaseStream#close() closed} after its
     * contents have been placed into this stream.  (If a mapped stream is
     * {@code null} an empty stream is used, instead.)
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element which produces
     *               a stream of new values
     * @return the new stream
     * @see #flatMap(Function)
     */
    default DistributedLongStream flatMapToLong(Distributed.Function<? super T, ? extends LongStream> mapper) {
        return flatMapToLong((Function<? super T, ? extends LongStream>) mapper);
    }

    /**
     * Returns an {@code DoubleStream} consisting of the results of replacing
     * each element of this stream with the contents of a mapped stream produced
     * by applying the provided mapping function to each element.  Each mapped
     * stream is {@link java.util.stream.BaseStream#close() closed} after its
     * contents have placed been into this stream.  (If a mapped stream is
     * {@code null} an empty stream is used, instead.)
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element which produces
     *               a stream of new values
     * @return the new stream
     * @see #flatMap(Function)
     */
    default DistributedDoubleStream flatMapToDouble(Distributed.Function<? super T, ? extends DoubleStream> mapper) {
        return flatMapToDouble((Function<? super T, ? extends DoubleStream>) mapper);
    }

    @Override
    DistributedStream<T> distinct();

    @Override
    DistributedStream<T> sorted();

    /**
     * Returns a stream consisting of the elements of this stream, sorted
     * according to the provided {@code Comparator}.
     *
     * <p>For ordered streams, the sort is stable.  For unordered streams, no
     * stability guarantees are made.
     *
     * <p>This is a stateful intermediate operation.
     *
     * @param comparator a non-interfering, stateless
     *                   {@code Distributed.Comparator} to be used to compare stream elements
     * @return the new stream
     */
    default DistributedStream<T> sorted(Distributed.Comparator<? super T> comparator) {
        return sorted((Comparator<? super T>) comparator);
    }

    /**
     * Returns a stream consisting of the elements of this stream, additionally
     * performing the provided action on each element as elements are consumed
     * from the resulting stream.
     *
     * <p>This is an intermediate operation.
     *
     * <p>The action may be called at whatever time and in whatever thread the element is made
     * available by the upstream operation.  If the action modifies shared state,
     * it is responsible for providing the required synchronization.
     *
     * @param action a
     *               non-interfering action to perform on the elements as
     *               they are consumed from the stream
     * @return the new stream
     */
    default DistributedStream<T> peek(Distributed.Consumer<? super T> action) {
        return peek((Consumer<? super T>) action);
    }

    @Override
    DistributedStream<T> limit(long maxSize);

    @Override
    DistributedStream<T> skip(long n);

    /**
     * Performs a reduction on the
     * elements of this stream, using the provided identity value and an
     * associative
     * accumulation function, and returns the reduced value.  This is equivalent
     * to:
     * <pre>{@code
     *     T result = identity;
     *     for (T element : this stream)
     *         result = accumulator.apply(result, element)
     *     return result;
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code identity} value must be an identity for the accumulator
     * function. This means that for all {@code t},
     * {@code accumulator.apply(identity, t)} is equal to {@code t}.
     * The {@code accumulator} function must be an
     * associative function.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param identity    the identity value for the accumulating function
     * @param accumulator an associative, non-interfering, stateless
     *                    function for combining two values
     * @return the result of the reduction
     */
    default T reduce(T identity, Distributed.BinaryOperator<T> accumulator) {
        return reduce(identity, (BinaryOperator<T>) accumulator);
    }

    /**
     * Performs a reduction on the
     * elements of this stream, using an
     * associative accumulation
     * function, and returns an {@code Optional} describing the reduced value,
     * if any. This is equivalent to:
     * <pre>{@code
     *     boolean foundAny = false;
     *     T result = null;
     *     for (T element : this stream) {
     *         if (!foundAny) {
     *             foundAny = true;
     *             result = element;
     *         }
     *         else
     *             result = accumulator.apply(result, element);
     *     }
     *     return foundAny ? Optional.of(result) : Optional.empty();
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code accumulator} function must be an
     * associative function.
     *
     * <p>This is a terminal operation.
     *
     * @param accumulator an associative, non-interfering, stateless
     *                    function for combining two values
     * @return an {@link Optional} describing the result of the reduction
     * @throws NullPointerException if the result of the reduction is null
     * @see #reduce(Object, Distributed.BinaryOperator)
     * @see #min(Distributed.Comparator)
     * @see #max(Distributed.Comparator)
     */
    default Optional<T> reduce(Distributed.BinaryOperator<T> accumulator) {
        return reduce((BinaryOperator<T>) accumulator);
    }

    /**
     * Performs a reduction on the
     * elements of this stream, using the provided identity, accumulation and
     * combining functions.  This is equivalent to:
     * <pre>{@code
     *     U result = identity;
     *     for (T element : this stream)
     *         result = accumulator.apply(result, element)
     *     return result;
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code identity} value must be an identity for the combiner
     * function.  This means that for all {@code u}, {@code combiner(identity, u)}
     * is equal to {@code u}.  Additionally, the {@code combiner} function
     * must be compatible with the {@code accumulator} function; for all
     * {@code u} and {@code t}, the following must hold:
     * <pre>{@code
     *     combiner.apply(u, accumulator.apply(identity, t)) == accumulator.apply(u, t)
     * }</pre>
     *
     * <p>This is a terminal
     * operation.
     *
     * @param <U>         The type of the result
     * @param identity    the identity value for the combiner function
     * @param accumulator an associative, non-interfering, stateless
     *                    function for incorporating an additional element into a result
     * @param combiner    an associative, non-interfering, stateless
     *                    function for combining two values, which must be
     *                    compatible with the accumulator function
     * @return the result of the reduction
     * @apiNote Many reductions using this form can be represented more simply
     * by an explicit combination of {@code map} and {@code reduce} operations.
     * The {@code accumulator} function acts as a fused mapper and accumulator,
     * which can sometimes be more efficient than separate mapping and reduction,
     * such as when knowing the previously reduced value allows you to avoid
     * some computation.
     * @see #reduce(Distributed.BinaryOperator)
     * @see #reduce(Object, Distributed.BinaryOperator)
     */
    default <U> U reduce(U identity,
                         Distributed.BiFunction<U, ? super T, U> accumulator,
                         Distributed.BinaryOperator<U> combiner) {
        return reduce(identity, (BiFunction<U, ? super T, U>) accumulator, combiner);
    }

    /**
     * Performs a mutable
     * reduction operation on the elements of this stream.  A mutable
     * reduction is one in which the reduced value is a mutable result container,
     * such as an {@code ArrayList}, and elements are incorporated by updating
     * the state of the result rather than by replacing the result.  This
     * produces a result equivalent to:
     * <pre>{@code
     *     R result = supplier.get();
     *     for (T element : this stream)
     *         accumulator.accept(result, element);
     *     return result;
     * }</pre>
     *
     * <p>Like {@link #reduce(Object, Distributed.BinaryOperator)}, {@code collect} operations
     * can be parallelized without requiring additional synchronization.
     *
     * <p>This is a terminal operation.
     *
     * @param <R>         type of the result
     * @param supplier    a function that creates a new result container. For a
     *                    parallel execution, this function may be called
     *                    multiple times and must return a fresh value each time.
     * @param accumulator an associative,
     *                    non-interfering,
     *                    stateless
     *                    function for incorporating an additional element into a result
     * @param combiner    an associative,
     *                    non-interfering,
     *                    stateless
     *                    function for combining two values, which must be
     *                    compatible with the accumulator function
     * @return the result of the reduction
     */
    default <R> R collect(Distributed.Supplier<R> supplier,
                          Distributed.BiConsumer<R, ? super T> accumulator,
                          Distributed.BiConsumer<R, R> combiner) {
        return collect((Supplier<R>) supplier, accumulator, combiner);
    }

    /**
     * Performs a mutable
     * reduction operation on the elements of this stream using a
     * {@code Distributed.Collector}.  A {@code Distributed.Collector}
     * encapsulates the functions used as arguments to
     * {@link #collect(Distributed.Supplier, Distributed.BiConsumer, Distributed.BiConsumer)},
     * allowing for reuse of collection strategies and composition of collect operations such as
     * multiple-level grouping or partitioning.
     *
     * <p>This is a terminal operation.
     *
     * <p>When executed in parallel, multiple intermediate results may be
     * instantiated, populated, and merged so as to maintain isolation of
     * mutable data structures.  Therefore, even when executed in parallel
     * with non-thread-safe data structures (such as {@code ArrayList}), no
     * additional synchronization is needed for a parallel reduction.
     *
     * @param <R>       the type of the result
     * @param <A>       the intermediate accumulation type of the {@code Distributed.Collector}
     * @param collector the {@code Distributed.Collector} describing the reduction
     * @return the result of the reduction
     * @see #collect(Supplier, BiConsumer, BiConsumer)
     * @see Collectors
     */
    default <R, A> R collect(DistributedCollector<? super T, A, R> collector) {
        return collect(new CollectorReducer<>(collector.supplier(), collector.accumulator(),
                collector.combiner(), collector.finisher()));
    }

    /**
     * Terminate the stream using a reduction performed by {@link Reducer}
     * and return the resulting value.
     *
     * A {@link Reducer} is specific to Jet, and is responsible for building
     * and executing the underlying DAG. It can't be used as a downstream collector in a
     * collector cascade.
     *
     * @param reducer the reducer
     * @param <R> type of the return value
     * @return the result of the reduction operation
     */
    <R> R collect(Reducer<? super T, R> reducer);

    /**
     * Returns the minimum element of this stream according to the provided
     * {@code Distributed.Comparator}.  This is a special case of a
     * reduction.
     *
     * <p>This is a terminal operation.
     *
     * @param comparator a non-interfering, stateless
     *                   {@code Distributed.Comparator} to compare elements of this stream
     * @return an {@code Optional} describing the minimum element of this stream,
     * or an empty {@code Optional} if the stream is empty
     * @throws NullPointerException if the minimum element is null
     */
    default Optional<T> min(Distributed.Comparator<? super T> comparator) {
        return min((Comparator<? super T>) comparator);
    }

    /**
     * Returns the maximum element of this stream according to the provided
     * {@code Distributed.Comparator}.  This is a special case of a
     * reduction.
     *
     * <p>This is a terminal operation.
     *
     * @param comparator a non-interfering,
     *                   stateless
     *                   {@code Distributed.Comparator} to compare elements of this stream
     * @return an {@code Optional} describing the maximum element of this stream,
     * or an empty {@code Optional} if the stream is empty
     * @throws NullPointerException if the maximum element is null
     */
    default Optional<T> max(Distributed.Comparator<? super T> comparator) {
        return max((Comparator<? super T>) comparator);
    }

    /**
     * Returns whether any elements of this stream match the provided
     * predicate.  May not evaluate the predicate on all elements if not
     * necessary for determining the result.  If the stream is empty then
     * {@code false} is returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering, stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if any elements of the stream match the provided
     * predicate, otherwise {@code false}
     */
    default boolean anyMatch(Distributed.Predicate<? super T> predicate) {
        return anyMatch((Predicate<? super T>) predicate);
    }

    /**
     * Returns whether all elements of this stream match the provided predicate.
     * May not evaluate the predicate on all elements if not necessary for
     * determining the result.  If the stream is empty then {@code true} is
     * returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting terminal operation.
     *
     * @param predicate a non-interfering, stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if either all elements of the stream match the
     * provided predicate or the stream is empty, otherwise {@code false}
     */
    default boolean allMatch(Distributed.Predicate<? super T> predicate) {
        return allMatch((Predicate<? super T>) predicate);
    }

    /**
     * Returns whether no elements of this stream match the provided predicate.
     * May not evaluate the predicate on all elements if not necessary for
     * determining the result.  If the stream is empty then {@code true} is
     * returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if either no elements of the stream match the
     * provided predicate or the stream is empty, otherwise {@code false}
     */
    default boolean noneMatch(Distributed.Predicate<? super T> predicate) {
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
