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
    DistributedStream<T> filter(Distributed.Predicate<? super T> predicate);

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
    <R> DistributedStream<R> map(Distributed.Function<? super T, ? extends R> mapper);

    /**
     * Returns an {@code DistributedIntStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    DistributedIntStream mapToInt(Distributed.ToIntFunction<? super T> mapper);

    /**
     * Returns a {@code DistributedLongStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    DistributedLongStream mapToLong(Distributed.ToLongFunction<? super T> mapper);

    /**
     * Returns a {@code DistributedDoubleStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate operation.
     *
     * @param mapper a non-interfering, stateless function to apply to each element
     * @return the new stream
     */
    DistributedDoubleStream mapToDouble(Distributed.ToDoubleFunction<? super T> mapper);

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
    <R> DistributedStream<R> flatMap(Distributed.Function<? super T, ? extends Stream<? extends R>> mapper);

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
    DistributedIntStream flatMapToInt(Distributed.Function<? super T, ? extends IntStream> mapper);

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
    DistributedLongStream flatMapToLong(Distributed.Function<? super T, ? extends LongStream> mapper);

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
    DistributedDoubleStream flatMapToDouble(Distributed.Function<? super T, ? extends DoubleStream> mapper);

    /**
     * Returns a stream consisting of the distinct elements (according to
     * {@link Object#equals(Object)}) of this stream.
     *
     * <p>For ordered streams, the selection of distinct elements is stable
     * (for duplicated elements, the element appearing first in the encounter
     * order is preserved.)  For unordered streams, no stability guarantees
     * are made.
     *
     * <p>This is a stateful intermediate operation.
     *
     * @return the new stream
     */
    DistributedStream<T> distinct();

    /**
     * Returns a stream consisting of the elements of this stream, sorted
     * according to natural order.  If the elements of this stream are not
     * {@code Comparable}, a {@code java.lang.ClassCastException} may be thrown
     * when the terminal operation is executed.
     *
     * <p>For ordered streams, the sort is stable.  For unordered streams, no
     * stability guarantees are made.
     *
     * <p>This is a stateful intermediate operation.
     *
     * @return the new stream
     */
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
    DistributedStream<T> sorted(Distributed.Comparator<? super T> comparator);

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
    DistributedStream<T> peek(Distributed.Consumer<? super T> action);

    /**
     * Returns a stream consisting of the elements of this stream, truncated
     * to be no longer than {@code maxSize} in length.
     *
     * <p>This is a short-circuiting stateful intermediate operation.
     *
     * @param maxSize the number of elements the stream should be limited to
     * @return the new stream
     * @throws IllegalArgumentException if {@code maxSize} is negative
     */
    DistributedStream<T> limit(long maxSize);

    /**
     * Returns a stream consisting of the remaining elements of this stream
     * after discarding the first {@code n} elements of the stream.
     * If this stream contains fewer than {@code n} elements then an
     * empty stream will be returned.
     *
     * <p>This is a stateful intermediate operation.
     *
     * @param n the number of leading elements to skip
     * @return the new stream
     * @throws IllegalArgumentException if {@code n} is negative
     */
    DistributedStream<T> skip(long n);

    /**
     * Performs an action for each element of this stream.
     *
     * <p>This is a terminal operation.
     *
     * @param action a
     *               non-interfering action to perform on the elements
     */
    void forEach(Distributed.Consumer<? super T> action);

    /**
     * Performs an action for each element of this stream, in the encounter
     * order of the stream if the stream has a defined encounter order.
     *
     * <p>This is a terminal operation.
     *
     * @param action a
     *               non-interfering action to perform on the elements
     * @see #forEach(Consumer)
     */
    void forEachOrdered(Distributed.Consumer<? super T> action);

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
    T reduce(T identity, Distributed.BinaryOperator<T> accumulator);

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
    Optional<T> reduce(Distributed.BinaryOperator<T> accumulator);

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
    <U> U reduce(U identity,
                 Distributed.BiFunction<U, ? super T, U> accumulator,
                 Distributed.BinaryOperator<U> combiner);

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
    <R> R collect(Distributed.Supplier<R> supplier,
                  Distributed.BiConsumer<R, ? super T> accumulator,
                  Distributed.BiConsumer<R, R> combiner);

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
    <R, A> R collect(Distributed.Collector<? super T, A, R> collector);

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
    Optional<T> min(Distributed.Comparator<? super T> comparator);

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
    Optional<T> max(Distributed.Comparator<? super T> comparator);

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
    boolean anyMatch(Distributed.Predicate<? super T> predicate);

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
    boolean allMatch(Distributed.Predicate<? super T> predicate);

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
    boolean noneMatch(Distributed.Predicate<? super T> predicate);

    /**
     * Returns an equivalent stream that is sequential.  May return
     * itself, either because the stream was already sequential, or because
     * the underlying stream state was modified to be sequential.
     *
     * <p>This is an intermediate operation.
     *
     * @return a sequential stream
     */
    @Override
    DistributedStream<T> sequential();

    /**
     * Returns an equivalent stream that is parallel.  May return
     * itself, either because the stream was already parallel, or because
     * the underlying stream state was modified to be parallel.
     *
     * <p>This is an intermediate operation.
     *
     * @return a parallel stream
     */
    @Override
    DistributedStream<T> parallel();

    /**
     * Returns an equivalent stream that is unordered. May return
     * itself, either because the stream was already unordered, or because
     * the underlying stream state was modified to be unordered.
     *
     * <p>This is an intermediate operation.
     *
     * @return an unordered stream
     */
    @Override
    DistributedStream<T> unordered();

    @Override
    default DistributedStream<T> filter(Predicate<? super T> predicate) {
        return filter((Distributed.Predicate) predicate);
    }

    @Override
    default <R> DistributedStream<R> map(Function<? super T, ? extends R> mapper) {
        return map((Distributed.Function) mapper);
    }

    @Override
    default DistributedIntStream mapToInt(ToIntFunction<? super T> mapper) {
        return mapToInt((Distributed.ToIntFunction) mapper);
    }

    @Override
    default DistributedLongStream mapToLong(ToLongFunction<? super T> mapper) {
        return mapToLong((Distributed.ToLongFunction) mapper);
    }

    @Override
    default DistributedDoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return mapToDouble((Distributed.ToDoubleFunction) mapper);
    }

    @Override
    default <R> DistributedStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return flatMap((Distributed.Function) mapper);
    }

    @Override
    default DistributedIntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return flatMapToInt((Distributed.Function) mapper);
    }

    @Override
    default DistributedLongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return flatMapToLong((Distributed.Function) mapper);
    }

    @Override
    default DistributedDoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return flatMapToDouble((Distributed.Function) mapper);
    }

    @Override
    default DistributedStream<T> sorted(Comparator<? super T> comparator) {
        return sorted((Distributed.Comparator) comparator);
    }

    @Override
    default DistributedStream<T> peek(Consumer<? super T> action) {
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
