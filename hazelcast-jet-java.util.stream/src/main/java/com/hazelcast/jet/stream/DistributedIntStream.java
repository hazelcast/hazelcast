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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * An extension of {@link java.util.stream.IntStream} to support distributed stream operations by replacing
 * functional interfaces with their serializable equivalents.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedIntStream extends IntStream {

    /**
     * Returns a stream consisting of the elements of this stream that match
     * the given predicate.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to each element to determine if it
     *                  should be included
     * @return the new stream
     */
    DistributedIntStream filter(Distributed.IntPredicate predicate);

    /**
     * Returns a stream consisting of the results of applying the given
     * function to the elements of this stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    DistributedIntStream map(Distributed.IntUnaryOperator mapper);

    /**
     * Returns an object-valued {@code DistributedStream} consisting of the results of
     * applying the given function to the elements of this stream.
     *
     * <p>This is an
     *     intermediate operation.
     *
     * @param <U> the element type of the new stream
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    <U> DistributedStream<U> mapToObj(Distributed.IntFunction<? extends U> mapper);

    /**
     * Returns a {@code DistributedLongStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    DistributedLongStream mapToLong(Distributed.IntToLongFunction mapper);

    /**
     * Returns a {@code DistributedDoubleStream} consisting of the results of applying the
     * given function to the elements of this stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element
     * @return the new stream
     */
    DistributedDoubleStream mapToDouble(Distributed.IntToDoubleFunction mapper);

    /**
     * Returns a stream consisting of the results of replacing each element of
     * this stream with the contents of a mapped stream produced by applying
     * the provided mapping function to each element.  Each mapped stream is
     * {@link java.util.stream.BaseStream#close() closed} after its contents
     * have been placed into this stream.  (If a mapped stream is {@code null}
     * an empty stream is used, instead.)
     *
     * <p>This is an intermediate
     * operation.
     *
     * @param mapper a non-interfering,
     *               stateless
     *               function to apply to each element which produces an
     *               {@code IntStream} of new values
     * @return the new stream
     * @see DistributedStream#flatMap(Distributed.Function)
     */
    DistributedIntStream flatMap(Distributed.IntFunction<? extends IntStream> mapper);

    /**
     * Returns a stream consisting of the distinct elements of this stream.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @return the new stream
     */
    DistributedIntStream distinct();

    /**
     * Returns a stream consisting of the elements of this stream in sorted
     * order.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @return the new stream
     */
    DistributedIntStream sorted();

    /**
     * Returns a stream consisting of the elements of this stream, additionally
     * performing the provided action on each element as elements are consumed
     * from the resulting stream.
     *
     * <p>This is an intermediate
     * operation.
     *
     * <p>For parallel stream pipelines, the action may be called at
     * whatever time and in whatever thread the element is made available by the
     * upstream operation.  If the action modifies shared state,
     * it is responsible for providing the required synchronization.
     *
     * @param action a
     *               non-interfering action to perform on the elements as
     *               they are consumed from the stream
     * @return the new stream
     */
    DistributedIntStream peek(Distributed.IntConsumer action);

    /**
     * Returns a stream consisting of the elements of this stream, truncated
     * to be no longer than {@code maxSize} in length.
     *
     * <p>This is a short-circuiting
     * stateful intermediate operation.
     *
     * @param maxSize the number of elements the stream should be limited to
     * @return the new stream
     * @throws IllegalArgumentException if {@code maxSize} is negative
     */
    DistributedIntStream limit(long maxSize);

    /**
     * Returns a stream consisting of the remaining elements of this stream
     * after discarding the first {@code n} elements of the stream.
     * If this stream contains fewer than {@code n} elements then an
     * empty stream will be returned.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @param n the number of leading elements to skip
     * @return the new stream
     * @throws IllegalArgumentException if {@code n} is negative
     */
    DistributedIntStream skip(long n);

    /**
     * Performs an action for each element of this stream.
     *
     * <p>This is a terminal
     * operation.
     *
     * <p>For parallel stream pipelines, this operation does <em>not</em>
     * guarantee to respect the encounter order of the stream, as doing so
     * would sacrifice the benefit of parallelism.  For any given element, the
     * action may be performed at whatever time and in whatever thread the
     * library chooses.  If the action accesses shared state, it is
     * responsible for providing the required synchronization.
     *
     * @param action a
     *               non-interfering action to perform on the elements
     */
    void forEach(Distributed.IntConsumer action);

    /**
     * Performs an action for each element of this stream, guaranteeing that
     * each element is processed in encounter order for streams that have a
     * defined encounter order.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param action a
     *               non-interfering action to perform on the elements
     * @see #forEach(Distributed.IntConsumer)
     */
    void forEachOrdered(Distributed.IntConsumer action);

    /**
     * Performs a reduction on the
     * elements of this stream, using the provided identity value and an
     * associative
     * accumulation function, and returns the reduced value.  This is equivalent
     * to:
     * <pre>{@code
     *     int result = identity;
     *     for (int element : this stream)
     *         result = accumulator.applyAsInt(result, element)
     *     return result;
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code identity} value must be an identity for the accumulator
     * function. This means that for all {@code x},
     * {@code accumulator.apply(identity, x)} is equal to {@code x}.
     * The {@code accumulator} function must be an
     * associative function.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param identity the identity value for the accumulating function
     * @param op an associative,
     *           non-interfering,
     *           stateless
     *           function for combining two values
     * @return the result of the reduction
     * @see #sum()
     * @see #min()
     * @see #max()
     * @see #average()
     */
    int reduce(int identity, Distributed.IntBinaryOperator op);

    /**
     * Performs a reduction on the
     * elements of this stream, using an
     * associative accumulation
     * function, and returns an {@code OptionalInt} describing the reduced value,
     * if any. This is equivalent to:
     * <pre>{@code
     *     boolean foundAny = false;
     *     int result = null;
     *     for (int element : this stream) {
     *         if (!foundAny) {
     *             foundAny = true;
     *             result = element;
     *         }
     *         else
     *             result = accumulator.applyAsInt(result, element);
     *     }
     *     return foundAny ? OptionalInt.of(result) : OptionalInt.empty();
     * }</pre>
     *
     * but is not constrained to execute sequentially.
     *
     * <p>The {@code accumulator} function must be an
     * associative function.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param op an associative,
     *           non-interfering,
     *           stateless
     *           function for combining two values
     * @return the result of the reduction
     * @see #reduce(int, Distributed.IntBinaryOperator)
     */
    OptionalInt reduce(Distributed.IntBinaryOperator op);

    /**
     * Performs a mutable
     * reduction operation on the elements of this stream.  A mutable
     * reduction is one in which the reduced value is a mutable result container,
     * such as an {@code ArrayList}, and elements are incorporated by updating
     * the state of the result rather than by replacing the result.  This
     * produces a result equivalent to:
     * <pre>{@code
     *     R result = supplier.get();
     *     for (int element : this stream)
     *         accumulator.accept(result, element);
     *     return result;
     * }</pre>
     *
     * <p>Like {@link #reduce(int, Distributed.IntBinaryOperator)}, {@code collect} operations
     * can be parallelized without requiring additional synchronization.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param <R> type of the result
     * @param supplier a function that creates a new result container. For a
     *                 parallel execution, this function may be called
     *                 multiple times and must return a fresh value each time.
     * @param accumulator an associative,
     *                    non-interfering,
     *                    stateless
     *                    function for incorporating an additional element into a result
     * @param combiner an associative,
     *                    non-interfering,
     *                    stateless
     *                    function for combining two values, which must be
     *                    compatible with the accumulator function
     * @return the result of the reduction
     * @see DistributedStream#collect(Distributed.Supplier, Distributed.BiConsumer, Distributed.BiConsumer)
     */
    <R> R collect(Distributed.Supplier<R> supplier, Distributed.ObjIntConsumer<R> accumulator,
                  Distributed.BiConsumer<R, R> combiner);

    /**
     * Returns whether any elements of this stream match the provided
     * predicate.  May not evaluate the predicate on all elements if not
     * necessary for determining the result.  If the stream is empty then
     * {@code false} is returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if any elements of the stream match the provided
     * predicate, otherwise {@code false}
     */
    boolean anyMatch(Distributed.IntPredicate predicate);

    /**
     * Returns whether all elements of this stream match the provided predicate.
     * May not evaluate the predicate on all elements if not necessary for
     * determining the result.  If the stream is empty then {@code true} is
     * returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if either all elements of the stream match the
     * provided predicate or the stream is empty, otherwise {@code false}
     */
    boolean allMatch(Distributed.IntPredicate predicate);

    /**
     * Returns whether no elements of this stream match the provided predicate.
     * May not evaluate the predicate on all elements if not necessary for
     * determining the result.  If the stream is empty then {@code true} is
     * returned and the predicate is not evaluated.
     *
     * <p>This is a short-circuiting
     * terminal operation.
     *
     * @param predicate a non-interfering,
     *                  stateless
     *                  predicate to apply to elements of this stream
     * @return {@code true} if either no elements of the stream match the
     * provided predicate or the stream is empty, otherwise {@code false}
     */
    boolean noneMatch(Distributed.IntPredicate predicate);

    /**
     * Returns a {@code LongStream} consisting of the elements of this stream,
     * converted to {@code long}.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a {@code LongStream} consisting of the elements of this stream,
     * converted to {@code long}
     */
    @Override
    DistributedLongStream asLongStream();

    /**
     * Returns a {@code DoubleStream} consisting of the elements of this stream,
     * converted to {@code double}.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a {@code DoubleStream} consisting of the elements of this stream,
     * converted to {@code double}
     */
    @Override
    DistributedDoubleStream asDoubleStream();

    /**
     * Returns a {@code Stream} consisting of the elements of this stream,
     * each boxed to an {@code Integer}.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a {@code Stream} consistent of the elements of this stream,
     * each boxed to an {@code Integer}
     */
    @Override
    DistributedStream<Integer> boxed();

    /**
     * Returns an equivalent stream that is sequential.  May return
     * itself, either because the stream was already sequential, or because
     * the underlying stream state was modified to be sequential.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a sequential stream
     */
    @Override
    DistributedIntStream sequential();

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
    DistributedIntStream parallel();

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
