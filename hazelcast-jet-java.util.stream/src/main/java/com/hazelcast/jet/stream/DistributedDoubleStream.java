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
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * An extension of {@link java.util.stream.DoubleStream} to support distributed stream operations by replacing
 * functional interfaces with their serializable equivalents.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface DistributedDoubleStream extends DoubleStream {

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
    DistributedDoubleStream filter(Distributed.DoublePredicate predicate);

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
    DistributedDoubleStream map(Distributed.DoubleUnaryOperator mapper);

    /**
     * Returns an object-valued {@code Stream} consisting of the results of
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
    <U> DistributedStream<U> mapToObj(Distributed.DoubleFunction<? extends U> mapper);

    /**
     * Returns a {@code LongStream} consisting of the results of applying the
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
    DistributedLongStream mapToLong(Distributed.DoubleToLongFunction mapper);

    /**
     * Returns an {@code IntStream} consisting of the results of applying the
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
    DistributedIntStream mapToInt(Distributed.DoubleToIntFunction mapper);

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
     *               function to apply to each element which produces a
     *               {@code DoubleStream} of new values
     * @return the new stream
     * @see DistributedStream#flatMap(Distributed.Function)
     */
    DistributedDoubleStream flatMap(Distributed.DoubleFunction<? extends DoubleStream> mapper);

    /**
     * Returns a stream consisting of the distinct elements of this stream. The
     * elements are compared for equality according to
     * {@link java.lang.Double#compare(double, double)}.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @return the result stream
     */
    DistributedDoubleStream distinct();

    /**
     * Returns a stream consisting of the elements of this stream in sorted
     * order. The elements are compared for equality according to
     * {@link java.lang.Double#compare(double, double)}.
     *
     * <p>This is a stateful
     * intermediate operation.
     *
     * @return the result stream
     */
    DistributedDoubleStream sorted();

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
    DistributedDoubleStream peek(Distributed.DoubleConsumer action);

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
    DistributedDoubleStream limit(long maxSize);

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
    DistributedDoubleStream skip(long n);

    /**
     * Performs an action for each element of this stream.
     *
     * <p>This is a terminal
     * operation.
     *
     * @param action a
     *               non-interfering action to perform on the elements
     */
    void forEach(Distributed.DoubleConsumer action);

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
     * @see #forEach(DoubleConsumer)
     */
    void forEachOrdered(Distributed.DoubleConsumer action);

    /**
     * Performs a reduction on the
     * elements of this stream, using the provided identity value and an
     * associative
     * accumulation function, and returns the reduced value.  This is equivalent
     * to:
     * <pre>{@code
     *     double result = identity;
     *     for (double element : this stream)
     *         result = accumulator.applyAsDouble(result, element)
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
    double reduce(double identity, Distributed.DoubleBinaryOperator op);

    /**
     * Performs a reduction on the
     * elements of this stream, using an
     * associative accumulation
     * function, and returns an {@code OptionalDouble} describing the reduced
     * value, if any. This is equivalent to:
     * <pre>{@code
     *     boolean foundAny = false;
     *     double result = null;
     *     for (double element : this stream) {
     *         if (!foundAny) {
     *             foundAny = true;
     *             result = element;
     *         }
     *         else
     *             result = accumulator.applyAsDouble(result, element);
     *     }
     *     return foundAny ? OptionalDouble.of(result) : OptionalDouble.empty();
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
     * @see #reduce(double, DoubleBinaryOperator)
     */
    OptionalDouble reduce(Distributed.DoubleBinaryOperator op);

    /**
     * Performs a mutable
     * reduction operation on the elements of this stream.  A mutable
     * reduction is one in which the reduced value is a mutable result container,
     * such as an {@code ArrayList}, and elements are incorporated by updating
     * the state of the result rather than by replacing the result.  This
     * produces a result equivalent to:
     * <pre>{@code
     *     R result = supplier.get();
     *     for (double element : this stream)
     *         accumulator.accept(result, element);
     *     return result;
     * }</pre>
     *
     * <p>Like {@link #reduce(double, DoubleBinaryOperator)}, {@code collect}
     * operations can be parallelized without requiring additional
     * synchronization.
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
     * @see Stream#collect(Supplier, BiConsumer, BiConsumer)
     */
    <R> R collect(Distributed.Supplier<R> supplier, Distributed.ObjDoubleConsumer<R> accumulator,
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
    boolean anyMatch(Distributed.DoublePredicate predicate);

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
    boolean allMatch(Distributed.DoublePredicate predicate);

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
    boolean noneMatch(Distributed.DoublePredicate predicate);

    /**
     * Returns a {@code DistributedStream} consisting of the elements of this stream,
     * boxed to {@code Double}.
     *
     * <p>This is an intermediate
     * operation.
     *
     * @return a {@code DistributedStream} consistent of the elements of this stream,
     * each boxed to a {@code Double}
     */
    @Override
    DistributedStream<Double> boxed();

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
    DistributedDoubleStream sequential();

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
    DistributedDoubleStream parallel();

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
