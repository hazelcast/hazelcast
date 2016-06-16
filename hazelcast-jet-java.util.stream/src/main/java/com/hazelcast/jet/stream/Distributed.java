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

import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.collectors.DistributedCollectorImpl;
import com.hazelcast.jet.stream.impl.distributed.DistributedComparators;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Enclosing class for serializable versions of {@code java.util.function} interfaces.
 */
public final class Distributed {

    private Distributed() {
    }

    /**
     * Represents an operation that accepts two input arguments and returns no
     * result.  This is the two-arity specialization of {@link Distributed.Consumer}.
     * Unlike most other functional interfaces, {@code BiConsumer} is expected
     * to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(Object, Object)}.
     *
     * @param <T> the type of the first argument to the operation
     * @param <U> the type of the second argument to the operation
     * @see Distributed.Consumer
     */
    @FunctionalInterface
    public interface BiConsumer<T, U> extends java.util.function.BiConsumer<T, U>, Serializable {

        /**
         * Returns a composed {@code BiConsumer} that performs, in sequence, this
         * operation followed by the {@code after} operation. If performing either
         * operation throws an exception, it is relayed to the caller of the
         * composed operation.  If performing this operation throws an exception,
         * the {@code after} operation will not be performed.
         *
         * @param after the operation to perform after this operation
         * @return a composed {@code BiConsumer} that performs in sequence this
         * operation followed by the {@code after} operation
         * @throws NullPointerException if {@code after} is null
         */
        default BiConsumer<T, U> andThen(BiConsumer<? super T, ? super U> after) {
            Objects.requireNonNull(after);

            return (l, r) -> {
                accept(l, r);
                after.accept(l, r);
            };
        }
    }

    /**
     * Represents a function that accepts two arguments and produces a result.
     * This is the two-arity specialization of {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(Object, Object)}.
     *
     * @param <T> the type of the first argument to the function
     * @param <U> the type of the second argument to the function
     * @param <R> the type of the result of the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface BiFunction<T, U, R> extends java.util.function.BiFunction<T, U, R>, Serializable {

        /**
         * Returns a composed function that first applies this function to
         * its input, and then applies the {@code after} function to the result.
         * If evaluation of either function throws an exception, it is relayed to
         * the caller of the composed function.
         *
         * @param <V>   the type of output of the {@code after} function, and of the
         *              composed function
         * @param after the function to apply after this function is applied
         * @return a composed function that first applies this function and then
         * applies the {@code after} function
         * @throws NullPointerException if after is null
         */
        default <V> BiFunction<T, U, V> andThen(Function<? super R, ? extends V> after) {
            Objects.requireNonNull(after);
            return (T t, U u) -> after.apply(apply(t, u));
        }
    }

    /**
     * Represents an operation upon two operands of the same type, producing a result
     * of the same type as the operands.  This is a specialization of
     * {@link Distributed.BiFunction} for the case where the operands and the result are all of
     * the same type.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(Object, Object)}.
     *
     * @param <T> the type of the operands and result of the operator
     * @see Distributed.BiFunction
     * @see Distributed.UnaryOperator
     */
    @FunctionalInterface
    public interface BinaryOperator<T> extends java.util.function.BinaryOperator<T>, Serializable {

        /**
         * Returns a {@link Distributed.BinaryOperator} which returns the lesser of two elements
         * according to the specified {@code Comparator}.
         *
         * @param <T>        the type of the input arguments of the comparator
         * @param comparator a {@code Comparator} for comparing the two values
         * @return a {@code BinaryOperator} which returns the lesser of its operands,
         * according to the supplied {@code Comparator}
         * @throws NullPointerException if the argument is null
         */
        static <T> BinaryOperator<T> minBy(java.util.Comparator<? super T> comparator) {
            Objects.requireNonNull(comparator);
            return (a, b) -> comparator.compare(a, b) <= 0 ? a : b;
        }

        /**
         * Returns a {@link Distributed.BinaryOperator} which returns the greater of two elements
         * according to the specified {@code Comparator}.
         *
         * @param <T>        the type of the input arguments of the comparator
         * @param comparator a {@code Comparator} for comparing the two values
         * @return a {@code BinaryOperator} which returns the greater of its operands,
         * according to the supplied {@code Comparator}
         * @throws NullPointerException if the argument is null
         */
        static <T> BinaryOperator<T> maxBy(java.util.Comparator<? super T> comparator) {
            Objects.requireNonNull(comparator);
            return (a, b) -> comparator.compare(a, b) >= 0 ? a : b;
        }
    }

    /**
     * Represents an operation upon two {@code int}-valued operands and producing an
     * {@code int}-valued result.   This is the primitive type specialization of
     * {@link Distributed.BinaryOperator} for {@code int}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsInt(int, int)}.
     *
     * @see Distributed.BinaryOperator
     * @see Distributed.IntUnaryOperator
     */
    @FunctionalInterface
    public interface IntBinaryOperator extends java.util.function.IntBinaryOperator, Serializable {
    }

    /**
     * Represents an operation upon two {@code double}-valued operands and producing a
     * {@code double}-valued result.   This is the primitive type specialization of
     * {@link Distributed.BinaryOperator} for {@code double}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsDouble(double, double)}.
     *
     * @see Distributed.BinaryOperator
     * @see Distributed.DoubleUnaryOperator
     */
    @FunctionalInterface
    public interface DoubleBinaryOperator extends java.util.function.DoubleBinaryOperator, Serializable {
    }

    /**
     * Represents an operation upon two {@code long}-valued operands and producing a
     * {@code long}-valued result.   This is the primitive type specialization of
     * {@link Distributed.BinaryOperator} for {@code long}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsLong(long, long)}.
     *
     * @see Distributed.BinaryOperator
     * @see Distributed.LongUnaryOperator
     */
    @FunctionalInterface
    public interface LongBinaryOperator extends java.util.function.LongBinaryOperator, Serializable {
    }

    /**
     * Represents a predicate (boolean-valued function) of two arguments.  This is
     * the two-arity specialization of {@link Distributed.Predicate}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #test(Object, Object)}.
     *
     * @param <T> the type of the first argument to the predicate
     * @param <U> the type of the second argument the predicate
     * @see Distributed.Predicate
     */
    @FunctionalInterface
    public interface BiPredicate<T, U> extends java.util.function.BiPredicate<T, U>, Serializable {
        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * AND of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code false}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ANDed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * AND of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default BiPredicate<T, U> and(BiPredicate<? super T, ? super U> other) {
            Objects.requireNonNull(other);
            return (T t, U u) -> test(t, u) && other.test(t, u);
        }

        /**
         * Returns a predicate that represents the logical negation of this
         * predicate.
         *
         * @return a predicate that represents the logical negation of this
         * predicate
         */
        default BiPredicate<T, U> negate() {
            return (T t, U u) -> !test(t, u);
        }

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * OR of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code true}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ORed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * OR of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default BiPredicate<T, U> or(BiPredicate<? super T, ? super U> other) {
            Objects.requireNonNull(other);
            return (T t, U u) -> test(t, u) || other.test(t, u);
        }
    }

    /**
     * Represents a supplier of {@code boolean}-valued results.  This is the
     * {@code boolean}-producing primitive specialization of {@link Distributed.Supplier}.
     *
     * <p>There is no requirement that a new or distinct result be returned each
     * time the supplier is invoked.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #getAsBoolean()}.
     *
     * @see Distributed.Supplier
     */
    @FunctionalInterface
    public interface BooleanSupplier extends java.util.function.BooleanSupplier, Serializable {
    }

    /**
     * Represents an operation that accepts a single input argument and returns no
     * result. Unlike most other functional interfaces, {@code Consumer} is expected
     * to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(Object)}.
     *
     * @param <T> the type of the input to the operation
     */
    @FunctionalInterface
    public interface Consumer<T> extends java.util.function.Consumer<T>, Serializable {
        /**
         * Returns a composed {@code Consumer} that performs, in sequence, this
         * operation followed by the {@code after} operation. If performing either
         * operation throws an exception, it is relayed to the caller of the
         * composed operation.  If performing this operation throws an exception,
         * the {@code after} operation will not be performed.
         *
         * @param after the operation to perform after this operation
         * @return a composed {@code Consumer} that performs in sequence this
         * operation followed by the {@code after} operation
         * @throws NullPointerException if {@code after} is null
         */
        default Consumer<T> andThen(Consumer<? super T> after) {
            Objects.requireNonNull(after);
            return (T t) -> {
                accept(t);
                after.accept(t);
            };
        }
    }

    /**
     * Represents an operation that accepts a single {@code int}-valued argument and
     * returns no result.  This is the primitive type specialization of
     * {@link Distributed.Consumer} for {@code int}.  Unlike most other functional interfaces,
     * {@code IntConsumer} is expected to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(int)}.
     *
     * @see Distributed.Consumer
     */
    @FunctionalInterface
    public interface IntConsumer extends java.util.function.IntConsumer, Serializable {
        /**
         * Returns a composed {@code IntConsumer} that performs, in sequence, this
         * operation followed by the {@code after} operation. If performing either
         * operation throws an exception, it is relayed to the caller of the
         * composed operation.  If performing this operation throws an exception,
         * the {@code after} operation will not be performed.
         *
         * @param after the operation to perform after this operation
         * @return a composed {@code IntConsumer} that performs in sequence this
         * operation followed by the {@code after} operation
         * @throws NullPointerException if {@code after} is null
         */
        default IntConsumer andThen(IntConsumer after) {
            Objects.requireNonNull(after);
            return (int t) -> {
                accept(t);
                after.accept(t);
            };
        }
    }

    /**
     * Represents an operation that accepts a single {@code double}-valued argument and
     * returns no result.  This is the primitive type specialization of
     * {@link Distributed.Consumer} for {@code double}.  Unlike most other functional interfaces,
     * {@code DoubleConsumer} is expected to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(double)}.
     *
     * @see Distributed.Consumer
     */
    @FunctionalInterface
    public interface DoubleConsumer extends java.util.function.DoubleConsumer, Serializable {
        /**
         * Returns a composed {@code DoubleConsumer} that performs, in sequence, this
         * operation followed by the {@code after} operation. If performing either
         * operation throws an exception, it is relayed to the caller of the
         * composed operation.  If performing this operation throws an exception,
         * the {@code after} operation will not be performed.
         *
         * @param after the operation to perform after this operation
         * @return a composed {@code DoubleConsumer} that performs in sequence this
         * operation followed by the {@code after} operation
         * @throws NullPointerException if {@code after} is null
         */
        default DoubleConsumer andThen(DoubleConsumer after) {
            Objects.requireNonNull(after);
            return (double t) -> {
                accept(t);
                after.accept(t);
            };
        }
    }

    /**
     * Represents an operation that accepts a single {@code long}-valued argument and
     * returns no result.  This is the primitive type specialization of
     * {@link Distributed.Consumer} for {@code long}.  Unlike most other functional interfaces,
     * {@code LongConsumer} is expected to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(long)}.
     *
     * @see Distributed.Consumer
     */
    @FunctionalInterface
    public interface LongConsumer extends java.util.function.LongConsumer, Serializable {
        /**
         * Returns a composed {@code LongConsumer} that performs, in sequence, this
         * operation followed by the {@code after} operation. If performing either
         * operation throws an exception, it is relayed to the caller of the
         * composed operation.  If performing this operation throws an exception,
         * the {@code after} operation will not be performed.
         *
         * @param after the operation to perform after this operation
         * @return a composed {@code LongConsumer} that performs in sequence this
         * operation followed by the {@code after} operation
         * @throws NullPointerException if {@code after} is null
         */
        default LongConsumer andThen(LongConsumer after) {
            Objects.requireNonNull(after);
            return (long t) -> {
                accept(t);
                after.accept(t);
            };
        }
    }

    /**
     * Represents an operation that accepts an object-valued and a
     * {@code int}-valued argument, and returns no result.  This is the
     * {@code (reference, int)} specialization of {@link Distributed.BiConsumer}.
     * Unlike most other functional interfaces, {@code ObjIntConsumer} is
     * expected to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(Object, int)}.
     *
     * @param <T> the type of the object argument to the operation
     * @see Distributed.BiConsumer
     */
    @FunctionalInterface
    public interface ObjIntConsumer<T> extends java.util.function.ObjIntConsumer<T>, Serializable {
    }

    /**
     * Represents an operation that accepts an object-valued and a
     * {@code long}-valued argument, and returns no result.  This is the
     * {@code (reference, long)} specialization of {@link Distributed.BiConsumer}.
     * Unlike most other functional interfaces, {@code ObjLongConsumer} is
     * expected to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(Object, long)}.
     *
     * @param <T> the type of the object argument to the operation
     * @see Distributed.BiConsumer
     */
    @FunctionalInterface
    public interface ObjLongConsumer<T> extends java.util.function.ObjLongConsumer<T>, Serializable {
    }

    /**
     * Represents an operation that accepts an object-valued and a
     * {@code double}-valued argument, and returns no result.  This is the
     * {@code (reference, double)} specialization of {@link Distributed.BiConsumer}.
     * Unlike most other functional interfaces, {@code ObjDoubleConsumer} is
     * expected to operate via side-effects.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #accept(Object, double)}.
     *
     * @param <T> the type of the object argument to the operation
     * @see Distributed.BiConsumer
     */
    @FunctionalInterface
    public interface ObjDoubleConsumer<T> extends java.util.function.ObjDoubleConsumer<T>, Serializable {
    }

    /**
     * Represents a function that accepts a double-valued argument and produces a
     * result.  This is the {@code double}-consuming primitive specialization for
     * {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(double)}.
     *
     * @param <R> the type of the result of the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface DoubleFunction<R> extends java.util.function.DoubleFunction<R>, Serializable {
    }

    /**
     * Represents a function that accepts a double-valued argument and produces an
     * int-valued result.  This is the {@code double}-to-{@code int} primitive
     * specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsInt(double)}.
     *
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface DoubleToIntFunction extends java.util.function.DoubleToIntFunction, Serializable {
    }

    /**
     * Represents a function that accepts a double-valued argument and produces a
     * long-valued result.  This is the {@code double}-to-{@code long} primitive
     * specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsLong(double)}.
     *
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface DoubleToLongFunction extends java.util.function.DoubleToLongFunction, Serializable {
    }

    /**
     * Represents a function that accepts one argument and produces a result.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(Object)}.
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     */
    @FunctionalInterface
    public interface Function<T, R> extends java.util.function.Function<T, R>, Serializable {
        /**
         * Returns a composed function that first applies the {@code before}
         * function to its input, and then applies this function to the result.
         * If evaluation of either function throws an exception, it is relayed to
         * the caller of the composed function.
         *
         * @param <V>    the type of input to the {@code before} function, and to the
         *               composed function
         * @param before the function to apply before this function is applied
         * @return a composed function that first applies the {@code before}
         * function and then applies this function
         * @throws NullPointerException if before is null
         * @see #andThen(Function)
         */
        default <V> Function<V, R> compose(Function<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return (V v) -> apply(before.apply(v));
        }

        /**
         * Returns a composed function that first applies this function to
         * its input, and then applies the {@code after} function to the result.
         * If evaluation of either function throws an exception, it is relayed to
         * the caller of the composed function.
         *
         * @param <V>   the type of output of the {@code after} function, and of the
         *              composed function
         * @param after the function to apply after this function is applied
         * @return a composed function that first applies this function and then
         * applies the {@code after} function
         * @throws NullPointerException if after is null
         * @see #compose(Function)
         */
        default <V> Function<T, V> andThen(Function<? super R, ? extends V> after) {
            Objects.requireNonNull(after);
            return (T t) -> after.apply(apply(t));
        }

        /**
         * Returns a function that always returns its input argument.
         *
         * @param <T> the type of the input and output objects to the function
         * @return a function that always returns its input argument
         */
        static <T> Function<T, T> identity() {
            return t -> t;
        }
    }

    /**
     * Represents a function that accepts an int-valued argument and produces a
     * result.  This is the {@code int}-consuming primitive specialization for
     * {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(int)}.
     *
     * @param <R> the type of the result of the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface IntFunction<R> extends java.util.function.IntFunction<R>, Serializable {
    }

    /**
     * Represents a function that accepts an int-valued argument and produces a
     * double-valued result.  This is the {@code int}-to-{@code double} primitive
     * specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsDouble(int)}.
     *
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface IntToDoubleFunction extends java.util.function.IntToDoubleFunction, Serializable {
    }

    /**
     * Represents a function that accepts an int-valued argument and produces a
     * long-valued result.  This is the {@code int}-to-{@code long} primitive
     * specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsLong(int)}.
     *
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface IntToLongFunction extends java.util.function.IntToLongFunction, Serializable {
    }

    /**
     * Represents a function that accepts a long-valued argument and produces a
     * result.  This is the {@code long}-consuming primitive specialization for
     * {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(long)}.
     *
     * @param <R> the type of the result of the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface LongFunction<R> extends java.util.function.LongFunction<R>, Serializable {
    }

    /**
     * Represents a function that accepts a long-valued argument and produces a
     * double-valued result.  This is the {@code long}-to-{@code double} primitive
     * specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsDouble(long)}.
     *
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface LongToDoubleFunction extends java.util.function.LongToDoubleFunction, Serializable {
    }

    /**
     * Represents a function that accepts a long-valued argument and produces an
     * int-valued result.  This is the {@code long}-to-{@code int} primitive
     * specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsInt(long)}.
     *
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface LongToIntFunction extends java.util.function.LongToIntFunction, Serializable {
    }

    /**
     * Represents a predicate (boolean-valued function) of one argument.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #test(Object)}.
     *
     * @param <T> the type of the input to the predicate
     */
    @FunctionalInterface
    public interface Predicate<T> extends java.util.function.Predicate<T>, Serializable {
        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * AND of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code false}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ANDed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * AND of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default Predicate<T> and(Predicate<? super T> other) {
            Objects.requireNonNull(other);
            return (t) -> test(t) && other.test(t);
        }

        /**
         * Returns a predicate that represents the logical negation of this
         * predicate.
         *
         * @return a predicate that represents the logical negation of this
         * predicate
         */
        default Predicate<T> negate() {
            return (t) -> !test(t);
        }

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * OR of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code true}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ORed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * OR of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default Predicate<T> or(Predicate<? super T> other) {
            Objects.requireNonNull(other);
            return (t) -> test(t) || other.test(t);
        }

        /**
         * Returns a predicate that tests if two arguments are equal according
         * to {@link Objects#equals(Object, Object)}.
         *
         * @param <T>       the type of arguments to the predicate
         * @param targetRef the object reference with which to compare for equality,
         *                  which may be {@code null}
         * @return a predicate that tests if two arguments are equal according
         * to {@link Objects#equals(Object, Object)}
         */
        static <T> Predicate<T> isEqual(Object targetRef) {
            return (null == targetRef)
                    ? Objects::isNull
                    : object -> targetRef.equals(object);
        }
    }

    /**
     * Represents a predicate (boolean-valued function) of one {@code int}-valued
     * argument. This is the {@code int}-consuming primitive type specialization of
     * {@link Distributed.Predicate}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #test(int)}.
     *
     * @see Distributed.Predicate
     */
    @FunctionalInterface
    public interface IntPredicate extends java.util.function.IntPredicate, Serializable {
        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * AND of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code false}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ANDed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * AND of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default IntPredicate and(IntPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) && other.test(value);
        }

        /**
         * Returns a predicate that represents the logical negation of this
         * predicate.
         *
         * @return a predicate that represents the logical negation of this
         * predicate
         */
        default IntPredicate negate() {
            return (value) -> !test(value);
        }

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * OR of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code true}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ORed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * OR of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default IntPredicate or(IntPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) || other.test(value);
        }
    }

    /**
     * Represents a predicate (boolean-valued function) of one {@code double}-valued
     * argument. This is the {@code double}-consuming primitive type specialization
     * of {@link Distributed.Predicate}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #test(double)}.
     *
     * @see Distributed.Predicate
     */
    @FunctionalInterface
    public interface DoublePredicate extends java.util.function.DoublePredicate, Serializable {
        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * AND of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code false}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ANDed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * AND of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default DoublePredicate and(DoublePredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) && other.test(value);
        }

        /**
         * Returns a predicate that represents the logical negation of this
         * predicate.
         *
         * @return a predicate that represents the logical negation of this
         * predicate
         */
        default DoublePredicate negate() {
            return (value) -> !test(value);
        }

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * OR of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code true}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ORed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * OR of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default DoublePredicate or(DoublePredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) || other.test(value);
        }
    }

    /**
     * Represents a predicate (boolean-valued function) of one {@code long}-valued
     * argument. This is the {@code long}-consuming primitive type specialization of
     * {@link Distributed.Predicate}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #test(long)}.
     *
     * @see Distributed.Predicate
     */
    @FunctionalInterface
    public interface LongPredicate extends java.util.function.LongPredicate, Serializable {
        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * AND of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code false}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ANDed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * AND of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default LongPredicate and(LongPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) && other.test(value);
        }

        /**
         * Returns a predicate that represents the logical negation of this
         * predicate.
         *
         * @return a predicate that represents the logical negation of this
         * predicate
         */
        default LongPredicate negate() {
            return (value) -> !test(value);
        }

        /**
         * Returns a composed predicate that represents a short-circuiting logical
         * OR of this predicate and another.  When evaluating the composed
         * predicate, if this predicate is {@code true}, then the {@code other}
         * predicate is not evaluated.
         *
         * <p>Any exceptions thrown during evaluation of either predicate are relayed
         * to the caller; if evaluation of this predicate throws an exception, the
         * {@code other} predicate will not be evaluated.
         *
         * @param other a predicate that will be logically-ORed with this
         *              predicate
         * @return a composed predicate that represents the short-circuiting logical
         * OR of this predicate and the {@code other} predicate
         * @throws NullPointerException if other is null
         */
        default LongPredicate or(LongPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) || other.test(value);
        }
    }

    /**
     * Represents a supplier of results.
     *
     * <p>There is no requirement that a new or distinct result be returned each
     * time the supplier is invoked.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #get()}.
     *
     * @param <T> the type of results supplied by this supplier
     */
    @FunctionalInterface
    public interface Supplier<T> extends java.util.function.Supplier<T>, Serializable {
    }

    /**
     * Represents a function that accepts two arguments and produces a double-valued
     * result.  This is the {@code double}-producing primitive specialization for
     * {@link Distributed.BiFunction}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsDouble(Object, Object)}.
     *
     * @param <T> the type of the first argument to the function
     * @param <U> the type of the second argument to the function
     * @see Distributed.BiFunction
     */
    @FunctionalInterface
    public interface ToDoubleBiFunction<T, U> extends java.util.function.ToDoubleBiFunction<T, U>, Serializable {
    }

    /**
     * Represents a function that produces a double-valued result.  This is the
     * {@code double}-producing primitive specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsDouble(Object)}.
     *
     * @param <T> the type of the input to the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface ToDoubleFunction<T> extends java.util.function.ToDoubleFunction<T>, Serializable {
    }

    /**
     * Represents a function that accepts two arguments and produces an int-valued
     * result.  This is the {@code int}-producing primitive specialization for
     * {@link Distributed.BiFunction}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsInt(Object, Object)}.
     *
     * @param <T> the type of the first argument to the function
     * @param <U> the type of the second argument to the function
     * @see Distributed.BiFunction
     */
    @FunctionalInterface
    public interface ToIntBiFunction<T, U> extends java.util.function.ToIntBiFunction<T, U>, Serializable {
    }

    /**
     * Represents a function that accepts two arguments and produces a long-valued
     * result.  This is the {@code long}-producing primitive specialization for
     * {@link Distributed.BiFunction}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsLong(Object, Object)}.
     *
     * @param <T> the type of the first argument to the function
     * @param <U> the type of the second argument to the function
     * @see Distributed.BiFunction
     */
    @FunctionalInterface
    public interface ToLongBiFunction<T, U> extends java.util.function.ToLongBiFunction<T, U>, Serializable {
    }

    /**
     * Represents a function that produces an int-valued result.  This is the
     * {@code int}-producing primitive specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsInt(Object)}.
     *
     * @param <T> the type of the input to the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface ToIntFunction<T> extends java.util.function.ToIntFunction<T>, Serializable {
    }

    /**
     * Represents a function that produces a long-valued result.  This is the
     * {@code long}-producing primitive specialization for {@link Distributed.Function}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsLong(Object)}.
     *
     * @param <T> the type of the input to the function
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface ToLongFunction<T> extends java.util.function.ToLongFunction<T>, Serializable {
    }

    /**
     * Represents an operation on a single operand that produces a result of the
     * same type as its operand.  This is a specialization of {@code Function} for
     * the case where the operand and result are of the same type.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #apply(Object)}.
     *
     * @param <T> the type of the operand and result of the operator
     * @see Distributed.Function
     */
    @FunctionalInterface
    public interface UnaryOperator<T> extends Function<T, T>, java.util.function.UnaryOperator<T>, Serializable {
        /**
         * Returns a unary operator that always returns its input argument.
         *
         * @param <T> the type of the input and output of the operator
         * @return a unary operator that always returns its input argument
         */
        static <T> UnaryOperator<T> identity() {
            return t -> t;
        }
    }

    /**
     * Represents an operation on a single {@code int}-valued operand that produces
     * an {@code int}-valued result.  This is the primitive type specialization of
     * {@link Distributed.UnaryOperator} for {@code int}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsInt(int)}.
     *
     * @see Distributed.UnaryOperator
     */
    @FunctionalInterface
    public interface IntUnaryOperator extends java.util.function.IntUnaryOperator, Serializable {
        /**
         * Returns a composed operator that first applies the {@code before}
         * operator to its input, and then applies this operator to the result.
         * If evaluation of either operator throws an exception, it is relayed to
         * the caller of the composed operator.
         *
         * @param before the operator to apply before this operator is applied
         * @return a composed operator that first applies the {@code before}
         * operator and then applies this operator
         * @throws NullPointerException if before is null
         * @see #andThen(IntUnaryOperator)
         */
        default IntUnaryOperator compose(IntUnaryOperator before) {
            Objects.requireNonNull(before);
            return (int v) -> applyAsInt(before.applyAsInt(v));
        }

        /**
         * Returns a composed operator that first applies this operator to
         * its input, and then applies the {@code after} operator to the result.
         * If evaluation of either operator throws an exception, it is relayed to
         * the caller of the composed operator.
         *
         * @param after the operator to apply after this operator is applied
         * @return a composed operator that first applies this operator and then
         * applies the {@code after} operator
         * @throws NullPointerException if after is null
         * @see #compose(IntUnaryOperator)
         */
        default IntUnaryOperator andThen(IntUnaryOperator after) {
            Objects.requireNonNull(after);
            return (int t) -> after.applyAsInt(applyAsInt(t));
        }

        /**
         * Returns a unary operator that always returns its input argument.
         *
         * @return a unary operator that always returns its input argument
         */
        static IntUnaryOperator identity() {
            return t -> t;
        }
    }

    /**
     * Represents an operation on a single {@code long}-valued operand that produces
     * a {@code long}-valued result.  This is the primitive type specialization of
     * {@link Distributed.UnaryOperator} for {@code long}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsLong(long)}.
     *
     * @see Distributed.UnaryOperator
     */
    @FunctionalInterface
    public interface LongUnaryOperator extends java.util.function.LongUnaryOperator, Serializable {
        /**
         * Returns a composed operator that first applies the {@code before}
         * operator to its input, and then applies this operator to the result.
         * If evaluation of either operator throws an exception, it is relayed to
         * the caller of the composed operator.
         *
         * @param before the operator to apply before this operator is applied
         * @return a composed operator that first applies the {@code before}
         * operator and then applies this operator
         * @throws NullPointerException if before is null
         * @see #andThen(LongUnaryOperator)
         */
        default LongUnaryOperator compose(LongUnaryOperator before) {
            Objects.requireNonNull(before);
            return (long v) -> applyAsLong(before.applyAsLong(v));
        }

        /**
         * Returns a composed operator that first applies this operator to
         * its input, and then applies the {@code after} operator to the result.
         * If evaluation of either operator throws an exception, it is relayed to
         * the caller of the composed operator.
         *
         * @param after the operator to apply after this operator is applied
         * @return a composed operator that first applies this operator and then
         * applies the {@code after} operator
         * @throws NullPointerException if after is null
         * @see #compose(LongUnaryOperator)
         */
        default LongUnaryOperator andThen(LongUnaryOperator after) {
            Objects.requireNonNull(after);
            return (long t) -> after.applyAsLong(applyAsLong(t));
        }

        /**
         * Returns a unary operator that always returns its input argument.
         *
         * @return a unary operator that always returns its input argument
         */
        static LongUnaryOperator identity() {
            return t -> t;
        }
    }

    /**
     * Represents an operation on a single {@code double}-valued operand that produces
     * a {@code double}-valued result.  This is the primitive type specialization of
     * {@link Distributed.UnaryOperator} for {@code double}.
     *
     * <p>This is a functional interface
     * whose functional method is {@link #applyAsDouble(double)}.
     *
     * @see Distributed.UnaryOperator
     */
    @FunctionalInterface
    public interface DoubleUnaryOperator extends java.util.function.DoubleUnaryOperator, Serializable {
        /**
         * Returns a composed operator that first applies the {@code before}
         * operator to its input, and then applies this operator to the result.
         * If evaluation of either operator throws an exception, it is relayed to
         * the caller of the composed operator.
         *
         * @param before the operator to apply before this operator is applied
         * @return a composed operator that first applies the {@code before}
         * operator and then applies this operator
         * @throws NullPointerException if before is null
         * @see #andThen(DoubleUnaryOperator)
         */
        default DoubleUnaryOperator compose(DoubleUnaryOperator before) {
            Objects.requireNonNull(before);
            return (double v) -> applyAsDouble(before.applyAsDouble(v));
        }

        /**
         * Returns a composed operator that first applies this operator to
         * its input, and then applies the {@code after} operator to the result.
         * If evaluation of either operator throws an exception, it is relayed to
         * the caller of the composed operator.
         *
         * @param after the operator to apply after this operator is applied
         * @return a composed operator that first applies this operator and then
         * applies the {@code after} operator
         * @throws NullPointerException if after is null
         * @see #compose(DoubleUnaryOperator)
         */
        default DoubleUnaryOperator andThen(DoubleUnaryOperator after) {
            Objects.requireNonNull(after);
            return (double t) -> after.applyAsDouble(applyAsDouble(t));
        }

        /**
         * Returns a unary operator that always returns its input argument.
         *
         * @return a unary operator that always returns its input argument
         */
        static DoubleUnaryOperator identity() {
            return t -> t;
        }
    }

    /**
     * A comparison function, which imposes a <i>total ordering</i> on some
     * collection of objects.  Comparators can be passed to a sort method (such
     * as {@link Collections#sort(List, java.util.Comparator) Collections.sort} or {@link
     * java.util.Arrays#sort(Object[], java.util.Comparator) Arrays.sort}) to allow precise control
     * over the sort order.  Comparators can also be used to control the order of
     * certain data structures (such as {@link SortedSet sorted sets} or {@link
     * java.util.SortedMap sorted maps}), or to provide an ordering for collections of
     * objects that don't have a {@link Comparable natural ordering}.
     *
     * The ordering imposed by a comparator <tt>c</tt> on a set of elements
     * <tt>S</tt> is said to be <i>consistent with equals</i> if and only if
     * <tt>c.compare(e1, e2)==0</tt> has the same boolean value as
     * <tt>e1.equals(e2)</tt> for every <tt>e1</tt> and <tt>e2</tt> in
     * <tt>S</tt>.
     *
     * Caution should be exercised when using a comparator capable of imposing an
     * ordering inconsistent with equals to order a sorted set (or sorted map).
     * Suppose a sorted set (or sorted map) with an explicit comparator <tt>c</tt>
     * is used with elements (or keys) drawn from a set <tt>S</tt>.  If the
     * ordering imposed by <tt>c</tt> on <tt>S</tt> is inconsistent with equals,
     * the sorted set (or sorted map) will behave "strangely."  In particular the
     * sorted set (or sorted map) will violate the general contract for set (or
     * map), which is defined in terms of <tt>equals</tt>.
     *
     * For example, suppose one adds two elements {@code a} and {@code b} such that
     * {@code (a.equals(b) && c.compare(a, b) != 0)}
     * to an empty {@code TreeSet} with comparator {@code c}.
     * The second {@code add} operation will return
     * true (and the size of the tree set will increase) because {@code a} and
     * {@code b} are not equivalent from the tree set's perspective, even though
     * this is contrary to the specification of the
     * {@link Set#add Set.add} method.
     *
     * Note: It is generally a good idea for comparators to also implement
     * <tt>java.io.Serializable</tt>, as they may be used as ordering methods in
     * serializable data structures (like {@link TreeSet}, {@link TreeMap}).  In
     * order for the data structure to serialize successfully, the comparator (if
     * provided) must implement <tt>Serializable</tt>.
     *
     * For the mathematically inclined, the <i>relation</i> that defines the
     * <i>imposed ordering</i> that a given comparator <tt>c</tt> imposes on a
     * given set of objects <tt>S</tt> is:<pre>
     *       {(x, y) such that c.compare(x, y) &lt;= 0}.
     * </pre> The <i>quotient</i> for this total order is:<pre>
     *       {(x, y) such that c.compare(x, y) == 0}.
     * </pre>
     *
     * It follows immediately from the contract for <tt>compare</tt> that the
     * quotient is an <i>equivalence relation</i> on <tt>S</tt>, and that the
     * imposed ordering is a <i>total order</i> on <tt>S</tt>.  When we say that
     * the ordering imposed by <tt>c</tt> on <tt>S</tt> is <i>consistent with
     * equals</i>, we mean that the quotient for the ordering is the equivalence
     * relation defined by the objects' {@link Object#equals(Object)
     * equals(Object)} method(s):<pre>
     *     {(x, y) such that x.equals(y)}. </pre>
     *
     * <p>Unlike {@code Comparable}, a comparator may optionally permit
     * comparison of null arguments, while maintaining the requirements for
     * an equivalence relation.
     *
     * <p>This interface is a member of the
     * Java Collections Framework.
     *
     * @param <T> the type of objects that may be compared by this comparator
     * @see Comparable
     * @see java.io.Serializable
     */
    @FunctionalInterface
    public interface Comparator<T> extends java.util.Comparator<T>, Serializable {

        /**
         * Returns a comparator that compares {@link Comparable} objects in natural
         * order.
         *
         * <p>The returned comparator is serializable and throws {@link
         * NullPointerException} when comparing {@code null}.
         *
         * @param <T> the {@link Comparable} type of element to be compared
         * @return a comparator that imposes the <i>natural ordering</i> on {@code
         * Comparable} objects.
         * @see Comparable
         */
        @SuppressWarnings("unchecked")
        static <T extends Comparable<? super T>> Comparator<T> naturalOrder() {
            return (Comparator<T>) DistributedComparators.NATURAL_ORDER_COMPARATOR;
        }

        /**
         * Returns a comparator that imposes the reverse of the <em>natural
         * ordering</em>.
         *
         * <p>The returned comparator is serializable and throws {@link
         * NullPointerException} when comparing {@code null}.
         *
         * @param  <T> the {@link Comparable} type of element to be compared
         * @return a comparator that imposes the reverse of the <i>natural
         *         ordering</i> on {@code Comparable} objects.
         * @see Comparable
         */
        @SuppressWarnings("unchecked")
        static <T extends Comparable<? super T>> Comparator<T> reverseOrder() {
            return (Comparator<T>) DistributedComparators.REVERSE_ORDER_COMPARATOR;
        }
    }

    /**
     * <i>A mutable reduction operation</i> that
     * accumulates input elements into a mutable result container, optionally transforming
     * the accumulated result into a final representation after all input elements
     * have been processed.
     *
     * @param <T> the type of input elements to the reduction operation
     * @param <A> the mutable accumulation type of the reduction operation (often
     *            hidden as an implementation detail)
     * @param <R> the result type of the reduction operation
     * @see java.util.stream.Collector
     * @see DistributedCollectors
     */
    public interface Collector<T, A, R> extends java.util.stream.Collector<T, A, R>, Serializable {

        /**
         * A function that creates and returns a new mutable result container.
         *
         * @return a function which returns a new, mutable result container
         */
        @Override
        Supplier<A> supplier();

        /**
         * A function that folds a value into a mutable result container.
         *
         * @return a function which folds a value into a mutable result container
         */
        @Override
        BiConsumer<A, T> accumulator();

        /**
         * A function that accepts two partial results and merges them.  The
         * combiner function may fold state from one argument into the other and
         * return that, or may return a new result container.
         *
         * @return a function which combines two partial results into a combined
         * result
         */
        @Override
        BinaryOperator<A> combiner();

        /**
         * Perform the final transformation from the intermediate accumulation type
         * {@code A} to the final result type {@code R}.
         *
         * <p>If the characteristic {@code IDENTITY_TRANSFORM} is
         * set, this function may be presumed to be an identity transform with an
         * unchecked cast from {@code A} to {@code R}.
         *
         * @return a function which transforms the intermediate result to the final
         * result
         */
        @Override
        Function<A, R> finisher();

        /**
         * Returns a new {@code Distributed.Collector} described by the given {@code supplier},
         * {@code accumulator}, and {@code combiner} functions.  The resulting
         * {@code Distributed.Collector} has the {@code Collector.Characteristics.IDENTITY_FINISH}
         * characteristic.
         *
         * @param supplier        The supplier function for the new collector
         * @param accumulator     The accumulator function for the new collector
         * @param combiner        The combiner function for the new collector
         * @param characteristics The collector characteristics for the new
         *                        collector
         * @param <T>             The type of input elements for the new collector
         * @param <R>             The type of intermediate accumulation result, and final result,
         *                        for the new collector
         * @return the new {@code Distributed.Collector}
         * @throws NullPointerException if any argument is null
         */
        static <T, R> Collector<T, R, R> of(Supplier<R> supplier,
                                            BiConsumer<R, T> accumulator,
                                            BinaryOperator<R> combiner,
                                            Characteristics... characteristics) {
            Objects.requireNonNull(supplier);
            Objects.requireNonNull(accumulator);
            Objects.requireNonNull(combiner);
            Objects.requireNonNull(characteristics);
            Set<Characteristics> cs = (characteristics.length == 0)
                    ? DistributedCollectors.CH_ID
                    : Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH,
                    characteristics));
            return new DistributedCollectorImpl<>(supplier, accumulator, combiner, cs);
        }

        /**
         * Returns a new {@code Distributed.Collector} described by the given {@code supplier},
         * {@code accumulator}, {@code combiner}, and {@code finisher} functions.
         *
         * @param supplier The supplier function for the new collector
         * @param accumulator The accumulator function for the new collector
         * @param combiner The combiner function for the new collector
         * @param finisher The finisher function for the new collector
         * @param characteristics The collector characteristics for the new
         *                        collector
         * @param <T> The type of input elements for the new collector
         * @param <A> The intermediate accumulation type of the new collector
         * @param <R> The final result type of the new collector
         * @throws NullPointerException if any argument is null
         * @return the new {@code Distributed.Collector}
         */
        static <T, A, R> Collector<T, A, R> of(Supplier<A> supplier,
                                               BiConsumer<A, T> accumulator,
                                               BinaryOperator<A> combiner,
                                               Function<A, R> finisher,
                                               Characteristics... characteristics) {
            Objects.requireNonNull(supplier);
            Objects.requireNonNull(accumulator);
            Objects.requireNonNull(combiner);
            Objects.requireNonNull(finisher);
            Objects.requireNonNull(characteristics);
            Set<Characteristics> cs = DistributedCollectors.CH_NOID;
            if (characteristics.length > 0) {
                cs = EnumSet.noneOf(Characteristics.class);
                Collections.addAll(cs, characteristics);
                cs = Collections.unmodifiableSet(cs);
            }
            return new DistributedCollectorImpl<>(supplier, accumulator, combiner, finisher, cs);
        }

        /**
         * Executes the collector with the given context and upstream pipeline.
         *
         * @param context the context of the stream
         * @param upstream the upstream pipeline to execute the stream on
         * @return the result of the executed collector
         */
        R collect(StreamContext context, Pipeline<? extends T> upstream);
    }

    /**
     * A container object which may or may not contain a non-null value.
     * If a value is present, {@code isPresent()} will return {@code true} and
     * {@code get()} will return the value.
     *
     * <p>Additional methods that depend on the presence or absence of a contained
     * value are provided, such as {@link #orElse(java.lang.Object) orElse()}
     * (return a default value if value not present) and
     * {@link #ifPresent(Distributed.Consumer) ifPresent()} (execute a block
     * of code if the value is present).
     *
     * <p>This is a value-based
     * class; use of identity-sensitive operations (including reference equality
     * ({@code ==}), identity hash code, or synchronization) on instances of
     * {@code Optional} may have unpredictable results and should be avoided.
     *
     */
    public static final class Optional<T> implements Serializable {
        /**
         * Common instance for {@code empty()}.
         */
        private static final Optional<?> EMPTY = new Optional<>();

        /**
         * If non-null, the value; if null, indicates no value is present
         */
        private final T value;

        /**
         * Constructs an empty instance.
         *
         * @implNote Generally only one empty instance, {@link Optional#EMPTY},
         * should exist per VM.
         */
        private Optional() {
            this.value = null;
        }

        /**
         * Constructs an instance with the value present.
         *
         * @param value the non-null value to be present
         * @throws NullPointerException if value is null
         */
        private Optional(T value) {
            this.value = Objects.requireNonNull(value);
        }

        /**
         * Returns an empty {@code Optional} instance.  No value is present for this
         * Optional.
         *
         * @param <T> Type of the non-existent value
         * @return an empty {@code Optional}
         * @apiNote Though it may be tempting to do so, avoid testing if an object
         * is empty by comparing with {@code ==} against instances returned by
         * {@code Option.empty()}. There is no guarantee that it is a singleton.
         * Instead, use {@link #isPresent()}.
         */
        public static <T> Optional<T> empty() {
            @SuppressWarnings("unchecked")
            Optional<T> t = (Optional<T>) EMPTY;
            return t;
        }

        /**
         * Returns an {@code Optional} with the specified present non-null value.
         *
         * @param <T>   the class of the value
         * @param value the value to be present, which must be non-null
         * @return an {@code Optional} with the value present
         * @throws NullPointerException if value is null
         */
        public static <T> Optional<T> of(T value) {
            return new Optional<>(value);
        }

        /**
         * Returns an {@code Optional} describing the specified value, if non-null,
         * otherwise returns an empty {@code Optional}.
         *
         * @param <T>   the class of the value
         * @param value the possibly-null value to describe
         * @return an {@code Optional} with a present value if the specified value
         * is non-null, otherwise an empty {@code Optional}
         */
        public static <T> Optional<T> ofNullable(T value) {
            return value == null ? empty() : of(value);
        }

        /**
         * If a value is present in this {@code Optional}, returns the value,
         * otherwise throws {@code NoSuchElementException}.
         *
         * @return the non-null value held by this {@code Optional}
         * @throws NoSuchElementException if there is no value present
         * @see Optional#isPresent()
         */
        public T get() {
            if (value == null) {
                throw new NoSuchElementException("No value present");
            }
            return value;
        }

        /**
         * Return {@code true} if there is a value present, otherwise {@code false}.
         *
         * @return {@code true} if there is a value present, otherwise {@code false}
         */
        public boolean isPresent() {
            return value != null;
        }

        /**
         * If a value is present, invoke the specified consumer with the value,
         * otherwise do nothing.
         *
         * @param consumer block to be executed if a value is present
         * @throws NullPointerException if value is present and {@code consumer} is
         *                              null
         */
        public void ifPresent(Distributed.Consumer<? super T> consumer) {
            if (value != null) {
                consumer.accept(value);
            }
        }

        /**
         * If a value is present, and the value matches the given predicate,
         * return an {@code Optional} describing the value, otherwise return an
         * empty {@code Optional}.
         *
         * @param predicate a predicate to apply to the value, if present
         * @return an {@code Optional} describing the value of this {@code Optional}
         * if a value is present and the value matches the given predicate,
         * otherwise an empty {@code Optional}
         * @throws NullPointerException if the predicate is null
         */
        public Optional<T> filter(Distributed.Predicate<? super T> predicate) {
            Objects.requireNonNull(predicate);
            if (!isPresent()) {
                return this;
            } else {
                return predicate.test(value) ? this : empty();
            }
        }

        /**
         * If a value is present, apply the provided mapping function to it,
         * and if the result is non-null, return an {@code Optional} describing the
         * result.  Otherwise return an empty {@code Optional}.
         *
         * @param <U>    The type of the result of the mapping function
         * @param mapper a mapping function to apply to the value, if present
         * @return an {@code Optional} describing the result of applying a mapping
         * function to the value of this {@code Optional}, if a value is present,
         * otherwise an empty {@code Optional}
         * @throws NullPointerException if the mapping function is null
         * @apiNote This method supports post-processing on optional values, without
         * the need to explicitly check for a return status.  For example, the
         * following code traverses a stream of file names, selects one that has
         * not yet been processed, and then opens that file, returning an
         * {@code Optional<FileInputStream>}:
         *
         * <pre>{@code
         *     Optional<FileInputStream> fis =
         *         names.stream().filter(name -> !isProcessedYet(name))
         *                       .findFirst()
         *                       .map(name -> new FileInputStream(name));
         * }</pre>
         *
         * Here, {@code findFirst} returns an {@code Optional<String>}, and then
         * {@code map} returns an {@code Optional<FileInputStream>} for the desired
         * file if one exists.
         */
        public <U> Optional<U> map(Distributed.Function<? super T, ? extends U> mapper) {
            Objects.requireNonNull(mapper);
            if (!isPresent()) {
                return empty();
            } else {
                return Optional.ofNullable(mapper.apply(value));
            }
        }

        /**
         * If a value is present, apply the provided {@code Optional}-bearing
         * mapping function to it, return that result, otherwise return an empty
         * {@code Optional}.  This method is similar to {@link #map(Distributed.Function)},
         * but the provided mapper is one whose result is already an {@code Optional},
         * and if invoked, {@code flatMap} does not wrap it with an additional
         * {@code Optional}.
         *
         * @param <U>    The type parameter to the {@code Optional} returned by
         * @param mapper a mapping function to apply to the value, if present
         *               the mapping function
         * @return the result of applying an {@code Optional}-bearing mapping
         * function to the value of this {@code Optional}, if a value is present,
         * otherwise an empty {@code Optional}
         * @throws NullPointerException if the mapping function is null or returns
         *                              a null result
         */
        public <U> Optional<U> flatMap(Distributed.Function<? super T, Optional<U>> mapper) {
            Objects.requireNonNull(mapper);
            if (!isPresent()) {
                return empty();
            } else {
                return Objects.requireNonNull(mapper.apply(value));
            }
        }

        /**
         * Return the value if present, otherwise invoke {@code other} and return
         * the result of that invocation.
         *
         * @param other a {@code Supplier} whose result is returned if no value
         *              is present
         * @return the value if present otherwise the result of {@code other.get()}
         * @throws NullPointerException if value is not present and {@code other} is
         *                              null
         */
        public T orElseGet(Distributed.Supplier<? extends T> other) {
            return value != null ? value : other.get();
        }

        /**
         * Return the value if present, otherwise return {@code other}.
         *
         * @param other the value to be returned if there is no value present, may
         *              be null
         * @return the value, if present, otherwise {@code other}
         */
        public T orElse(T other) {
            return value != null ? value : other;
        }

        /**
         * Return the contained value, if present, otherwise throw an exception
         * to be created by the provided supplier.
         *
         * @param <X>               Type of the exception to be thrown
         * @param exceptionSupplier The supplier which will return the exception to
         *                          be thrown
         * @return the present value
         * @throws X                    if there is no value present
         * @throws NullPointerException if no value is present and
         *                              {@code exceptionSupplier} is null
         * @apiNote A method reference to the exception constructor with an empty
         * argument list can be used as the supplier. For example,
         * {@code IllegalStateException::new}
         */
        public <X extends Throwable> T orElseThrow(Distributed.Supplier<? extends X> exceptionSupplier) throws X {
            if (value != null) {
                return value;
            } else {
                throw exceptionSupplier.get();
            }
        }

        /**
         * Indicates whether some other object is "equal to" this Optional. The
         * other object is considered equal if:
         * <ul>
         * <li>it is also an {@code Optional} and;
         * <li>both instances have no value present or;
         * <li>the present values are "equal to" each other via {@code equals()}.
         * </ul>
         *
         * @param obj an object to be tested for equality
         * @return {code true} if the other object is "equal to" this object
         * otherwise {@code false}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof Optional)) {
                return false;
            }

            Optional<?> other = (Optional<?>) obj;
            return Objects.equals(value, other.value);
        }

        /**
         * Returns the hash code value of the present value, if any, or 0 (zero) if
         * no value is present.
         *
         * @return hash code value of the present value or 0 if no value is present
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }

        /**
         * Returns a non-empty string representation of this Optional suitable for
         * debugging. The exact presentation format is unspecified and may vary
         * between implementations and versions.
         *
         * @return the string representation of this instance
         * @implSpec If a value is present the result must include its string
         * representation in the result. Empty and present Optionals must be
         * unambiguously differentiable.
         */
        @Override
        public String toString() {
            return value != null
                    ? String.format("Optional[%s]", value)
                    : "Optional.empty";
        }
    }

}
