/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.function;

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.function.ComparatorsEx.NullComparator;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Comparator java.util.Comparator}
 * which declares checked exception.
 *
 * @param <T> the type of objects that may be compared by this comparator
 *
 * @since 4.0
 */
@FunctionalInterface
@SuppressWarnings("checkstyle:methodcount")
public interface ComparatorEx<T> extends Comparator<T>, Serializable {

    /**
     * Exception-declaring version of {@link Comparator#compare}.
     * @throws Exception in case of any exceptional case
     */
    int compareEx(T o1, T o2) throws Exception;

    @Override
    default int compare(T o1, T o2) {
        try {
            return compareEx(o1, o2);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#naturalOrder()
     * java.util.Comparator#naturalOrder()}.
     * @param  <T> the {@link Comparable} type of element to be compared
     */
    @SuppressWarnings("unchecked")
    static <T extends Comparable<? super T>> ComparatorEx<T> naturalOrder() {
        return (ComparatorEx<T>) ComparatorsEx.NATURAL_ORDER;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#reverseOrder()
     * java.util.Comparator#reverseOrder()}.
     * @param  <T> the {@link Comparable} type of element to be compared
     */
    @SuppressWarnings("unchecked")
    static <T extends Comparable<? super T>> ComparatorEx<T> reverseOrder() {
        return (ComparatorEx<T>) ComparatorsEx.REVERSE_ORDER;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsFirst(Comparator)
     * java.util.Comparator#nullsFirst(Comparator)}.
     * @param  <T> the type of the elements to be compared
     */
    static <T> ComparatorEx<T> nullsFirst(Comparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        NullComparator<T> c = new NullComparator<>(true);
        return comparator != null ? c.thenComparing(comparator) : c;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsFirst(Comparator)
     * java.util.Comparator#nullsFirst(Comparator)}.
     * @param  <T> the type of the elements to be compared
     */
    static <T> ComparatorEx<T> nullsFirst(ComparatorEx<? super T> comparator) {
        return nullsFirst((Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsLast(Comparator)
     * java.util.Comparator#nullsLast(Comparator)}.
     * @param  <T> the type of the elements to be compared
     */
    static <T> ComparatorEx<T> nullsLast(Comparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        NullComparator<T> c = new NullComparator<>(false);
        return comparator != null ? c.thenComparing(comparator) : c;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsLast(Comparator)
     * java.util.Comparator#nullsLast(Comparator)}.
     * @param  <T> the type of the elements to be compared
     */
    static <T> ComparatorEx<T> nullsLast(ComparatorEx<? super T> comparator) {
        return nullsLast((Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function, Comparator)
     * java.util.Comparator#comparing(Function, Comparator)}.
     * @param  <T> the type of element to be compared
     * @param  <U> the type of the sort key
     */
    static <T, U> ComparatorEx<T> comparing(
            Function<? super T, ? extends U> toKeyFn,
            Comparator<? super U> keyComparator
    ) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkNotNull(keyComparator, "keyComparator");
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(keyComparator, "keyComparator");
        return (c1, c2) -> keyComparator.compare(toKeyFn.apply(c1),
                toKeyFn.apply(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function, Comparator)
     * java.util.Comparator#comparing(Function, Comparator)}.
     * @param  <T> the type of element to be compared
     * @param  <U> the type of the sort key
     */
    static <T, U> ComparatorEx<T> comparing(
            FunctionEx<? super T, ? extends U> toKeyFn,
            ComparatorEx<? super U> keyComparator) {
        return comparing((Function<? super T, ? extends U>) toKeyFn, keyComparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function)
     * java.util.Comparator#comparing(Function)}.
     * @param  <T> the type of element to be compared
     * @param  <U> the type of the {@code Comparable} sort key
     */
    static <T, U extends Comparable<? super U>> ComparatorEx<T> comparing(
            Function<? super T, ? extends U> toKeyFn
    ) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (left, right) -> toKeyFn.apply(left).compareTo(toKeyFn.apply(right));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function)
     * java.util.Comparator#comparing(Function)}.
     * @param  <T> the type of element to be compared
     * @param  <U> the type of the {@code Comparable} sort key
     */
    static <T, U extends Comparable<? super U>> ComparatorEx<T> comparing(
            FunctionEx<? super T, ? extends U> toKeyFn
    ) {
        return comparing((Function<? super T, ? extends U>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingInt(ToIntFunction)
     * java.util.Comparator#comparingInt(ToIntFunction)}.
     * @param  <T> the type of element to be compared
     */
    static <T> ComparatorEx<T> comparingInt(ToIntFunction<? super T> toKeyFn) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (c1, c2) -> Integer.compare(toKeyFn.applyAsInt(c1), toKeyFn.applyAsInt(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingInt(ToIntFunction)
     * java.util.Comparator#comparingInt(ToIntFunction)}.
     * @param  <T> the type of element to be compared
     */
    static <T> ComparatorEx<T> comparingInt(ToIntFunctionEx<? super T> toKeyFn) {
        return comparingInt((ToIntFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingLong(ToLongFunction)
     * java.util.Comparator#comparingLong(ToLongFunction)}.
     * @param  <T> the type of element to be compared
     */
    static <T> ComparatorEx<T> comparingLong(ToLongFunction<? super T> toKeyFn) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (c1, c2) -> Long.compare(toKeyFn.applyAsLong(c1), toKeyFn.applyAsLong(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingLong(ToLongFunction)
     * java.util.Comparator#comparingLong(ToLongFunction)}.
     * @param  <T> the type of element to be compared
     */
    static <T> ComparatorEx<T> comparingLong(ToLongFunctionEx<? super T> toKeyFn) {
        return comparingLong((ToLongFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingDouble(ToDoubleFunction)
     * java.util.Comparator#comparingDouble(ToDoubleFunction)}.
     * @param  <T> the type of element to be compared
     */
    static <T> ComparatorEx<T> comparingDouble(ToDoubleFunction<? super T> toKeyFn) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (c1, c2) -> Double.compare(toKeyFn.applyAsDouble(c1), toKeyFn.applyAsDouble(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingDouble(ToDoubleFunction)
     * java.util.Comparator#comparingDouble(ToDoubleFunction)}.
     * @param  <T> the type of element to be compared
     */
    static <T> ComparatorEx<T> comparingDouble(ToDoubleFunctionEx<? super T> toKeyFn) {
        return comparingDouble((ToDoubleFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Comparator)
     * java.util.Comparator#thenComparing(Comparator)}.
     */
    @Override
    default ComparatorEx<T> thenComparing(Comparator<? super T> other) {
        checkNotNull(other, "other");
        checkSerializable(other, "other");
        return (c1, c2) -> {
            int res = compare(c1, c2);
            return (res != 0) ? res : other.compare(c1, c2);
        };
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Comparator)
     * java.util.Comparator#thenComparing(Comparator)}.
     */
    default ComparatorEx<T> thenComparing(ComparatorEx<? super T> other) {
        return thenComparing((Comparator<? super T>) other);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function, Comparator)
     * java.util.Comparator#thenComparing(Function, Comparator)}.
     * @param  <U>  the type of the sort key
     */
    @Override
    default <U> ComparatorEx<T> thenComparing(
            Function<? super T, ? extends U> toKeyFn, Comparator<? super U> keyComparator
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(keyComparator, "keyComparator");
        return thenComparing(comparing(toKeyFn, keyComparator));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function, Comparator)
     * java.util.Comparator#thenComparing(Function, Comparator)}.
     * @param  <U>  the type of the sort key
     */
    default <U> ComparatorEx<T> thenComparing(
            FunctionEx<? super T, ? extends U> toKeyFn,
            ComparatorEx<? super U> keyComparator) {
        return thenComparing((Function<? super T, ? extends U>) toKeyFn, keyComparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function)
     * java.util.Comparator#thenComparing(Function)}.
     * @param  <U>  the type of the {@link Comparable} sort key
     */
    @Override
    default <U extends Comparable<? super U>> ComparatorEx<T> thenComparing(
            Function<? super T, ? extends U> toKeyFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparing(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function)
     * java.util.Comparator#thenComparing(Function)}.
     * @param  <U>  the type of the {@link Comparable} sort key
     */
    default <U extends Comparable<? super U>> ComparatorEx<T> thenComparing(
            FunctionEx<? super T, ? extends U> toKeyFn) {
        return thenComparing((Function<? super T, ? extends U>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingInt(ToIntFunction)
     * java.util.Comparator#thenComparingInt(ToIntFunction)}.
     */
    @Override
    default ComparatorEx<T> thenComparingInt(ToIntFunction<? super T> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparingInt(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingInt(ToIntFunction)
     * java.util.Comparator#thenComparingInt(ToIntFunction)}.
     */
    default ComparatorEx<T> thenComparingInt(ToIntFunctionEx<? super T> toKeyFn) {
        return thenComparingInt((ToIntFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingLong(ToLongFunction)
     * java.util.Comparator#thenComparingLong(ToLongFunction)}.
     */
    @Override
    default ComparatorEx<T> thenComparingLong(ToLongFunction<? super T> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparingLong(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingLong(ToLongFunction)
     * java.util.Comparator#thenComparingLong(ToLongFunction)}.
     */
    default ComparatorEx<T> thenComparingLong(ToLongFunctionEx<? super T> toKeyFn) {
        return thenComparingLong((ToLongFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingDouble(ToDoubleFunction)
     * java.util.Comparator#thenComparingDouble(ToDoubleFunction)}.
     */
    @Override
    default ComparatorEx<T> thenComparingDouble(ToDoubleFunction<? super T> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparingDouble(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingDouble(ToDoubleFunction)
     * java.util.Comparator#thenComparingDouble(ToDoubleFunction)}.
     */
    default ComparatorEx<T> thenComparingDouble(ToDoubleFunctionEx<? super T> toKeyFn) {
        return thenComparingDouble((ToDoubleFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link Comparator#reversed()
     * java.util.Comparator#reversed()}
     */
    @Override
    default ComparatorEx<T> reversed() {
        return (o1, o2) -> compare(o2, o1);
    }
}
