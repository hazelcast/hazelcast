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

package com.hazelcast.jet.function;

import com.hazelcast.jet.function.DistributedComparators.NullComparator;
import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Comparator java.util.Comparator}
 * which declares checked exception.
 */
@FunctionalInterface
public interface DistributedComparator<T> extends Comparator<T>, Serializable {

    /**
     * Exception-declaring version of {@link Comparator#compare}.
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
     */
    @SuppressWarnings("unchecked")
    static <T extends Comparable<? super T>> DistributedComparator<T> naturalOrder() {
        return (DistributedComparator<T>) DistributedComparators.NATURAL_ORDER;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#reverseOrder()
     * java.util.Comparator#reverseOrder()}.
     */
    @SuppressWarnings("unchecked")
    static <T extends Comparable<? super T>> DistributedComparator<T> reverseOrder() {
        return (DistributedComparator<T>) DistributedComparators.REVERSE_ORDER;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsFirst(Comparator)
     * java.util.Comparator#nullsFirst(Comparator)}.
     */
    static <T> DistributedComparator<T> nullsFirst(Comparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        NullComparator<T> c = new NullComparator<>(true);
        return comparator != null ? c.thenComparing(comparator) : c;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsFirst(Comparator)
     * java.util.Comparator#nullsFirst(Comparator)}.
     */
    static <T> DistributedComparator<T> nullsFirst(DistributedComparator<? super T> comparator) {
        return nullsFirst((java.util.Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsLast(Comparator)
     * java.util.Comparator#nullsLast(Comparator)}.
     */
    static <T> DistributedComparator<T> nullsLast(Comparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        NullComparator<T> c = new NullComparator<>(false);
        return comparator != null ? c.thenComparing(comparator) : c;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#nullsLast(Comparator)
     * java.util.Comparator#nullsLast(Comparator)}.
     */
    static <T> DistributedComparator<T> nullsLast(DistributedComparator<? super T> comparator) {
        return nullsLast((java.util.Comparator<? super T>) comparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function, Comparator)
     * java.util.Comparator#comparing(Function, Comparator)}.
     */
    static <T, U> DistributedComparator<T> comparing(
            java.util.function.Function<? super T, ? extends U> toKeyFn,
            java.util.Comparator<? super U> keyComparator
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
     */
    static <T, U> DistributedComparator<T> comparing(
            DistributedFunction<? super T, ? extends U> toKeyFn,
            DistributedComparator<? super U> keyComparator) {
        return comparing((java.util.function.Function<? super T, ? extends U>) toKeyFn, keyComparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function)
     * java.util.Comparator#comparing(Function)}.
     */
    static <T, U extends Comparable<? super U>> DistributedComparator<T> comparing(
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
     */
    static <T, U extends Comparable<? super U>> DistributedComparator<T> comparing(
            DistributedFunction<? super T, ? extends U> toKeyFn
    ) {
        return comparing((java.util.function.Function<? super T, ? extends U>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingInt(ToIntFunction)
     * java.util.Comparator#comparingInt(ToIntFunction)}.
     */
    static <T> DistributedComparator<T> comparingInt(ToIntFunction<? super T> toKeyFn) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (c1, c2) -> Integer.compare(toKeyFn.applyAsInt(c1), toKeyFn.applyAsInt(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingInt(ToIntFunction)
     * java.util.Comparator#comparingInt(ToIntFunction)}.
     */
    static <T> DistributedComparator<T> comparingInt(DistributedToIntFunction<? super T> toKeyFn) {
        return comparingInt((java.util.function.ToIntFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingLong(ToLongFunction)
     * java.util.Comparator#comparingLong(ToLongFunction)}.
     */
    static <T> DistributedComparator<T> comparingLong(ToLongFunction<? super T> toKeyFn) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (c1, c2) -> Long.compare(toKeyFn.applyAsLong(c1), toKeyFn.applyAsLong(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingLong(ToLongFunction)
     * java.util.Comparator#comparingLong(ToLongFunction)}.
     */
    static <T> DistributedComparator<T> comparingLong(DistributedToLongFunction<? super T> toKeyFn) {
        return comparingLong((java.util.function.ToLongFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingDouble(ToDoubleFunction)
     * java.util.Comparator#comparingDouble(ToDoubleFunction)}.
     */
    static <T> DistributedComparator<T> comparingDouble(ToDoubleFunction<? super T> toKeyFn) {
        checkNotNull(toKeyFn, "toKeyFn");
        checkSerializable(toKeyFn, "toKeyFn");
        return (c1, c2) -> Double.compare(toKeyFn.applyAsDouble(c1), toKeyFn.applyAsDouble(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingDouble(ToDoubleFunction)
     * java.util.Comparator#comparingDouble(ToDoubleFunction)}.
     */
    static <T> DistributedComparator<T> comparingDouble(DistributedToDoubleFunction<? super T> toKeyFn) {
        return comparingDouble((java.util.function.ToDoubleFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Comparator)
     * java.util.Comparator#thenComparing(Comparator)}.
     */
    @Override
    default DistributedComparator<T> thenComparing(Comparator<? super T> other) {
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
    default DistributedComparator<T> thenComparing(DistributedComparator<? super T> other) {
        return thenComparing((Comparator<? super T>) other);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function, Comparator)
     * java.util.Comparator#thenComparing(Function, Comparator)}.
     */
    @Override
    default <U> DistributedComparator<T> thenComparing(
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
     */
    default <U> DistributedComparator<T> thenComparing(
            DistributedFunction<? super T, ? extends U> toKeyFn,
            DistributedComparator<? super U> keyComparator) {
        return thenComparing((java.util.function.Function<? super T, ? extends U>) toKeyFn, keyComparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function)
     * java.util.Comparator#thenComparing(Function)}.
     */
    @Override
    default <U extends Comparable<? super U>> DistributedComparator<T> thenComparing(
            Function<? super T, ? extends U> toKeyFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparing(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function)
     * java.util.Comparator#thenComparing(Function)}.
     */
    default <U extends Comparable<? super U>> DistributedComparator<T> thenComparing(
            DistributedFunction<? super T, ? extends U> toKeyFn) {
        return thenComparing((java.util.function.Function<? super T, ? extends U>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingInt(ToIntFunction)
     * java.util.Comparator#thenComparingInt(ToIntFunction)}.
     */
    @Override
    default DistributedComparator<T> thenComparingInt(ToIntFunction<? super T> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparingInt(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingInt(ToIntFunction)
     * java.util.Comparator#thenComparingInt(ToIntFunction)}.
     */
    default DistributedComparator<T> thenComparingInt(DistributedToIntFunction<? super T> toKeyFn) {
        return thenComparingInt((java.util.function.ToIntFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingLong(ToLongFunction)
     * java.util.Comparator#thenComparingLong(ToLongFunction)}.
     */
    @Override
    default DistributedComparator<T> thenComparingLong(ToLongFunction<? super T> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparingLong(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingLong(ToLongFunction)
     * java.util.Comparator#thenComparingLong(ToLongFunction)}.
     */
    default DistributedComparator<T> thenComparingLong(DistributedToLongFunction<? super T> toKeyFn) {
        return thenComparingLong((ToLongFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingDouble(ToDoubleFunction)
     * java.util.Comparator#thenComparingDouble(ToDoubleFunction)}.
     */
    @Override
    default DistributedComparator<T> thenComparingDouble(ToDoubleFunction<? super T> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        return thenComparing(comparingDouble(toKeyFn));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingDouble(ToDoubleFunction)
     * java.util.Comparator#thenComparingDouble(ToDoubleFunction)}.
     */
    default DistributedComparator<T> thenComparingDouble(DistributedToDoubleFunction<? super T> toKeyFn) {
        return thenComparingDouble((java.util.function.ToDoubleFunction<? super T>) toKeyFn);
    }

    /**
     * {@code Serializable} variant of {@link Comparator#reversed()
     * java.util.Comparator#reversed()}
     */
    @Override
    default DistributedComparator<T> reversed() {
        return (o1, o2) -> compare(o2, o1);
    }
}
