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

package com.hazelcast.jet.function;

import com.hazelcast.jet.stream.impl.distributed.DistributedComparators;
import com.hazelcast.jet.stream.impl.distributed.DistributedComparators.NullComparator;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * {@code Serializable} variant of {@link Comparator
 * java.util.Comparator}.
 */
@FunctionalInterface
public interface DistributedComparator<T> extends Comparator<T>, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * Comparator#naturalOrder()
     * java.util.Comparator#naturalOrder()}.
     */
    @SuppressWarnings("unchecked")
    static <T extends Comparable<? super T>> DistributedComparator<T> naturalOrder() {
        return (DistributedComparator<T>) DistributedComparators.NATURAL_ORDER_COMPARATOR;
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#reverseOrder()
     * java.util.Comparator#reverseOrder()}.
     */
    @SuppressWarnings("unchecked")
    static <T extends Comparable<? super T>> DistributedComparator<T> reverseOrder() {
        return (DistributedComparator<T>) DistributedComparators.REVERSE_ORDER_COMPARATOR;
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
            java.util.function.Function<? super T, ? extends U> keyExtractor,
            java.util.Comparator<? super U> keyComparator
    ) {
        Objects.requireNonNull(keyExtractor);
        Objects.requireNonNull(keyComparator);
        checkSerializable(keyExtractor, "keyExtractor");
        checkSerializable(keyComparator, "keyComparator");
        return (c1, c2) -> keyComparator.compare(keyExtractor.apply(c1),
                keyExtractor.apply(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function, Comparator)
     * java.util.Comparator#comparing(Function, Comparator)}.
     */
    static <T, U> DistributedComparator<T> comparing(
            DistributedFunction<? super T, ? extends U> keyExtractor,
            DistributedComparator<? super U> keyComparator) {
        return comparing((java.util.function.Function<? super T, ? extends U>) keyExtractor, keyComparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function)
     * java.util.Comparator#comparing(Function)}.
     */
    static <T, U extends Comparable<? super U>> DistributedComparator<T> comparing(
            Function<? super T, ? extends U> keyExtractor
    ) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> keyExtractor.apply(c1).compareTo(keyExtractor.apply(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparing(Function)
     * java.util.Comparator#comparing(Function)}.
     */
    static <T, U extends Comparable<? super U>> DistributedComparator<T> comparing(
            DistributedFunction<? super T, ? extends U> keyExtractor
    ) {
        return comparing((java.util.function.Function<? super T, ? extends U>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingInt(ToIntFunction)
     * java.util.Comparator#comparingInt(ToIntFunction)}.
     */
    static <T> DistributedComparator<T> comparingInt(ToIntFunction<? super T> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> Integer.compare(keyExtractor.applyAsInt(c1), keyExtractor.applyAsInt(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingInt(ToIntFunction)
     * java.util.Comparator#comparingInt(ToIntFunction)}.
     */
    static <T> DistributedComparator<T> comparingInt(DistributedToIntFunction<? super T> keyExtractor) {
        return comparingInt((java.util.function.ToIntFunction<? super T>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingLong(ToLongFunction)
     * java.util.Comparator#comparingLong(ToLongFunction)}.
     */
    static <T> DistributedComparator<T> comparingLong(ToLongFunction<? super T> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c1), keyExtractor.applyAsLong(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingLong(ToLongFunction)
     * java.util.Comparator#comparingLong(ToLongFunction)}.
     */
    static <T> DistributedComparator<T> comparingLong(DistributedToLongFunction<? super T> keyExtractor) {
        return comparingLong((java.util.function.ToLongFunction<? super T>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingDouble(ToDoubleFunction)
     * java.util.Comparator#comparingDouble(ToDoubleFunction)}.
     */
    static <T> DistributedComparator<T> comparingDouble(ToDoubleFunction<? super T> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> Double.compare(keyExtractor.applyAsDouble(c1), keyExtractor.applyAsDouble(c2));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#comparingDouble(ToDoubleFunction)
     * java.util.Comparator#comparingDouble(ToDoubleFunction)}.
     */
    static <T> DistributedComparator<T> comparingDouble(DistributedToDoubleFunction<? super T> keyExtractor) {
        return comparingDouble((java.util.function.ToDoubleFunction<? super T>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Comparator)
     * java.util.Comparator#thenComparing(Comparator)}.
     */
    @Override
    default DistributedComparator<T> thenComparing(Comparator<? super T> other) {
        Objects.requireNonNull(other);
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
            Function<? super T, ? extends U> keyExtractor, Comparator<? super U> keyComparator
    ) {
        checkSerializable(keyExtractor, "keyExtractor");
        checkSerializable(keyComparator, "keyComparator");
        return thenComparing(comparing(keyExtractor, keyComparator));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function, Comparator)
     * java.util.Comparator#thenComparing(Function, Comparator)}.
     */
    default <U> DistributedComparator<T> thenComparing(
            DistributedFunction<? super T, ? extends U> keyExtractor,
            DistributedComparator<? super U> keyComparator) {
        return thenComparing((java.util.function.Function<? super T, ? extends U>) keyExtractor, keyComparator);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function)
     * java.util.Comparator#thenComparing(Function)}.
     */
    @Override
    default <U extends Comparable<? super U>> DistributedComparator<T> thenComparing(
            Function<? super T, ? extends U> keyExtractor
    ) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparing(keyExtractor));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparing(Function)
     * java.util.Comparator#thenComparing(Function)}.
     */
    default <U extends Comparable<? super U>> DistributedComparator<T> thenComparing(
            DistributedFunction<? super T, ? extends U> keyExtractor) {
        return thenComparing((java.util.function.Function<? super T, ? extends U>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingInt(ToIntFunction)
     * java.util.Comparator#thenComparingInt(ToIntFunction)}.
     */
    @Override
    default DistributedComparator<T> thenComparingInt(ToIntFunction<? super T> keyExtractor) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparingInt(keyExtractor));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingInt(ToIntFunction)
     * java.util.Comparator#thenComparingInt(ToIntFunction)}.
     */
    default DistributedComparator<T> thenComparingInt(DistributedToIntFunction<? super T> keyExtractor) {
        return thenComparingInt((java.util.function.ToIntFunction<? super T>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingLong(ToLongFunction)
     * java.util.Comparator#thenComparingLong(ToLongFunction)}.
     */
    @Override
    default DistributedComparator<T> thenComparingLong(ToLongFunction<? super T> keyExtractor) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparingLong(keyExtractor));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingLong(ToLongFunction)
     * java.util.Comparator#thenComparingLong(ToLongFunction)}.
     */
    default DistributedComparator<T> thenComparingLong(DistributedToLongFunction<? super T> keyExtractor) {
        return thenComparingLong((ToLongFunction<? super T>) keyExtractor);
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingDouble(ToDoubleFunction)
     * java.util.Comparator#thenComparingDouble(ToDoubleFunction)}.
     */
    @Override
    default DistributedComparator<T> thenComparingDouble(ToDoubleFunction<? super T> keyExtractor) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparingDouble(keyExtractor));
    }

    /**
     * {@code Serializable} variant of {@link
     * Comparator#thenComparingDouble(ToDoubleFunction)
     * java.util.Comparator#thenComparingDouble(ToDoubleFunction)}.
     */
    default DistributedComparator<T> thenComparingDouble(DistributedToDoubleFunction<? super T> keyExtractor) {
        return thenComparingDouble((java.util.function.ToDoubleFunction<? super T>) keyExtractor);
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
