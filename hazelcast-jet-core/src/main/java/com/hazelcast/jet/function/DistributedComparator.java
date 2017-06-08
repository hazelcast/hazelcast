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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

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
public interface DistributedComparator<T> extends Comparator<T>, Serializable {

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
    static <T extends Comparable<? super T>> DistributedComparator<T> naturalOrder() {
        return (DistributedComparator<T>) DistributedComparators.NATURAL_ORDER_COMPARATOR;
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
    static <T extends Comparable<? super T>> DistributedComparator<T> reverseOrder() {
        return (DistributedComparator<T>) DistributedComparators.REVERSE_ORDER_COMPARATOR;
    }

    /**
     * @see java.util.Comparator#nullsFirst(java.util.Comparator)
     */
    static <T> DistributedComparator<T> nullsFirst(java.util.Comparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        NullComparator<T> c = new NullComparator<>(true);
        return comparator != null ? c.thenComparing(comparator) : c;
    }

    /**
     * @see java.util.Comparator#nullsFirst(java.util.Comparator)
     */
    static <T> DistributedComparator<T> nullsFirst(DistributedComparator<? super T> comparator) {
        return nullsFirst((java.util.Comparator<? super T>) comparator);
    }

    /**
     * @see java.util.Comparator#nullsLast(java.util.Comparator)
     */
    static <T> DistributedComparator<T> nullsLast(java.util.Comparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        NullComparator<T> c = new NullComparator<>(false);
        return comparator != null ? c.thenComparing(comparator) : c;
    }

    /**
     * @see java.util.Comparator#nullsLast(java.util.Comparator)
     */
    static <T> DistributedComparator<T> nullsLast(DistributedComparator<? super T> comparator) {
        return nullsLast((java.util.Comparator<? super T>) comparator);
    }

    /**
     * @see java.util.Comparator#comparing(java.util.function.Function, java.util.Comparator)
     */
    static <T, U> DistributedComparator<T> comparing(
            java.util.function.Function<? super T, ? extends U> keyExtractor,
            java.util.Comparator<? super U> keyComparator) {
        Objects.requireNonNull(keyExtractor);
        Objects.requireNonNull(keyComparator);
        checkSerializable(keyExtractor, "keyExtractor");
        checkSerializable(keyComparator, "keyComparator");
        return (c1, c2) -> keyComparator.compare(keyExtractor.apply(c1),
                        keyExtractor.apply(c2));
    }

    /**
     * @see java.util.Comparator#comparing(java.util.function.Function, java.util.Comparator)
     */
    static <T, U> DistributedComparator<T> comparing(
            DistributedFunction<? super T, ? extends U> keyExtractor,
            DistributedComparator<? super U> keyComparator) {
        return comparing((java.util.function.Function<? super T, ? extends U>) keyExtractor, keyComparator);
    }

    /**
     * @see java.util.Comparator#comparing(java.util.function.Function)
     */
    static <T, U extends Comparable<? super U>> DistributedComparator<T> comparing(
            java.util.function.Function<? super T, ? extends U> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> keyExtractor.apply(c1).compareTo(keyExtractor.apply(c2));
    }

    /**
     * @see java.util.Comparator#comparing(java.util.function.Function)
     */
    static <T, U extends Comparable<? super U>> DistributedComparator<T> comparing(
            DistributedFunction<? super T, ? extends U> keyExtractor) {
        return comparing((java.util.function.Function<? super T, ? extends U>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#comparingInt(java.util.function.ToIntFunction)
     */
    static <T> DistributedComparator<T> comparingInt(java.util.function.ToIntFunction<? super T> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> Integer.compare(keyExtractor.applyAsInt(c1), keyExtractor.applyAsInt(c2));
    }

    /**
     * @see java.util.Comparator#comparingInt(java.util.function.ToIntFunction)
     */
    static <T> DistributedComparator<T> comparingInt(DistributedToIntFunction<? super T> keyExtractor) {
        return comparingInt((java.util.function.ToIntFunction<? super T>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#comparingInt(java.util.function.ToIntFunction)
     */
    static <T> DistributedComparator<T> comparingLong(java.util.function.ToLongFunction<? super T> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c1), keyExtractor.applyAsLong(c2));
    }

    /**
     * @see java.util.Comparator#comparingLong(java.util.function.ToLongFunction)
     */
    static <T> DistributedComparator<T> comparingLong(DistributedToLongFunction<? super T> keyExtractor) {
        return comparingLong((java.util.function.ToLongFunction<? super T>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#comparingDouble(java.util.function.ToDoubleFunction)
     */
    static <T> DistributedComparator<T> comparingDouble(java.util.function.ToDoubleFunction<? super T> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        checkSerializable(keyExtractor, "keyExtractor");
        return (c1, c2) -> Double.compare(keyExtractor.applyAsDouble(c1), keyExtractor.applyAsDouble(c2));
    }

    /**
     * @see java.util.Comparator#comparingDouble(java.util.function.ToDoubleFunction)
     */
    static <T> DistributedComparator<T> comparingDouble(DistributedToDoubleFunction<? super T> keyExtractor) {
        return comparingDouble((java.util.function.ToDoubleFunction<? super T>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#thenComparing(java.util.Comparator)
     */
    @Override
    default DistributedComparator<T> thenComparing(java.util.Comparator<? super T> other) {
        Objects.requireNonNull(other);
        checkSerializable(other, "other");
        return (c1, c2) -> {
            int res = compare(c1, c2);
            return (res != 0) ? res : other.compare(c1, c2);
        };
    }

    /**
     * @see java.util.Comparator#thenComparing(java.util.Comparator)
     */
    default DistributedComparator<T> thenComparing(DistributedComparator<? super T> other) {
        return thenComparing((java.util.Comparator<? super T>) other);
    }

    /**
     * @see java.util.Comparator#thenComparing(java.util.function.Function, java.util.Comparator)
     */
    @Override
    default <U> DistributedComparator<T> thenComparing(
            java.util.function.Function<? super T, ? extends U> keyExtractor,
            java.util.Comparator<? super U> keyComparator) {
        checkSerializable(keyExtractor, "keyExtractor");
        checkSerializable(keyComparator, "keyComparator");
        return thenComparing(comparing(keyExtractor, keyComparator));
    }

    /**
     * @see java.util.Comparator#thenComparing(java.util.function.Function, java.util.Comparator)
     */
    default <U> DistributedComparator<T> thenComparing(
            DistributedFunction<? super T, ? extends U> keyExtractor,
            DistributedComparator<? super U> keyComparator) {
        return thenComparing((java.util.function.Function<? super T, ? extends U>) keyExtractor, keyComparator);
    }

    /**
     * @see java.util.Comparator#thenComparing(java.util.function.Function)
     */
    @Override
    default <U extends Comparable<? super U>> DistributedComparator<T> thenComparing(
            java.util.function.Function<? super T, ? extends U> keyExtractor) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparing(keyExtractor));
    }

    /**
     * @see java.util.Comparator#thenComparing(java.util.function.Function)
     */
    default <U extends Comparable<? super U>> DistributedComparator<T> thenComparing(
            DistributedFunction<? super T, ? extends U> keyExtractor) {
        return thenComparing((java.util.function.Function<? super T, ? extends U>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#thenComparingInt(java.util.function.ToIntFunction)
     */
    @Override
    default DistributedComparator<T> thenComparingInt(java.util.function.ToIntFunction<? super T> keyExtractor) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparingInt(keyExtractor));
    }

    /**
     * @see java.util.Comparator#thenComparingInt(java.util.function.ToIntFunction)
     */
    default DistributedComparator<T> thenComparingInt(DistributedToIntFunction<? super T> keyExtractor) {
        return thenComparingInt((java.util.function.ToIntFunction<? super T>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#thenComparingLong(java.util.function.ToLongFunction)
     */
    @Override
    default DistributedComparator<T> thenComparingLong(java.util.function.ToLongFunction<? super T> keyExtractor) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparingLong(keyExtractor));
    }

    /**
     * @see java.util.Comparator#thenComparingLong(java.util.function.ToLongFunction)
     */
    default DistributedComparator<T> thenComparingLong(DistributedToLongFunction<? super T> keyExtractor) {
        return thenComparingLong((java.util.function.ToLongFunction<? super T>) keyExtractor);
    }

    /**
     * @see java.util.Comparator#thenComparingDouble(java.util.function.ToDoubleFunction)
     */
    @Override
    default DistributedComparator<T> thenComparingDouble(
            java.util.function.ToDoubleFunction<? super T> keyExtractor
    ) {
        checkSerializable(keyExtractor, "keyExtractor");
        return thenComparing(comparingDouble(keyExtractor));
    }

    /**
     * @see java.util.Comparator#thenComparingDouble(java.util.function.ToDoubleFunction)
     */
    default DistributedComparator<T> thenComparingDouble(DistributedToDoubleFunction<? super T> keyExtractor) {
        return thenComparingDouble((java.util.function.ToDoubleFunction<? super T>) keyExtractor);
    }

    @Override
    default DistributedComparator<T> reversed() {
        return (o1, o2) -> compare(o2, o1);
    }
}
