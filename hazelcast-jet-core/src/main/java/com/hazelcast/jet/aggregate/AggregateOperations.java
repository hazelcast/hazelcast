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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperationBuilder.VarArity;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.DistributedTriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Utility class with factory methods for several useful aggregate
 * operations.
 */
public final class AggregateOperations {

    private AggregateOperations() {
    }

    /**
     * Returns an aggregate operation that computes the number of items.
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongAccumulator, Long> counting() {
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator a, T item) -> a.add(1))
                .andCombine(LongAccumulator::add)
                .andDeduct(LongAccumulator::subtractAllowingOverflow)
                .andFinish(LongAccumulator::get);
    }

    /**
     * Returns an aggregate operation that computes the sum of the {@code long}
     * values it obtains by applying {@code getLongValueFn} to each item.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongAccumulator, Long> summingLong(
            @Nonnull DistributedToLongFunction<? super T> getLongValueFn
    ) {
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator a, T item) -> a.add(getLongValueFn.applyAsLong(item)))
                .andCombine(LongAccumulator::add)
                .andDeduct(LongAccumulator::subtract)
                .andFinish(LongAccumulator::get);
    }

    /**
     * Returns an aggregate operation that computes the sum of the {@code double}
     * values it obtains by applying {@code getDoubleValueFn} to each item.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, DoubleAccumulator, Double> summingDouble(
            @Nonnull DistributedToDoubleFunction<? super T> getDoubleValueFn
    ) {
        return AggregateOperation
                .withCreate(DoubleAccumulator::new)
                .andAccumulate((DoubleAccumulator a, T item) -> a.accumulate(getDoubleValueFn.applyAsDouble(item)))
                .andCombine(DoubleAccumulator::combine)
                .andDeduct(DoubleAccumulator::deduct)
                .andFinish(DoubleAccumulator::finish);
    }

    /**
     * Returns an aggregate operation that computes the minimal item according
     * to the given {@code comparator}.
     * <p>
     * This aggregate operation does not implement the {@link
     * AggregateOperation1#deductFn() deduct} primitive.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, MutableReference<T>, T> minBy(
            @Nonnull DistributedComparator<? super T> comparator
    ) {
        return maxBy(comparator.reversed());
    }

    /**
     * Returns an aggregate operation that computes the maximal item according
     * to the given {@code comparator}.
     * <p>
     * This aggregate operation does not implement the {@link
     * AggregateOperation1#deductFn() deduct} primitive.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, MutableReference<T>, T> maxBy(
            @Nonnull DistributedComparator<? super T> comparator
    ) {
        return AggregateOperation
                .withCreate(MutableReference<T>::new)
                .andAccumulate((MutableReference<T> a, T i) -> {
                    if (!a.isPresent() || comparator.compare(i, a.get()) > 0) {
                        a.set(i);
                    }
                })
                .andCombine((a1, a2) -> {
                    if (!a1.isPresent() || comparator.compare(a1.get(), a2.get()) < 0) {
                        a1.set(a2.get());
                    }
                })
                .andFinish(MutableReference::get);
    }

    /**
     * Returns an aggregate operation that computes the arithmetic mean of the
     * {@code long} values it obtains by applying {@code getLongValueFn} to
     * each item.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongLongAccumulator, Double> averagingLong(
            @Nonnull DistributedToLongFunction<? super T> getLongValueFn
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation
                .withCreate(LongLongAccumulator::new)
                .andAccumulate((LongLongAccumulator a, T i) -> {
                    // a bit faster check than in addExact, specialized for increment
                    if (a.get1() == Long.MAX_VALUE) {
                        throw new ArithmeticException("Counter overflow");
                    }
                    a.set1(a.get1() + 1);
                    a.set2(Math.addExact(a.get2(), getLongValueFn.applyAsLong(i)));
                })
                .andCombine((a1, a2) -> {
                    a1.set1(Math.addExact(a1.get1(), a2.get1()));
                    a1.set2(Math.addExact(a1.get2(), a2.get2()));
                })
                .andDeduct((a1, a2) -> {
                    a1.set1(Math.subtractExact(a1.get1(), a2.get1()));
                    a1.set2(Math.subtractExact(a1.get2(), a2.get2()));
                })
                .andFinish(a -> (double) a.get2() / a.get1());
    }

    /**
     * Returns an aggregate operation that computes the arithmetic mean of the
     * {@code double} values it obtains by applying {@code getDoubleValueFn} to
     * each item.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongDoubleAccumulator, Double> averagingDouble(
            @Nonnull DistributedToDoubleFunction<? super T> getDoubleValueFn
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation
                .withCreate(LongDoubleAccumulator::new)
                .andAccumulate((LongDoubleAccumulator a, T item) -> {
                    // a bit faster check than in addExact, specialized for increment
                    if (a.getLong() == Long.MAX_VALUE) {
                        throw new ArithmeticException("Counter overflow");
                    }
                    a.setLong(a.getLong() + 1);
                    a.setDouble(a.getDouble() + getDoubleValueFn.applyAsDouble(item));
                })
                .andCombine((a1, a2) -> {
                    a1.setLong(Math.addExact(a1.getLong(), a2.getLong()));
                    a1.setDouble(a1.getDouble() + a2.getDouble());
                })
                .andDeduct((a1, a2) -> {
                    a1.setLong(Math.subtractExact(a1.getLong(), a2.getLong()));
                    a1.setDouble(a1.getDouble() - a2.getDouble());
                })
                .andFinish(a -> a.getDouble() / a.getLong());
    }

    /**
     * Returns an aggregate operation that computes a linear trend on the items.
     * The operation will produce a {@code double}-valued coefficient that
     * approximates the rate of change of {@code y} as a function of {@code x},
     * where {@code x} and {@code y} are {@code long} quantities obtained
     * by applying the two provided functions to each item.
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LinTrendAccumulator, Double> linearTrend(
            @Nonnull DistributedToLongFunction<T> getXFn,
            @Nonnull DistributedToLongFunction<T> getYFn
    ) {
        return AggregateOperation
                .withCreate(LinTrendAccumulator::new)
                .andAccumulate((LinTrendAccumulator a, T item) ->
                        a.accumulate(getXFn.applyAsLong(item), getYFn.applyAsLong(item)))
                .andCombine(LinTrendAccumulator::combine)
                .andDeduct(LinTrendAccumulator::deduct)
                .andFinish(LinTrendAccumulator::finish);
    }

    /**
     * Convenience for {@link #allOf(AggregateOperation1, AggregateOperation1,
     * DistributedBiFunction)} with identity finish.
     */
    @Nonnull
    public static <T, A1, A2, R1, R2> AggregateOperation1<T, Tuple2<A1, A2>, Tuple2<R1, R2>> allOf(
            @Nonnull AggregateOperation1<? super T, A1, R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, R2> op2
    ) {
        return allOf(op1, op2, Tuple2::tuple2);
    }

    /**
     * Returns composite aggregate operation from 2 other aggregate operations.
     * It allows you to calculate multiple aggregations over the same items at once.
     *
     * @param op1 1st operation
     * @param op2 2nd operation
     * @param finishFn a function combining 2 results into single target instance
     *
     * @param <T> type of input items
     * @param <A1> 1st accumulator type
     * @param <A2> 2nd accumulator type
     * @param <R1> 1st result type
     * @param <R2> 2nd result type
     * @param <R> final result type
     *
     * @return the composite operation
     */
    @Nonnull
    public static <T, A1, A2, R1, R2, R> AggregateOperation1<T, Tuple2<A1, A2>, R> allOf(
            @Nonnull AggregateOperation1<? super T, A1, R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, R2> op2,
            @Nonnull DistributedBiFunction<? super R1, ? super R2, R> finishFn
    ) {
        return AggregateOperation
                .withCreate(() -> tuple2(op1.createFn().get(), op2.createFn().get()))
                .<T>andAccumulate((acc, item) -> {
                    op1.accumulateFn().accept(acc.f0(), item);
                    op2.accumulateFn().accept(acc.f1(), item);
                })
                .andCombine(op1.combineFn() == null || op2.combineFn() == null ? null :
                        (acc1, acc2) -> {
                            op1.combineFn().accept(acc1.f0(), acc2.f0());
                            op2.combineFn().accept(acc1.f1(), acc2.f1());
                        })
                .andDeduct(op1.deductFn() == null || op2.deductFn() == null ? null :
                        (acc1, acc2) -> {
                            op1.deductFn().accept(acc1.f0(), acc2.f0());
                            op2.deductFn().accept(acc1.f1(), acc2.f1());
                        })
                .andFinish(acc -> finishFn.apply(op1.finishFn().apply(acc.f0()), op2.finishFn().apply(acc.f1())));
    }

    /**
     * Convenience for {@link #allOf(AggregateOperation1, AggregateOperation1,
     * AggregateOperation1, DistributedTriFunction)} with identity finisher.
     */
    @Nonnull
    public static <T, A1, A2, A3, R1, R2, R3> AggregateOperation1<T, Tuple3<A1, A2, A3>, Tuple3<R1, R2, R3>> allOf(
            @Nonnull AggregateOperation1<? super T, A1, R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, R2> op2,
            @Nonnull AggregateOperation1<? super T, A3, R3> op3
    ) {
        return allOf(op1, op2, op3, Tuple3::tuple3);
    }

    /**
     * Returns composite aggregate operation from 3 other aggregate operations.
     * It allows you to calculate multiple aggregations over the same items at once.
     *
     * @param op1 1st operation
     * @param op2 2nd operation
     * @param op3 3rd operation
     * @param finishFn a function combining 3 results into single target instance
     *
     * @param <T> type of input items
     * @param <A1> 1st accumulator type
     * @param <A2> 2nd accumulator type
     * @param <A3> 3rd accumulator type
     * @param <R1> 1st result type
     * @param <R2> 2nd result type
     * @param <R3> 3rd result type
     * @param <R> final result type
     *
     * @return the composite operation
     */
    @Nonnull
    public static <T, A1, A2, A3, R1, R2, R3, R> AggregateOperation1<T, Tuple3<A1, A2, A3>, R> allOf(
            @Nonnull AggregateOperation1<? super T, A1, R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, R2> op2,
            @Nonnull AggregateOperation1<? super T, A3, R3> op3,
            @Nonnull DistributedTriFunction<? super R1, ? super R2, ? super R3, R> finishFn
    ) {
        return AggregateOperation
                .withCreate(() -> tuple3(op1.createFn().get(), op2.createFn().get(), op3.createFn().get()))
                .<T>andAccumulate((acc, item) -> {
                    op1.accumulateFn().accept(acc.f0(), item);
                    op2.accumulateFn().accept(acc.f1(), item);
                    op3.accumulateFn().accept(acc.f2(), item);
                })
                .andCombine(op1.combineFn() == null || op2.combineFn() == null ? null :
                        (acc1, acc2) -> {
                            op1.combineFn().accept(acc1.f0(), acc2.f0());
                            op2.combineFn().accept(acc1.f1(), acc2.f1());
                            op3.combineFn().accept(acc1.f2(), acc2.f2());
                        })
                .andDeduct(op1.deductFn() == null || op2.deductFn() == null ? null :
                        (acc1, acc2) -> {
                            op1.deductFn().accept(acc1.f0(), acc2.f0());
                            op2.deductFn().accept(acc1.f1(), acc2.f1());
                            op3.deductFn().accept(acc1.f2(), acc2.f2());
                        })
                .andFinish(acc -> finishFn.apply(
                        op1.finishFn().apply(acc.f0()),
                        op2.finishFn().apply(acc.f1()),
                        op3.finishFn().apply(acc.f2())));
    }

    /**
     * Returns a builder to create a composite of multiple aggregate
     * operations. It allows you to calculate multiple aggregations over the
     * same items at once. The number of operations is unbounded. Results are
     * stored in single {@link com.hazelcast.jet.datamodel.ItemsByTag} object.
     * <p>
     * If you have exactly 2 or 3 operations, you might prefer more type-safe
     * versions ({@link #allOf(AggregateOperation1, AggregateOperation1) here}
     * or {@link #allOf(AggregateOperation1, AggregateOperation1,
     * AggregateOperation1) here}).
     * <p>
     * Example: to calculate sum and count at the same time, you can use:
     * <pre>{@code
     *     AllOfAggregationBuilder<Long> builder = allOfBuilder();
     *     Tag<Long> tagSum = builder.add(summingLong(Long::longValue));
     *     Tag<Long> tagCount = builder.add(counting());
     *     AggregateOperation1<Long, ?, ItemsByTag> op = builder.build();
     * }</pre>
     *
     * When you receive the resulting {@link com.hazelcast.jet.datamodel.ItemsByTag}
     * object, query individual values like this:
     * <pre>{@code
     *     ItemsByTag result = ...;
     *     Long sum = result.get(tagSum);
     *     Long count = result.get(tagCount);
     * }</pre>
     *
     * @param <T> type of input items
     * @return the builder
     */
    @Nonnull
    public static <T> AllOfAggregationBuilder<T> allOfBuilder() {
        return new AllOfAggregationBuilder<>();
    }

    /**
     * Returns an aggregate operation that concatenates the input items into a
     * string.
     */
    public static AggregateOperation1<CharSequence, StringBuilder, String> concatenating() {
        return AggregateOperation
                .withCreate(StringBuilder::new)
                .<CharSequence>andAccumulate(StringBuilder::append)
                .andCombine(StringBuilder::append)
                .andFinish(StringBuilder::toString);
    }

    /**
     * Returns an aggregate operation that concatenates the input items into a
     * string with the given {@code delimiter}.
     */
    public static AggregateOperation1<CharSequence, StringBuilder, String> concatenating(
            CharSequence delimiter
    ) {
        return concatenating(delimiter, "", "");
    }


    /**
     * Returns an aggregate operation that concatenates the input items into a
     * string with the given {@code delimiter}. The resulting string will also
     * have the given {@code prefix} and {@code suffix}.
     **/
    public static AggregateOperation1<CharSequence, StringBuilder, String> concatenating(
            CharSequence delimiter, CharSequence prefix, CharSequence suffix
    ) {
        int prefixLen = prefix.length();
        return AggregateOperation
                .withCreate(() -> new StringBuilder().append(prefix))
                .<CharSequence>andAccumulate((builder, val) -> {
                    if (builder.length() != prefixLen && val.length() > 0) {
                        builder.append(delimiter);
                    }
                    builder.append(val);
                })
                .andCombine((l, r) -> {
                    if (l.length() != prefixLen && r.length() != prefixLen) {
                        l.append(delimiter);
                    }
                    l.append(r, prefixLen, r.length());
                })
                .andFinish(r -> r.append(suffix).toString());
    }

    /**
     * Adapts an aggregate operation accepting items of type {@code
     * U} to one accepting items of type {@code T} by applying a mapping
     * function to each item before accumulation.
     * <p>
     * If the {@code mapFn} returns {@code null}, the item won't be aggregated
     * at all. This allows applying a filter at the same time.
     *
     * @param <T> input item type
     * @param <U> input type of the downstream aggregate operation
     * @param <A> downstream operation's accumulator type
     * @param <R> downstream operation's result type
     * @param mapFn the function to apply to input items
     * @param downstream the downstream aggregate operation
     */
    public static <T, U, A, R>
    AggregateOperation1<T, A, R> mapping(
            @Nonnull DistributedFunction<? super T, ? extends U> mapFn,
            @Nonnull AggregateOperation1<? super U, A, R> downstream
    ) {
        DistributedBiConsumer<? super A, ? super U> downstreamAccumulateFn = downstream.accumulateFn();
        return AggregateOperation
                .withCreate(downstream.createFn())
                .andAccumulate((A a, T t) -> {
                    U mapped = mapFn.apply(t);
                    if (mapped != null) {
                        downstreamAccumulateFn.accept(a, mapped);
                    }
                })
                .andCombine(downstream.combineFn())
                .andDeduct(downstream.deductFn())
                .andFinish(downstream.finishFn());
    }

    /**
     * Returns an aggregate operation that accumulates the items into a {@code
     * Collection}. It creates the collections as needed by calling the
     * provided {@code createCollectionFn}.
     * <p>
     * If you use a collection that preserves the insertion order, keep in mind
     * that there is no specified order in which the items are aggregated.
     *
     * @param <T> input item type
     * @param <C> the type of the collection
     * @param createCollectionFn a {@code Supplier} which returns a new, empty {@code Collection} of the
     *                           appropriate type
     */
    public static <T, C extends Collection<T>> AggregateOperation1<T, C, C> toCollection(
            DistributedSupplier<C> createCollectionFn
    ) {
        return AggregateOperation
                .withCreate(createCollectionFn)
                .<T>andAccumulate(Collection::add)
                .andCombine(Collection::addAll)
                .andIdentityFinish();
    }

    /**
     * Returns an aggregate operation that accumulates the items into an {@code
     * ArrayList}.
     *
     * @param <T> input item type
     */
    public static <T> AggregateOperation1<T, List<T>, List<T>> toList() {
        return toCollection(ArrayList::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a {@code
     * HashSet}.
     *
     * @param <T> input item type
     */
    public static <T> AggregateOperation1<T, Set<T>, Set<T>> toSet() {
        return toCollection(HashSet::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions.
     * <p>
     * This aggregate operation does not tolerate duplicate keys and will
     * throw {@code IllegalStateException} if it detects them. If your
     * data contains duplicates, use the {@link #toMap(DistributedFunction,
     * DistributedFunction, DistributedBinaryOperator) toMap()} overload
     * that can resolve them.
     *
     * @param <T> input item type
     * @param <K> type of the key
     * @param <U> type of the value
     * @param toKeyFn a function to extract the key from the input item
     * @param toValueFn a function to extract the value from the input item
     *
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator)
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator, DistributedSupplier)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> toKeyFn,
            DistributedFunction<? super T, ? extends U> toValueFn
    ) {
        return toMap(toKeyFn, toValueFn,
                (k, v) -> { throw new IllegalStateException("Duplicate key: " + k); },
                HashMap::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions.
     * <p>
     * This aggregate operation resolves duplicate keys by applying {@code
     * mergeFn} to the conflicting values. {@code mergeFn} will act upon the
     * values after {@code toValueFn} has already been applied.
     *
     * @param <T> input item type
     * @param <K> the type of key
     * @param <U> the output type of the value mapping function
     * @param toKeyFn a function to extract the key from input item
     * @param toValueFn a function to extract value from input item
     * @param mergeFn a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     *
     * @see #toMap(DistributedFunction, DistributedFunction)
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator, DistributedSupplier)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> toKeyFn,
            DistributedFunction<? super T, ? extends U> toValueFn,
            DistributedBinaryOperator<U> mergeFn
    ) {
        return toMap(toKeyFn, toValueFn, mergeFn, HashMap::new);
    }

    /**
     * Returns an {@code AggregateOperation1} that accumulates elements
     * into a {@code Map} whose keys and values are the result of applying the
     * provided mapping functions to the input elements.
     * <p>
     * If the mapped keys contain duplicates (according to {@link
     * Object#equals(Object)}), the value mapping function is applied to each
     * equal element, and the results are merged using the provided merging
     * function. The {@code Map} is created by a provided {@code createMapFn}
     * function.
     *
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param <M> the type of the resulting {@code Map}
     * @param toKeyFn a function to extract the key from input item
     * @param toValueFn a function to extract value from input item
     * @param mergeFn a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     * @param createMapFn a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     *
     * @see #toMap(DistributedFunction, DistributedFunction)
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator)
     */
    public static <T, K, U, M extends Map<K, U>> AggregateOperation1<T, M, M> toMap(
            DistributedFunction<? super T, ? extends K> toKeyFn,
            DistributedFunction<? super T, ? extends U> toValueFn,
            DistributedBinaryOperator<U> mergeFn,
            DistributedSupplier<M> createMapFn
    ) {
        DistributedBiConsumer<M, T> accumulateFn =
                (map, element) -> map.merge(toKeyFn.apply(element), toValueFn.apply(element), mergeFn);
        return AggregateOperation
                .withCreate(createMapFn)
                .andAccumulate(accumulateFn)
                .andCombine((l, r) -> r.forEach((key, value) -> l.merge(key, value, mergeFn)))
                .andIdentityFinish();
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates the items from
     * exactly two inputs into {@link TwoBags}: items from <em>inputN</em> are
     * accumulated into <em>bagN</em>.
     *
     * @param <T0> item type on input0
     * @param <T1> item type on input1
     *
     * @see #toThreeBags()
     * @see #toBagsByTag(Tag[])
     */
    @Nonnull
    public static <T0, T1> AggregateOperation2<T0, T1, TwoBags<T0, T1>, TwoBags<T0, T1>> toTwoBags() {
        return AggregateOperation
                .withCreate(TwoBags::<T0, T1>twoBags)
                .<T0>andAccumulate0((acc, item0) -> acc.bag0().add(item0))
                .<T1>andAccumulate1((acc, item1) -> acc.bag1().add(item1))
                .andCombine(TwoBags::combineWith)
                .andDeduct(TwoBags::deduct)
                .andFinish(TwoBags::finish);
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates the items from
     * exactly three inputs into {@link ThreeBags}: items from <em>inputN</em>
     * are accumulated into <em>bagN</em>.
     *
     * @param <T0> item type on input0
     * @param <T1> item type on input1
     * @param <T2> item type on input2
     *
     * @see #toTwoBags()
     * @see #toBagsByTag(Tag[])
     */
    @Nonnull
    public static <T0, T1, T2>
    AggregateOperation3<T0, T1, T2, ThreeBags<T0, T1, T2>, ThreeBags<T0, T1, T2>> toThreeBags() {
        return AggregateOperation
                .withCreate(ThreeBags::<T0, T1, T2>threeBags)
                .<T0>andAccumulate0((acc, item0) -> acc.bag0().add(item0))
                .<T1>andAccumulate1((acc, item1) -> acc.bag1().add(item1))
                .<T2>andAccumulate2((acc, item2) -> acc.bag2().add(item2))
                .andCombine(ThreeBags::combineWith)
                .andDeduct(ThreeBags::deduct)
                .andFinish(ThreeBags::finish);
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates the items from
     * any number of inputs into {@link BagsByTag}: items from <em>inputN</em>
     * are accumulated into under <em>tagN</em>.
     *
     * @see #toTwoBags()
     * @see #toThreeBags()
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static AggregateOperation<BagsByTag, BagsByTag> toBagsByTag(@Nonnull Tag<?> ... tags) {
        checkPositive(tags.length, "At least one tag required");
        VarArity<BagsByTag> builder = AggregateOperation
                .withCreate(BagsByTag::new)
                .andAccumulate(tags[0], (acc, item) -> ((Collection) acc.ensureBag(tags[0])).add(item));
        for (int i = 1; i < tags.length; i++) {
            Tag tag = tags[i];
            builder = builder.andAccumulate(tag, (acc, item) -> acc.ensureBag(tag).add(item));
        }
        return builder
                .andCombine(BagsByTag::combineWith)
                .andFinish(BagsByTag::finish);
    }

    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code HashMap} where the key is the result of applying {@code toKeyFn}
     * and the value is a list of the items with that key.
     * <p>
     * This operation achieves the effect of a cascaded group-by where the
     * members of each group are further classified by a secondary key.
     *
     * @param toKeyFn a function to extract the key from input item
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     *
     * @see #groupingBy(DistributedFunction, AggregateOperation1)
     * @see #groupingBy(DistributedFunction, DistributedSupplier, AggregateOperation1)
     */
    public static <T, K> AggregateOperation1<T, Map<K, List<T>>, Map<K, List<T>>> groupingBy(
            DistributedFunction<? super T, ? extends K> toKeyFn
    ) {
        return groupingBy(toKeyFn, toList());
    }

    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code HashMap} where the key is the result of applying {@code toKeyFn}
     * and the value is the result of applying the downstream aggregate
     * operation to the items with that key.
     * <p>
     * This operation achieves the effect of a cascaded group-by where the
     * members of each group are further classified by a secondary key.
     *
     * @param toKeyFn a function to extract the key from input item
     * @param downstream the downstream aggregate operation
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     * @param <R> the type of the downstream aggregation result
     * @param <A> downstream aggregation's accumulator type
     *
     * @see #groupingBy(DistributedFunction)
     * @see #groupingBy(DistributedFunction, DistributedSupplier, AggregateOperation1)
     */
    public static <T, K, A, R> AggregateOperation1<T, Map<K, A>, Map<K, R>> groupingBy(
            DistributedFunction<? super T, ? extends K> toKeyFn,
            AggregateOperation1<? super T, A, R> downstream
    ) {
        return groupingBy(toKeyFn, HashMap::new, downstream);
    }


    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code Map} (as obtained from {@code createMapFn}) where the key is the
     * result of applying {@code toKeyFn} and the value is the result of
     * applying the downstream aggregate operation to the items with that key.
     * <p>
     * This operation achieves the effect of a cascaded group-by where the
     * members of each group are further classified by a secondary key.
     *
     * @param toKeyFn a function to extract the key from input item
     * @param createMapFn a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     * @param downstream the downstream aggregate operation
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     * @param <R> the type of the downstream aggregation result
     * @param <A> downstream aggregation's accumulator type
     * @param <M> output type of the resulting {@code Map}
     *
     * @see #groupingBy(DistributedFunction)
     * @see #groupingBy(DistributedFunction, AggregateOperation1)
     */
    public static <T, K, R, A, M extends Map<K, R>> AggregateOperation1<T, Map<K, A>, M> groupingBy(
            DistributedFunction<? super T, ? extends K> toKeyFn,
            DistributedSupplier<M> createMapFn,
            AggregateOperation1<? super T, A, R> downstream
    ) {
        DistributedBiConsumer<? super Map<K, A>, T> accumulateFn = (m, t) -> {
            A acc = m.computeIfAbsent(toKeyFn.apply(t), k -> downstream.createFn().get());
            downstream.accumulateFn().accept(acc, t);
        };

        DistributedBiConsumer<Map<K, A>, Map<K, A>> combineFn = (l, r) ->
                r.forEach((key, value) -> l.merge(key, value, (a, b) -> {
                    downstream.combineFn().accept(a, b);
                    return a;
        }));

        // replace the map contents with finished values
        DistributedSupplier<Map<K, A>> createAccMapFn = (DistributedSupplier<Map<K, A>>) createMapFn;
        DistributedFunction<A, A> downstreamFinishFn = (DistributedFunction<A, A>) downstream.finishFn();
        DistributedFunction<? super Map<K, A>, M> finisher = accMap -> {
            accMap.replaceAll((K k, A v) -> downstreamFinishFn.apply(v));
            return (M) accMap;
        };

        return AggregateOperation.withCreate(createAccMapFn).andAccumulate(accumulateFn)
                .andCombine(combineFn).andFinish(finisher);
    }

    /**
     * A reducing operation maintains an accumulated value that starts out as
     * {@code emptyAccValue} and is iteratively transformed by applying
     * {@code combineAccValuesFn} to it and each stream item's accumulated
     * value, as returned from {@code toAccValueFn}. {@code combineAccValuesFn}
     * must be <em>associative</em> because it will also be used to combine
     * partial results, as well as <em>commutative</em> because the encounter
     * order of items is unspecified.
     * <p>
     * The optional {@code deductAccValueFn} allows Jet to compute the sliding
     * window in O(1) time. It must undo the effects of a previous {@code
     * combineAccValuesFn} call:
     * <pre>
     *     A accVal;  (has some pre-existing value)
     *     A itemAccVal = toAccValueFn.apply(item);
     *     A combined = combineAccValuesFn.apply(accVal, itemAccVal);
     *     A deducted = deductAccValueFn.apply(combined, itemAccVal);
     *     assert deducted.equals(accVal);
     * </pre>
     *
     * @param emptyAccValue the reducing operation's emptyAccValue element
     * @param toAccValueFn transforms the stream item into its accumulated value
     * @param combineAccValuesFn combines two accumulated values into one
     * @param deductAccValueFn deducts the right-hand accumulated value from the left-hand one
     *                        (optional)
     * @param <T> input item type
     * @param <A> type of the accumulated value
     */
    @Nonnull
    public static <T, A> AggregateOperation1<T, MutableReference<A>, A> reducing(
            @Nonnull A emptyAccValue,
            @Nonnull DistributedFunction<? super T, ? extends A> toAccValueFn,
            @Nonnull DistributedBinaryOperator<A> combineAccValuesFn,
            @Nullable DistributedBinaryOperator<A> deductAccValueFn
    ) {
        // workaround for spotbugs issue: https://github.com/spotbugs/spotbugs/issues/552
        DistributedBinaryOperator<A> deductFn = deductAccValueFn;
        return AggregateOperation
                .withCreate(() -> new MutableReference<>(emptyAccValue))
                .andAccumulate((MutableReference<A> a, T t) ->
                        a.set(combineAccValuesFn.apply(a.get(), toAccValueFn.apply(t))))
                .andCombine((a, b) -> a.set(combineAccValuesFn.apply(a.get(), b.get())))
                .andDeduct(deductAccValueFn != null
                        ? (a, b) -> a.set(deductFn.apply(a.get(), b.get()))
                        : null)
                .andFinish(MutableReference::get);
    }
}
