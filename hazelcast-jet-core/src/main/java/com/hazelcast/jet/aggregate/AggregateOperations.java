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
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
                .andAccumulate((LongAccumulator a, T item) -> a.addExact(1))
                .andCombine(LongAccumulator::addExact)
                .andDeduct(LongAccumulator::subtract)
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
            @Nonnull DistributedToLongFunction<T> getLongValueFn
    ) {
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator a, T item) -> a.addExact(getLongValueFn.applyAsLong(item)))
                .andCombine(LongAccumulator::addExact)
                .andDeduct(LongAccumulator::subtractExact)
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
            @Nonnull DistributedToDoubleFunction<T> getDoubleValueFn
    ) {
        return AggregateOperation
                .withCreate(DoubleAccumulator::new)
                .andAccumulate((DoubleAccumulator a, T item) -> a.add(getDoubleValueFn.applyAsDouble(item)))
                .andCombine(DoubleAccumulator::add)
                .andDeduct(DoubleAccumulator::subtract)
                .andFinish(DoubleAccumulator::get);
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
                    if (a.get() == null || comparator.compare(i, a.get()) > 0) {
                        a.set(i);
                    }
                })
                .andCombine((a1, a2) -> {
                    if (a1.get() == null || comparator.compare(a1.get(), a2.get()) < 0) {
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
            @Nonnull DistributedToLongFunction<T> getLongValueFn
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation
                .withCreate(LongLongAccumulator::new)
                .andAccumulate((LongLongAccumulator a, T i) -> {
                    // a bit faster check than in addExact, specialized for increment
                    if (a.getValue1() == Long.MAX_VALUE) {
                        throw new ArithmeticException("Counter overflow");
                    }
                    a.setValue1(a.getValue1() + 1);
                    a.setValue2(Math.addExact(a.getValue2(), getLongValueFn.applyAsLong(i)));
                })
                .andCombine((a1, a2) -> {
                    a1.setValue1(Math.addExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(Math.addExact(a1.getValue2(), a2.getValue2()));
                })
                .andDeduct((a1, a2) -> {
                    a1.setValue1(Math.subtractExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(Math.subtractExact(a1.getValue2(), a2.getValue2()));
                })
                .andFinish(a -> (double) a.getValue2() / a.getValue1());
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
            @Nonnull DistributedToDoubleFunction<T> getDoubleValueFn
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation
                .withCreate(LongDoubleAccumulator::new)
                .andAccumulate((LongDoubleAccumulator a, T item) -> {
                    // a bit faster check than in addExact, specialized for increment
                    if (a.getValue1() == Long.MAX_VALUE) {
                        throw new ArithmeticException("Counter overflow");
                    }
                    a.setValue1(a.getValue1() + 1);
                    a.setValue2(a.getValue2() + getDoubleValueFn.applyAsDouble(item));
                })
                .andCombine((a1, a2) -> {
                    a1.setValue1(Math.addExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(a1.getValue2() + a2.getValue2());
                })
                .andDeduct((a1, a2) -> {
                    a1.setValue1(Math.subtractExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(a1.getValue2() - a2.getValue2());
                })
                .andFinish(a -> a.getValue2() / a.getValue1());
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
     * Returns a composite operation that computes multiple aggregate
     * operations and returns their results in a {@code List<Object>}.
     *
     * @param operations aggregate operations to apply
     */
    @SafeVarargs @Nonnull
    public static <T> AggregateOperation1<T, List<Object>, List<Object>> allOf(
            @Nonnull AggregateOperation1<? super T, ?, ?>... operations
    ) {
        AggregateOperation1[] untypedOps = operations;

        return AggregateOperation
                .withCreate(() -> {
                    List<Object> res = new ArrayList<>(untypedOps.length);
                    for (AggregateOperation untypedOp : untypedOps) {
                        res.add(untypedOp.createFn().get());
                    }
                    return res;
                })
                .andAccumulate((List<Object> accs, T item) -> {
                    for (int i = 0; i < untypedOps.length; i++) {
                        untypedOps[i].accumulateFn().accept(accs.get(i), item);
                    }
                })
                .andCombine(
                        // we can support combine only if all operations do
                        Stream.of(untypedOps).allMatch(o -> o.combineFn() != null)
                                ? (accs1, accs2) -> {
                                    for (int i = 0; i < untypedOps.length; i++) {
                                        untypedOps[i].combineFn().accept(accs1.get(i), accs2.get(i));
                            }
                        } : null)
                .andDeduct(
                        // we can support deduct only if all operations do
                        Stream.of(untypedOps).allMatch(o -> o.deductFn() != null)
                                ? (accs1, accs2) -> {
                                    for (int i = 0; i < untypedOps.length; i++) {
                                        untypedOps[i].deductFn().accept(accs1.get(i), accs2.get(i));
                                    }
                                }
                                : null)
                .andFinish(accs -> {
                    List<Object> res = new ArrayList<>(untypedOps.length);
                    for (int i = 0; i < untypedOps.length; i++) {
                        res.add(untypedOps[i].finishFn().apply(accs.get(i)));
                    }
                    return res;
                });
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
    AggregateOperation1<T, ?, R> mapping(
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
    public static <T>
    AggregateOperation1<T, ?, Set<T>> toSet() {
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
     * @param getKeyFn a function to extract the key from the input item
     * @param getValueFn a function to extract the value from the input item
     *
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator)
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator, DistributedSupplier)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> getKeyFn,
            DistributedFunction<? super T, ? extends U> getValueFn
    ) {
        return toMap(getKeyFn, getValueFn, throwingMerger(), HashMap::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions.
     * <p>
     * This aggregate operation resolves duplicate keys by applying {@code
     * mergeFn} to the conflicting values. {@code mergeFn} will act upon the
     * values after {@code getValueFn} has already been applied.
     *
     * @param <T> input item type
     * @param <K> the type of key
     * @param <U> the output type of the value mapping function
     * @param getKeyFn a function to extract the key from input item
     * @param getValueFn a function to extract value from input item
     * @param mergeFn a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     *
     * @see #toMap(DistributedFunction, DistributedFunction)
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator, DistributedSupplier)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> getKeyFn,
            DistributedFunction<? super T, ? extends U> getValueFn,
            DistributedBinaryOperator<U> mergeFn
    ) {
        return toMap(getKeyFn, getValueFn, mergeFn, HashMap::new);
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
     * @param getKeyFn a function to extract the key from input item
     * @param getValueFn a function to extract value from input item
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
            DistributedFunction<? super T, ? extends K> getKeyFn,
            DistributedFunction<? super T, ? extends U> getValueFn,
            DistributedBinaryOperator<U> mergeFn,
            DistributedSupplier<M> createMapFn
    ) {
        DistributedBiConsumer<M, T> accumulateFn =
                (map, element) -> map.merge(getKeyFn.apply(element), getValueFn.apply(element), mergeFn);
        return AggregateOperation
                .withCreate(createMapFn)
                .andAccumulate(accumulateFn)
                .andCombine(mapMerger(mergeFn))
                .andIdentityFinish();
    }

    private static <T> DistributedBinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException("Duplicate key: " + u);
        };
    }

    private static <K, V, M extends Map<K, V>> DistributedBiConsumer<M, M> mapMerger(
            DistributedBinaryOperator<V> mergeFunction
    ) {
        return (m1, m2) -> {
            for (Map.Entry<K, V> e : m2.entrySet()) {
                m1.merge(e.getKey(), e.getValue(), mergeFunction);
            }
        };
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
        return AggregateOperation
                .withCreate(() -> new MutableReference<>(emptyAccValue))
                .andAccumulate((MutableReference<A> a, T t) ->
                        a.set(combineAccValuesFn.apply(a.get(), toAccValueFn.apply(t))))
                .andCombine((a, b) -> a.set(combineAccValuesFn.apply(a.get(), b.get())))
                .andDeduct(deductAccValueFn != null
                        ? (a, b) -> a.set(deductAccValueFn.apply(a.get(), b.get()))
                        : null)
                .andFinish(MutableReference::get);
    }
}
