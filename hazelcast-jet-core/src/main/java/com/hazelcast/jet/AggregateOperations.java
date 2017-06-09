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

package com.hazelcast.jet;

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

import static com.hazelcast.jet.function.DistributedFunction.identity;

/**
 * Utility class with factory methods for several useful windowing
 * operations.
 */
public final class AggregateOperations {

    private AggregateOperations() {
    }

    /**
     * Returns an operation that tracks the count of items in the window.
     */
    @Nonnull
    public static AggregateOperation<Object, LongAccumulator, Long> counting() {
        return AggregateOperation.of(
                LongAccumulator::new,
                (a, item) -> a.addExact(1),
                LongAccumulator::addExact,
                LongAccumulator::subtract,
                LongAccumulator::get
        );
    }

    /**
     * Returns an operation that tracks the sum of the quantity returned by
     * {@code mapToLongF} applied to each item in the window.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, LongAccumulator, Long> summingLong(
            @Nonnull DistributedToLongFunction<T> mapToLongF
    ) {
        return AggregateOperation.of(
                LongAccumulator::new,
                (a, item) -> a.addExact(mapToLongF.applyAsLong(item)),
                LongAccumulator::addExact,
                LongAccumulator::subtractExact,
                LongAccumulator::get
        );
    }

    /**
     * Returns an operation that tracks the sum of the quantity returned by
     * {@code mapToDoubleF} applied to each item in the window.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, DoubleAccumulator, Double> summingDouble(
            @Nonnull DistributedToDoubleFunction<T> mapToDoubleF
    ) {
        return AggregateOperation.of(
                DoubleAccumulator::new,
                (a, item) -> a.add(mapToDoubleF.applyAsDouble(item)),
                DoubleAccumulator::add,
                DoubleAccumulator::subtract,
                DoubleAccumulator::get
        );
    }

    /**
     * Returns an operation that returns the minimum item, according the given
     * {@code comparator}.
     * <p>
     * The implementation doesn't have the <i>deduction function </i>. {@link
     * AggregateOperation#deductAccumulatorF() See note here}.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, MutableReference<T>, T> minBy(
            @Nonnull DistributedComparator<? super T> comparator
    ) {
        return maxBy(comparator.reversed());
    }

    /**
     * Returns an operation that returns the maximum item, according the given
     * {@code comparator}.
     * <p>
     * The implementation doesn't have the <i>deduction function </i>. {@link
     * AggregateOperation#deductAccumulatorF() See note here}.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, MutableReference<T>, T> maxBy(
            @Nonnull DistributedComparator<? super T> comparator
    ) {
        return AggregateOperation.of(
                MutableReference::new,
                (a, i) -> {
                    if (a.get() == null || comparator.compare(i, a.get()) > 0) {
                        a.set(i);
                    }
                },
                (a1, a2) -> {
                    if (a1.get() == null || comparator.compare(a1.get(), a2.get()) < 0) {
                        a1.set(a2.get());
                    }
                },
                null,
                MutableReference::get
        );
    }

    /**
     * Returns an operation that calculates the arithmetic mean of {@code long}
     * values returned by the {@code mapToLongF} function.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, LongLongAccumulator, Double> averagingLong(
            @Nonnull DistributedToLongFunction<T> mapToLongF
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation.of(
                LongLongAccumulator::new,
                (a, i) -> {
                    if (a.getValue1() == Long.MAX_VALUE) {
                        // this is a bit faster overflow check when we know that we are adding 1
                        throw new ArithmeticException("long overflow");
                    }
                    a.setValue1(a.getValue1() + 1);
                    a.setValue2(Math.addExact(a.getValue2(), mapToLongF.applyAsLong(i)));
                },
                (a1, a2) -> {
                    a1.setValue1(Math.addExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(Math.addExact(a1.getValue2(), a2.getValue2()));
                },
                (a1, a2) -> {
                    a1.setValue1(Math.subtractExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(Math.subtractExact(a1.getValue2(), a2.getValue2()));
                },
                a -> (double) a.getValue2() / a.getValue1()
        );
    }

    /**
     * Returns an operation that calculates the arithmetic mean of {@code double}
     * values returned by the {@code mapToDoubleF} function.
     *
     * @param <T> Input item type
     */
    @Nonnull
    public static <T> AggregateOperation<T, LongDoubleAccumulator, Double> averagingDouble(
            @Nonnull DistributedToDoubleFunction<T> mapToDoubleF
    ) {
        // accumulator.value1 is count
        // accumulator.value2 is sum
        return AggregateOperation.of(
                LongDoubleAccumulator::new,
                (a, i) -> {
                    if (a.getValue1() == Long.MAX_VALUE) {
                        // this is a bit faster overflow check when we know that we are adding 1
                        throw new ArithmeticException("long overflow");
                    }
                    a.setValue1(a.getValue1() + 1);
                    a.setValue2(a.getValue2() + mapToDoubleF.applyAsDouble(i));
                },
                (a1, a2) -> {
                    a1.setValue1(Math.addExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(a1.getValue2() + a2.getValue2());
                },
                (a1, a2) -> {
                    a1.setValue1(Math.subtractExact(a1.getValue1(), a2.getValue1()));
                    a1.setValue2(a1.getValue2() - a2.getValue2());
                },
                a -> a.getValue2() / a.getValue1()
        );
    }

    /**
     * Returns an operation that computes a linear trend on the items in the
     * window. The operation will produce a {@code double}-valued coefficient
     * that approximates the rate of change of {@code y} as a function of
     * {@code x}, where {@code x} and {@code y} are {@code long} quantities
     * extracted from each item by the two provided functions.
     */
    @Nonnull
    public static <T> AggregateOperation<T, LinTrendAccumulator, Double> linearTrend(
            @Nonnull DistributedToLongFunction<T> getX,
            @Nonnull DistributedToLongFunction<T> getY
    ) {
        return AggregateOperation.of(
                LinTrendAccumulator::new,
                (a, item) -> a.accumulate(getX.applyAsLong(item), getY.applyAsLong(item)),
                LinTrendAccumulator::combine,
                LinTrendAccumulator::deduct,
                LinTrendAccumulator::finish
        );
    }

    /**
     * Returns an operation, that calculates multiple aggregations and returns their value in
     * {@code List<Object>}.
     * <p>
     * Useful, if you want to calculate multiple values for the same window.
     *
     * @param operations Operations to calculate.
     */
    @SafeVarargs @Nonnull
    public static <T> AggregateOperation<T, List<Object>, List<Object>> allOf(
            @Nonnull AggregateOperation<? super T, ?, ?> ... operations
    ) {
        AggregateOperation[] untypedOps = operations;

        return AggregateOperation.of(
                () -> {
                    List<Object> res = new ArrayList<>(untypedOps.length);
                    for (AggregateOperation untypedOp : untypedOps) {
                        res.add(untypedOp.createAccumulatorF().get());
                    }
                    return res;
                },
                (accs, item) -> {
                    for (int i = 0; i < untypedOps.length; i++) {
                        untypedOps[i].accumulateItemF().accept(accs.get(i), item);
                    }
                },
                (accs1, accs2) -> {
                    for (int i = 0; i < untypedOps.length; i++) {
                        untypedOps[i].combineAccumulatorsF().accept(accs1.get(i), accs2.get(i));
                    }
                },
                // we can support deduct only if all operations do
                Stream.of(untypedOps).allMatch(o -> o.deductAccumulatorF() != null)
                        ? (accs1, accs2) -> {
                            for (int i = 0; i < untypedOps.length; i++) {
                                untypedOps[i].deductAccumulatorF().accept(accs1.get(i), accs2.get(i));
                            }
                        }
                        : null,
                accs -> {
                    List<Object> res = new ArrayList<>(untypedOps.length);
                    for (int i = 0; i < untypedOps.length; i++) {
                        res.add(untypedOps[i].finishAccumulationF().apply(accs.get(i)));
                    }
                    return res;
                }
        );
    }

    /**
     * Adapts an {@code AggregateOperation} accepting elements of type {@code
     * U} to one accepting elements of type {@code T} by applying a mapping
     * function to each input element before accumulation.
     * <p>
     * If the {@code mapF} maps to {@code null}, the item won't be aggregated
     * at all. This allows the mapping to be used as a filter at the same time.
     * <p>
     * This operation is useful if we cannot precede the aggregating vertex
     * with a {@link
     * com.hazelcast.jet.processor.Processors#map(DistributedFunction) map()}
     * processors, which is useful
     *
     * @param <T> the type of the input elements
     * @param <U> type of elements accepted by downstream operation
     * @param <A> intermediate accumulation type of the downstream operation
     * @param <R> result type of operation
     * @param mapF a function to be applied to the input elements
     * @param downstream an operation which will accept mapped values
     */
    public static <T, U, A, R>
    AggregateOperation<T, ?, R> mapping(@Nonnull DistributedFunction<? super T, ? extends U> mapF,
                               @Nonnull AggregateOperation<? super U, A, R> downstream) {
        DistributedBiConsumer<? super A, ? super U> downstreamAccumulateF = downstream.accumulateItemF();
        return AggregateOperation.of(
                downstream.createAccumulatorF(),
                (r, t) -> {
                    U mapped = mapF.apply(t);
                    if (mapped != null) {
                        downstreamAccumulateF.accept(r, mapped);
                    }
                },
                downstream.combineAccumulatorsF(),
                downstream.deductAccumulatorF(),
                downstream.finishAccumulationF());
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates the input
     * elements into a new {@code Collection}. The {@code Collection} is
     * created by the provided factory.
     * <p>
     * Note: due to the distributed nature of processing the order might be
     * unspecified.
     *
     * @param <T> the type of the input elements
     * @param <C> the type of the resulting {@code Collection}
     * @param createCollectionF a {@code Supplier} which returns a new, empty
     *                          {@code Collection} of the appropriate type
     */
    public static <T, C extends Collection<T>> AggregateOperation<T, C, C> toCollection(
            DistributedSupplier<C> createCollectionF) {
        return AggregateOperation.of(
                createCollectionF,
                Collection::add,
                Collection::addAll,
                null,
                identity());
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates the input
     * elements into a new {@code ArrayList}.
     *
     * @param <T> the type of the input elements
     */
    public static <T> AggregateOperation<T, List<T>, List<T>> toList() {
        return toCollection(ArrayList::new);
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates the input
     * elements into a new {@code HashSet}.
     *
     * @param <T> the type of the input elements
     */
    public static <T>
    AggregateOperation<T, ?, Set<T>> toSet() {
        return toCollection(HashSet::new);
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates elements
     * into a {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions to the input elements.
     * <p>
     * If the mapped keys contain duplicates (according to {@link
     * Object#equals(Object)}), an {@code IllegalStateException} is thrown when
     * the collection operation is performed.  If the mapped keys may have
     * duplicates, use {@link #toMap(DistributedFunction, DistributedFunction,
     * DistributedBinaryOperator)} instead.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param getKeyF a function to extract the key from input item
     * @param getValueF a function to extract value from input item
     *
     * @see #toMap(DistributedFunction, DistributedFunction,
     *      DistributedBinaryOperator)
     * @see #toMap(DistributedFunction, DistributedFunction,
     *      DistributedBinaryOperator, DistributedSupplier)
     */
    public static <T, K, U> AggregateOperation<T, Map<K, U>, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> getKeyF,
            DistributedFunction<? super T, ? extends U> getValueF
    ) {
        return toMap(getKeyF, getValueF, throwingMerger(), HashMap::new);
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates elements
     * into a {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions to the input elements.
     *
     * <p>If the mapped keys contains duplicates (according to {@link
     * Object#equals(Object)}), the value mapping function is applied to each
     * equal element, and the results are merged using the provided merging
     * function.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param getKeyF a function to extract the key from input item
     * @param getValueF a function to extract value from input item
     * @param mergeF a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     *
     * @see #toMap(DistributedFunction, DistributedFunction)
     * @see #toMap(DistributedFunction, DistributedFunction,
     *      DistributedBinaryOperator, DistributedSupplier)
     */
    public static <T, K, U> AggregateOperation<T, Map<K, U>, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> getKeyF,
            DistributedFunction<? super T, ? extends U> getValueF,
            DistributedBinaryOperator<U> mergeF
    ) {
        return toMap(getKeyF, getValueF, mergeF, HashMap::new);
    }

    /**
     * Returns an {@code AggregateOperation} that accumulates elements
     * into a {@code Map} whose keys and values are the result of applying the
     * provided mapping functions to the input elements.
     * <p>
     * If the mapped keys contain duplicates (according to {@link
     * Object#equals(Object)}), the value mapping function is applied to each
     * equal element, and the results are merged using the provided merging
     * function. The {@code Map} is created by a provided {@code createMapF}
     * function.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param <M> the type of the resulting {@code Map}
     * @param getKeyF a function to extract the key from input item
     * @param getValueF a function to extract value from input item
     * @param mergeF a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     * @param createMapF a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     *
     * @see #toMap(DistributedFunction, DistributedFunction)
     * @see #toMap(DistributedFunction, DistributedFunction, DistributedBinaryOperator)
     */
    public static <T, K, U, M extends Map<K, U>> AggregateOperation<T, M, M> toMap(
            DistributedFunction<? super T, ? extends K> getKeyF,
            DistributedFunction<? super T, ? extends U> getValueF,
            DistributedBinaryOperator<U> mergeF,
            DistributedSupplier<M> createMapF
    ) {
        DistributedBiConsumer<M, T> accumulateF =
                (map, element) -> map.merge(getKeyF.apply(element),
                    getValueF.apply(element), mergeF);
        return AggregateOperation.of(
                createMapF,
                accumulateF,
                mapMerger(mergeF),
                null,
                identity());
    }

    private static <T> DistributedBinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException("Duplicate key: " + u);
        };
    }

    private static <K, V, M extends Map<K, V>> DistributedBiConsumer<M, M> mapMerger(
            DistributedBinaryOperator<V> mergeFunction) {
        return (m1, m2) -> {
            for (Map.Entry<K, V> e : m2.entrySet()) {
                m1.merge(e.getKey(), e.getValue(), mergeFunction);
            }
        };
    }

    /**
     * A reducing operation maintains an accumulated value that starts out as
     * {@code emptyAccValue} and is being iteratively transformed by applying
     * the {@code combine} primitive to it and each stream item's accumulated
     * value, as returned from {@code toAccValueF}. The {@code combine} must
     * be <em>associative</em> because it will also be used to combine partial
     * results, and <em>commutative</em> because the encounter order of items
     * is unspecified.
     * <p>
     * The optional {@code deduct} primitive allows Jet to compute the sliding
     * window in O(1) time. It must undo the effects of a previous
     * {@code combine}:
     * <pre>
     *     A accVal;  (has some pre-existing value)
     *     A itemAccVal = toAccValueF.apply(item);
     *     A combined = combineAccValuesF.apply(accVal, itemAccVal);
     *     A deducted = deductAccValueF.apply(combined, itemAccVal);
     *     assert deducted.equals(accVal);
     * </pre>
     *
     * @param emptyAccValue the reducing operation's emptyAccValue element
     * @param toAccValueF transforms the stream item into its accumulated value
     * @param combineAccValuesF combines two accumulated values into one
     * @param deductAccValueF deducts the right-hand accumulated value from the left-hand one
     *                        (optional)
     * @param <T> type of the stream item
     * @param <A> type of the accumulated value
     */
    @Nonnull
    public static <T, A> AggregateOperation<T, MutableReference<A>, A> reducing(
            @Nonnull A emptyAccValue,
            @Nonnull DistributedFunction<? super T, ? extends A> toAccValueF,
            @Nonnull DistributedBinaryOperator<A> combineAccValuesF,
            @Nullable DistributedBinaryOperator<A> deductAccValueF
    ) {
        return AggregateOperation.of(
                () -> new MutableReference<>(emptyAccValue),
                (a, t) -> a.set(combineAccValuesF.apply(a.get(), toAccValueF.apply(t))),
                (a, b) -> a.set(combineAccValuesF.apply(a.get(), b.get())),
                deductAccValueF != null ? (a, b) -> a.set(deductAccValueF.apply(a.get(), b.get())) : null,
                MutableReference::get);
    }
}
