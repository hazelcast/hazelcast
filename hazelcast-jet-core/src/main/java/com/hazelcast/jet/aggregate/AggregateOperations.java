/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.ComparatorEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BinaryOperatorEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.ToDoubleFunctionEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.StageWithWindow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.FunctionEx.identity;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Utility class with factory methods for several useful aggregate
 * operations. See the Javadoc on {@link AggregateOperation}. You can
 * also create your own aggregate operation using the {@link
 * AggregateOperation#withCreate builder object}.
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
                .andExportFinish(LongAccumulator::get);
    }

    /**
     * Returns an aggregate operation that computes the sum of the {@code long}
     * values it obtains by applying {@code getLongValueFn} to each item.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongAccumulator, Long> summingLong(
            @Nonnull ToLongFunctionEx<? super T> getLongValueFn
    ) {
        checkSerializable(getLongValueFn, "getLongValueFn");
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator a, T item) -> a.add(getLongValueFn.applyAsLong(item)))
                .andCombine(LongAccumulator::add)
                .andDeduct(LongAccumulator::subtract)
                .andExportFinish(LongAccumulator::get);
    }

    /**
     * Returns an aggregate operation that computes the sum of the {@code double}
     * values it obtains by applying {@code getDoubleValueFn} to each item.
     *
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, DoubleAccumulator, Double> summingDouble(
            @Nonnull ToDoubleFunctionEx<? super T> getDoubleValueFn
    ) {
        checkSerializable(getDoubleValueFn, "getDoubleValueFn");
        return AggregateOperation
                .withCreate(DoubleAccumulator::new)
                .andAccumulate((DoubleAccumulator a, T item) -> a.accumulate(getDoubleValueFn.applyAsDouble(item)))
                .andCombine(DoubleAccumulator::combine)
                .andDeduct(DoubleAccumulator::deduct)
                .andExportFinish(DoubleAccumulator::export);
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
            @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
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
            @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        return AggregateOperation
                .withCreate(MutableReference<T>::new)
                .andAccumulate((MutableReference<T> a, T i) -> {
                    if (a.isNull() || comparator.compare(i, a.get()) > 0) {
                        a.set(i);
                    }
                })
                .andCombine((a1, a2) -> {
                    if (a1.isNull() || comparator.compare(a1.get(), a2.get()) < 0) {
                        a1.set(a2.get());
                    }
                })
                .andExportFinish(MutableReference::get);
    }

    /**
     * Returns an aggregate operation that computes the top {@code n} items
     * calculated according to the given {@link ComparatorEx comparator}.
     * The returned list of elements is sorted in descending order.
     * <p>
     * This aggregate operation does not implement the {@link
     * AggregateOperation1#deductFn() deduct} primitive.
     *
     * @param n number of items to return
     * @param comparator the comparator to use
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, PriorityQueue<T>, List<T>> topN(
            int n, @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        ComparatorEx<? super T> comparatorReversed = comparator.reversed();
        BiConsumerEx<PriorityQueue<T>, T> accumulateFn = (queue, item) -> {
            if (queue.size() == n) {
                if (comparator.compare(item, queue.peek()) <= 0) {
                    // the new item is smaller or equal to the smallest in queue
                    return;
                }
                queue.poll();
            }
            queue.offer(item);
        };
        return AggregateOperation
                .withCreate(() -> new PriorityQueue<T>(n, comparator))
                .andAccumulate(accumulateFn)
                .andCombine((left, right) -> {
                    for (T item : right) {
                        accumulateFn.accept(left, item);
                    }
                })
                .andExportFinish(queue -> {
                    ArrayList<T> res = new ArrayList<>(queue);
                    res.sort(comparatorReversed);
                    return res;
                });
    }

    /**
     * Returns an aggregate operation that computes the bottom {@code n} items
     * calculated according to the given {@link ComparatorEx comparator}.
     * The returned list of elements is sorted in ascending order.
     * <p>
     * This aggregate operation does not implement the {@link
     * AggregateOperation1#deductFn() deduct} primitive.
     *
     * @param n number of items to return
     * @param comparator the comparator to use
     * @param <T> input item type
     */
    @Nonnull
    public static <T> AggregateOperation1<T, PriorityQueue<T>, List<T>> bottomN(
            int n, @Nonnull ComparatorEx<? super T> comparator
    ) {
        return topN(n, comparator.reversed());
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
            @Nonnull ToLongFunctionEx<? super T> getLongValueFn
    ) {
        checkSerializable(getLongValueFn, "getLongValueFn");
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
                .andExportFinish(a -> (double) a.get2() / a.get1());
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
            @Nonnull ToDoubleFunctionEx<? super T> getDoubleValueFn
    ) {
        checkSerializable(getDoubleValueFn, "getDoubleValueFn");
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
                .andExportFinish(a -> a.getDouble() / a.getLong());
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
            @Nonnull ToLongFunctionEx<T> getXFn,
            @Nonnull ToLongFunctionEx<T> getYFn
    ) {
        checkSerializable(getXFn, "getXFn");
        checkSerializable(getYFn, "getYFn");
        return AggregateOperation
                .withCreate(LinTrendAccumulator::new)
                .andAccumulate((LinTrendAccumulator a, T item) ->
                        a.accumulate(getXFn.applyAsLong(item), getYFn.applyAsLong(item)))
                .andCombine(LinTrendAccumulator::combine)
                .andDeduct(LinTrendAccumulator::deduct)
                .andExportFinish(LinTrendAccumulator::export);
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
                .andExportFinish(StringBuilder::toString);
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
                .andExportFinish(r -> {
                    try {
                        return r.append(suffix).toString();
                    } finally {
                        r.setLength(r.length() - suffix.length());
                    }
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
    AggregateOperation1<T, A, R> mapping(
            @Nonnull FunctionEx<? super T, ? extends U> mapFn,
            @Nonnull AggregateOperation1<? super U, A, ? extends R> downstream
    ) {
        checkSerializable(mapFn, "mapFn");
        BiConsumerEx<? super A, ? super U> downstreamAccumulateFn = downstream.accumulateFn();
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
                .<R>andExport(downstream.exportFn())
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
            SupplierEx<C> createCollectionFn
    ) {
        checkSerializable(createCollectionFn, "createCollectionFn");
        return AggregateOperation
                .withCreate(createCollectionFn)
                .<T>andAccumulate(Collection::add)
                .andCombine(Collection::addAll)
                .andExport(acc -> {
                    C result = createCollectionFn.get();
                    result.addAll(acc);
                    return result;
                })
                .andFinish(identity());
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
     * data contains duplicates, use the {@link #toMap(FunctionEx,
     * FunctionEx, BinaryOperatorEx) toMap()} overload
     * that can resolve them.
     *
     * @param <T> input item type
     * @param <K> type of the key
     * @param <U> type of the value
     * @param keyFn a function to extract the key from the input item
     * @param valueFn a function to extract the value from the input item
     *
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx)
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx, SupplierEx)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            FunctionEx<? super T, ? extends K> keyFn,
            FunctionEx<? super T, ? extends U> valueFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(valueFn, "valueFn");
        return toMap(keyFn, valueFn,
                (k, v) -> {
                    throw new IllegalStateException("Duplicate key: " + k);
                },
                HashMap::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions.
     * <p>
     * This aggregate operation resolves duplicate keys by applying {@code
     * mergeFn} to the conflicting values. {@code mergeFn} will act upon the
     * values after {@code valueFn} has already been applied.
     *
     * @param <T> input item type
     * @param <K> the type of key
     * @param <U> the output type of the value mapping function
     * @param keyFn a function to extract the key from input item
     * @param valueFn a function to extract value from input item
     * @param mergeFn a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     *
     * @see #toMap(FunctionEx, FunctionEx)
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx, SupplierEx)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            FunctionEx<? super T, ? extends K> keyFn,
            FunctionEx<? super T, ? extends U> valueFn,
            BinaryOperatorEx<U> mergeFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(valueFn, "valueFn");
        return toMap(keyFn, valueFn, mergeFn, HashMap::new);
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
     * @param keyFn a function to extract the key from input item
     * @param valueFn a function to extract value from input item
     * @param mergeFn a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     * @param createMapFn a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     *
     * @see #toMap(FunctionEx, FunctionEx)
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx)
     */
    public static <T, K, U, M extends Map<K, U>> AggregateOperation1<T, M, M> toMap(
            FunctionEx<? super T, ? extends K> keyFn,
            FunctionEx<? super T, ? extends U> valueFn,
            BinaryOperatorEx<U> mergeFn,
            SupplierEx<M> createMapFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(valueFn, "valueFn");
        checkSerializable(mergeFn, "mergeFn");
        checkSerializable(createMapFn, "createMapFn");
        BiConsumerEx<M, T> accumulateFn =
                (map, element) -> map.merge(keyFn.apply(element), valueFn.apply(element), mergeFn);
        return AggregateOperation
                .withCreate(createMapFn)
                .andAccumulate(accumulateFn)
                .andCombine((l, r) -> r.forEach((key, value) -> l.merge(key, value, mergeFn)))
                .andExport(acc -> {
                    M result = createMapFn.get();
                    result.putAll(acc);
                    return result;
                })
                .andFinish(identity());
    }

    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code HashMap} where the key is the result of applying {@code keyFn}
     * and the value is a list of the items with that key.
     * <p>
     * This operation achieves the effect of a cascaded group-by where the
     * members of each group are further classified by a secondary key.
     *
     * @param keyFn a function to extract the key from input item
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     *
     * @see #groupingBy(FunctionEx, AggregateOperation1)
     * @see #groupingBy(FunctionEx, SupplierEx, AggregateOperation1)
     */
    public static <T, K> AggregateOperation1<T, Map<K, List<T>>, Map<K, List<T>>> groupingBy(
            FunctionEx<? super T, ? extends K> keyFn
    ) {
        checkSerializable(keyFn, "keyFn");
        return groupingBy(keyFn, toList());
    }

    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code HashMap} where the key is the result of applying {@code keyFn}
     * and the value is the result of applying the downstream aggregate
     * operation to the items with that key.
     * <p>
     * This operation achieves the effect of a cascaded group-by where the
     * members of each group are further classified by a secondary key.
     *
     * @param keyFn a function to extract the key from input item
     * @param downstream the downstream aggregate operation
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     * @param <R> the type of the downstream aggregation result
     * @param <A> downstream aggregation's accumulator type
     *
     * @see #groupingBy(FunctionEx)
     * @see #groupingBy(FunctionEx, SupplierEx, AggregateOperation1)
     */
    public static <T, K, A, R> AggregateOperation1<T, Map<K, A>, Map<K, R>> groupingBy(
            FunctionEx<? super T, ? extends K> keyFn,
            AggregateOperation1<? super T, A, R> downstream
    ) {
        checkSerializable(keyFn, "keyFn");
        return groupingBy(keyFn, HashMap::new, downstream);
    }


    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code Map} (as obtained from {@code createMapFn}) where the key is the
     * result of applying {@code keyFn} and the value is the result of
     * applying the downstream aggregate operation to the items with that key.
     * <p>
     * This operation achieves the effect of a cascaded group-by where the
     * members of each group are further classified by a secondary key.
     *
     * @param keyFn a function to extract the key from input item
     * @param createMapFn a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     * @param downstream the downstream aggregate operation
     * @param <T> input item type
     * @param <K> the output type of the key mapping function
     * @param <R> the type of the downstream aggregation result
     * @param <A> downstream aggregation's accumulator type
     * @param <M> output type of the resulting {@code Map}
     *
     * @see #groupingBy(FunctionEx)
     * @see #groupingBy(FunctionEx, AggregateOperation1)
     */
    @SuppressWarnings("unchecked")
    public static <T, K, R, A, M extends Map<K, R>> AggregateOperation1<T, Map<K, A>, M> groupingBy(
            FunctionEx<? super T, ? extends K> keyFn,
            SupplierEx<M> createMapFn,
            AggregateOperation1<? super T, A, R> downstream
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(createMapFn, "createMapFn");

        BiConsumerEx<? super Map<K, A>, T> accumulateFn = (m, t) -> {
            A acc = m.computeIfAbsent(keyFn.apply(t), k -> downstream.createFn().get());
            downstream.accumulateFn().accept(acc, t);
        };

        BiConsumerEx<Map<K, A>, Map<K, A>> combineFn;
        BiConsumerEx<? super A, ? super A> downstreamCombineFn = downstream.combineFn();
        if (downstreamCombineFn != null) {
            combineFn = (l, r) ->
                    r.forEach((key, value) -> l.merge(key, value, (a, b) -> {
                        downstreamCombineFn.accept(a, b);
                        return a;
                    }));
        } else {
            combineFn = null;
        }

        // replace the map contents with finished values
        SupplierEx<Map<K, A>> createAccMapFn = (SupplierEx<Map<K, A>>) createMapFn;

        return (AggregateOperation1<T, Map<K, A>, M>) AggregateOperation
                .withCreate(createAccMapFn)
                .andAccumulate(accumulateFn)
                .andCombine(combineFn)
                .andExport(accMap -> accMap.entrySet().stream()
                                           .collect(Collectors.toMap(
                                                   Entry::getKey,
                                                   e -> downstream.exportFn().apply(e.getValue()))))
                .andFinish(accMap -> {
                    // replace the values in map in-place
                    accMap.replaceAll((K k, A v) -> ((FunctionEx<A, A>) downstream.finishFn()).apply(v));
                    return (Map) accMap;
                });
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
            @Nonnull FunctionEx<? super T, ? extends A> toAccValueFn,
            @Nonnull BinaryOperatorEx<A> combineAccValuesFn,
            @Nullable BinaryOperatorEx<A> deductAccValueFn
    ) {
        checkSerializable(emptyAccValue, "emptyAccValue");
        checkSerializable(toAccValueFn, "toAccValueFn");
        checkSerializable(combineAccValuesFn, "combineAccValuesFn");
        checkSerializable(deductAccValueFn, "deductAccValueFn");

        // workaround for spotbugs issue: https://github.com/spotbugs/spotbugs/issues/552
        BinaryOperatorEx<A> deductFn = deductAccValueFn;
        return AggregateOperation
                .withCreate(() -> new MutableReference<>(emptyAccValue))
                .andAccumulate((MutableReference<A> a, T t) ->
                        a.set(combineAccValuesFn.apply(a.get(), toAccValueFn.apply(t))))
                .andCombine((a, b) -> a.set(combineAccValuesFn.apply(a.get(), b.get())))
                .andDeduct(deductAccValueFn != null
                        ? (a, b) -> a.set(deductFn.apply(a.get(), b.get()))
                        : null)
                .andExportFinish(MutableReference::get);
    }

    /**
     * Returns an aggregate operation whose result is an arbitrary item it
     * observed, or {@code null} if it observed no items.
     * <p>
     * The implementation of {@link StageWithWindow#distinct()} uses this
     * operation and, if needed, you can use it directly for the same purpose.
     * For example, in a stream of Person objects you can specify the last name
     * as the key. The result will be a stream of Person objects, one for each
     * distinct last name:
     * <pre>
     *     Pipeline p = Pipeline.create();
     *     p.&lt;Person>drawFrom(list("persons"))
     *      .groupingKey(Person::getLastName)
     *      .aggregate(pickAny())
     *      .drainTo(...);
     * </pre>
     */
    @Nonnull
    @SuppressWarnings("checkstyle:needbraces")
    public static <T> AggregateOperation1<T, MutableReference<T>, T> pickAny() {
        return AggregateOperation
                .withCreate(MutableReference<T>::new)
                // Result would be correct even without the acc.isNull() check, but that
                // can cause more GC churn due to medium-lived objects.
                .<T>andAccumulate((acc, item) -> {
                    if (acc.isNull()) acc.set(item);
                })
                .andCombine((acc1, acc2) -> {
                    if (acc1.isNull()) acc1.set(acc2.get());
                })
                .andExportFinish(MutableReference::get);
    }

    /**
     * Returns an aggregate operation that accumulates all input items into an
     * {@code ArrayList} and sorts it with the given comparator. Use {@link
     * ComparatorEx#naturalOrder()} if you want to sort {@code
     * Comparable} items by their natural order.
     *
     * @param comparator the comparator to use for sorting
     * @param <T> the type of input items
     */
    public static <T> AggregateOperation1<T, ArrayList<T>, List<T>> sorting(
            @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        return AggregateOperation
                .withCreate(ArrayList<T>::new)
                .<T>andAccumulate(ArrayList::add)
                .andCombine(ArrayList::addAll)
                .andExport(acc -> {
                    // sorting the accumulator doesn't harm - will make the next sort an easier job
                    acc.sort(comparator);
                    return (List<T>) new ArrayList<>(acc);
                })
                .andFinish(list -> {
                    list.sort(comparator);
                    return list;
                });
    }

    /**
     * Returns an aggregate operation that is a composite of two aggregate
     * operations. It allows you to calculate multiple aggregations over the
     * same items at once.
     *
     * @param op0 1st operation
     * @param op1 2nd operation
     * @param exportFinishFn function combining the two results into a single target instance
     *
     * @param <T> type of input items
     * @param <A0> 1st accumulator type
     * @param <A1> 2nd accumulator type
     * @param <R0> 1st result type
     * @param <R1> 2nd result type
     * @param <R> final result type
     *
     * @return the composite operation
     */
    @Nonnull
    public static <T, A0, A1, R0, R1, R> AggregateOperation1<T, Tuple2<A0, A1>, R> allOf(
            @Nonnull AggregateOperation1<? super T, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T, A1, ? extends R1> op1,
            @Nonnull BiFunctionEx<? super R0, ? super R1, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple2(op0.createFn().get(), op1.createFn().get()))
                .<T>andAccumulate((acc, item) -> {
                    op0.accumulateFn().accept(acc.f0(), item);
                    op1.accumulateFn().accept(acc.f1(), item);
                })
                .andCombine(combine0 == null || combine1 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                        })
                .andDeduct(deduct0 == null || deduct1 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                        })
                .<R>andExport(acc ->
                        exportFinishFn.apply(op0.exportFn().apply(acc.f0()), op1.exportFn().apply(acc.f1())))
                .andFinish(acc ->
                        exportFinishFn.apply(op0.finishFn().apply(acc.f0()), op1.finishFn().apply(acc.f1())));
    }

    /**
     * Convenience for {@link #allOf(AggregateOperation1, AggregateOperation1,
     * BiFunctionEx)} with identity finish.
     */
    @Nonnull
    public static <T, A0, A1, R0, R1> AggregateOperation1<T, Tuple2<A0, A1>, Tuple2<R0, R1>> allOf(
            @Nonnull AggregateOperation1<? super T, A0, R0> op1,
            @Nonnull AggregateOperation1<? super T, A1, R1> op2
    ) {
        return allOf(op1, op2, Tuple2::tuple2);
    }

    /**
     * Returns an aggregate operation that is a composite of three aggregate
     * operations. It allows you to calculate multiple aggregations over the
     * same items at once.
     *
     * @param op0 1st operation
     * @param op1 2nd operation
     * @param op2 3rd operation
     * @param exportFinishFn function combining the three results into a single target instance
     *
     * @param <T> type of input items
     * @param <A0> 1st accumulator type
     * @param <A1> 2nd accumulator type
     * @param <A2> 3rd accumulator type
     * @param <R0> 1st result type
     * @param <R1> 2nd result type
     * @param <R2> 3rd result type
     * @param <R> final result type
     *
     * @return the composite operation
     */
    @Nonnull
    public static <T, A0, A1, A2, R0, R1, R2, R> AggregateOperation1<T, Tuple3<A0, A1, A2>, R> allOf(
            @Nonnull AggregateOperation1<? super T, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, ? extends R2> op2,
            @Nonnull TriFunction<? super R0, ? super R1, ? super R2, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A2, ? super A2> combine2 = op2.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        BiConsumerEx<? super A2, ? super A2> deduct2 = op2.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple3(op0.createFn().get(), op1.createFn().get(), op2.createFn().get()))
                .<T>andAccumulate((acc, item) -> {
                    op0.accumulateFn().accept(acc.f0(), item);
                    op1.accumulateFn().accept(acc.f1(), item);
                    op2.accumulateFn().accept(acc.f2(), item);
                })
                .andCombine(combine0 == null || combine1 == null || combine2 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                            combine2.accept(acc1.f2(), acc2.f2());
                        })
                .andDeduct(deduct0 == null || deduct1 == null || deduct2 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                            deduct2.accept(acc1.f2(), acc2.f2());
                        })
                .<R>andExport(acc -> exportFinishFn.apply(
                        op0.exportFn().apply(acc.f0()),
                        op1.exportFn().apply(acc.f1()),
                        op2.exportFn().apply(acc.f2())))
                .andFinish(acc -> exportFinishFn.apply(
                        op0.finishFn().apply(acc.f0()),
                        op1.finishFn().apply(acc.f1()),
                        op2.finishFn().apply(acc.f2())));
    }

    /**
     * Convenience for {@link #allOf(AggregateOperation1, AggregateOperation1,
     * AggregateOperation1, TriFunction)} with identity finisher.
     */
    @Nonnull
    public static <T, A0, A1, A2, R0, R1, R2>
    AggregateOperation1<T, Tuple3<A0, A1, A2>, Tuple3<R0, R1, R2>>
    allOf(
            @Nonnull AggregateOperation1<? super T, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, ? extends R2> op2
    ) {
        return allOf(op0, op1, op2, Tuple3::tuple3);
    }

    /**
     * Returns a builder object that helps you create a composite of multiple
     * aggregate operations. The resulting aggregate operation will perform all
     * of the constituent operations at the same time and you can retrieve each
     * result from the {@link com.hazelcast.jet.datamodel.ItemsByTag} object
     * you'll get in the output.
     * <p>
     * The builder object is primarily intended to build a composite of four or more
     * aggregate operations. For up to three operations, prefer the explicit, more
     * type-safe variants {@link #allOf(AggregateOperation1, AggregateOperation1) allOf(op1, op2)}
     * and {@link #allOf(AggregateOperation1, AggregateOperation1,
     * AggregateOperation1) allOf(op1, op2, op3)}.
     * <p>
     * Example that calculates the count and the sum of the items:
     * <pre>{@code
     * AllOfAggregationBuilder<Long> builder = allOfBuilder();
     * Tag<Long> tagSum = builder.add(summingLong(Long::longValue));
     * Tag<Long> tagCount = builder.add(counting());
     * AggregateOperation1<Long, ?, ItemsByTag> compositeAggrOp = builder.build();
     * }</pre>
     *
     * When you receive the resulting {@link com.hazelcast.jet.datamodel.ItemsByTag
     * ItemsByTag}, fetch the individual results using the tags as keys, for example:
     * <pre>{@code
     * batchStage.aggregate(compositeAggrOp).map((ItemsByTag result) -> {
     *     Long sum = result.get(tagSum);
     *     Long count = result.get(tagCount);
     *     ...
     * });
     * }</pre>
     *
     * @param <T> type of input items
     */
    @Nonnull
    public static <T> AllOfAggregationBuilder<T> allOfBuilder() {
        return new AllOfAggregationBuilder<>();
    }

    /**
     * Returns an aggregate operation that is a composite of two independent
     * aggregate operations, each one accepting its own input. You need this
     * kind of operation for a two-way co-aggregating pipeline stage:
     * {@link com.hazelcast.jet.pipeline.StageWithWindow#aggregate2
     * stage.aggregate2()}.
     * <p>
     * This method is suitable when you can express your computation as two
     * independent aggregate operations where you combine only their final
     * results. If you need an operation that combines the two inputs in the
     * accumulation phase, you can create an aggregate operation by specifying
     * each primitive using the {@linkplain AggregateOperation#withCreate
     * aggregate operation builder}.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param exportFinishFn the function that transforms the individual aggregate results into the
     *                 overall result that the co-aggregating stage emits
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     * @param <R> type of the result
     */
    public static <T0, A0, R0, T1, A1, R1, R> AggregateOperation2<T0, T1, Tuple2<A0, A1>, R> aggregateOperation2(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1,
            @Nonnull BiFunctionEx<? super R0, ? super R1, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple2(op0.createFn().get(), op1.createFn().get()))
                .<T0>andAccumulate0((acc, item) -> op0.accumulateFn().accept(acc.f0(), item))
                .<T1>andAccumulate1((acc, item) -> op1.accumulateFn().accept(acc.f1(), item))
                .andCombine(combine0 == null || combine1 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                        })
                .andDeduct(deduct0 == null || deduct1 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                        })
                .<R>andExport(acc ->
                        exportFinishFn.apply(op0.exportFn().apply(acc.f0()), op1.exportFn().apply(acc.f1())))
                .andFinish(acc ->
                        exportFinishFn.apply(op0.finishFn().apply(acc.f0()), op1.finishFn().apply(acc.f1())));
    }

    /**
     * Convenience for {@link #aggregateOperation2(AggregateOperation1,
     *      AggregateOperation1, BiFunctionEx)
     * aggregateOperation2(aggrOp0, aggrOp1, finishFn)} that outputs a
     * {@code Tuple2(result0, result1)}.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     */
    public static <T0, T1, A0, A1, R0, R1>
    AggregateOperation2<T0, T1, Tuple2<A0, A1>, Tuple2<R0, R1>>
    aggregateOperation2(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1
    ) {
        return aggregateOperation2(op0, op1, Tuple2::tuple2);
    }

    /**
     * Returns an aggregate operation that is a composite of three independent
     * aggregate operations, each one accepting its own input. You need this
     * kind of operation for a three-way co-aggregating pipeline stage:
     * {@link com.hazelcast.jet.pipeline.StageWithWindow#aggregate3
     * stage.aggregate3()}.
     * <p>
     * This method is suitable when you can express your computation as three
     * independent aggregate operations where you combine only their final
     * results. If you need an operation that combines the three inputs in the
     * accumulation phase, you can create an aggregate operation by specifying
     * each primitive using the {@linkplain AggregateOperation#withCreate
     * aggregate operation builder}.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param op2 the aggregate operation that will receive the third stage's input
     * @param exportFinishFn the function that transforms the individual aggregate results into the
     *                 overall result that the co-aggregating stage emits
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     * @param <T2> type of items in the third stage
     * @param <A2> type of the third aggregate operation's accumulator
     * @param <R2> type of the third aggregate operation's result
     * @param <R> type of the result
     */
    public static <T0, T1, T2, A0, A1, A2, R0, R1, R2, R>
    AggregateOperation3<T0, T1, T2, Tuple3<A0, A1, A2>, R> aggregateOperation3(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T2, A2, ? extends R2> op2,
            @Nonnull TriFunction<? super R0, ? super R1, ? super R2, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A2, ? super A2> combine2 = op2.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        BiConsumerEx<? super A2, ? super A2> deduct2 = op2.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple3(op0.createFn().get(), op1.createFn().get(), op2.createFn().get()))
                .<T0>andAccumulate0((acc, item) -> op0.accumulateFn().accept(acc.f0(), item))
                .<T1>andAccumulate1((acc, item) -> op1.accumulateFn().accept(acc.f1(), item))
                .<T2>andAccumulate2((acc, item) -> op2.accumulateFn().accept(acc.f2(), item))
                .andCombine(combine0 == null || combine1 == null || combine2 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                            combine2.accept(acc1.f2(), acc2.f2());
                        })
                .andDeduct(deduct0 == null || deduct1 == null || deduct2 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                            deduct2.accept(acc1.f2(), acc2.f2());
                        })
                .<R>andExport(acc -> exportFinishFn.apply(
                        op0.exportFn().apply(acc.f0()),
                        op1.exportFn().apply(acc.f1()),
                        op2.exportFn().apply(acc.f2())))
                .andFinish(acc -> exportFinishFn.apply(
                        op0.finishFn().apply(acc.f0()),
                        op1.finishFn().apply(acc.f1()),
                        op2.finishFn().apply(acc.f2())));
    }

    /**
     * Convenience for {@link #aggregateOperation3(AggregateOperation1, AggregateOperation1,
     *      AggregateOperation1, TriFunction)
     * aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, finishFn)} that outputs a
     * {@code Tuple3(result0, result1, result2)}.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param op2 the aggregate operation that will receive the third stage's input
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     * @param <T2> type of items in the third stage
     * @param <A2> type of the third aggregate operation's accumulator
     * @param <R2> type of the third aggregate operation's result
     */
    public static <T0, T1, T2, A0, A1, A2, R0, R1, R2>
    AggregateOperation3<T0, T1, T2, Tuple3<A0, A1, A2>, Tuple3<R0, R1, R2>>
    aggregateOperation3(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T2, A2, ? extends R2> op2
    ) {
        return aggregateOperation3(op0, op1, op2, Tuple3::tuple3);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to create
     * an aggregate operation that accepts multiple inputs. You must supply
     * this kind of operation to a co-aggregating pipeline stage. Most typically
     * you'll need this builder if you're using the {@link
     * com.hazelcast.jet.pipeline.StageWithWindow#aggregateBuilder()}. For
     * two-way or three-way co-aggregation you can use {@link
     * AggregateOperations#aggregateOperation2} and {@link AggregateOperations#aggregateOperation3}.
     * <p>
     * This builder is suitable when you can express your computation as
     * independent aggregate operations on each input where you combine only
     * their final results. If you need an operation that combines the inputs
     * in the accumulation phase, you can create an aggregate operation by
     * specifying each primitive using the {@linkplain AggregateOperation#withCreate
     * aggregate operation builder}.
     */
    @Nonnull
    public static CoAggregateOperationBuilder coAggregateOperationBuilder() {
        return new CoAggregateOperationBuilder();
    }
}
