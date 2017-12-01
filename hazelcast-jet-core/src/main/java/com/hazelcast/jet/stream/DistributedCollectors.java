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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedOptional;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToDoubleFunction;
import com.hazelcast.jet.function.DistributedToIntFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.distributed.DistributedDoubleSummaryStatistics;
import com.hazelcast.jet.stream.impl.distributed.DistributedIntSummaryStatistics;
import com.hazelcast.jet.stream.impl.distributed.DistributedLongSummaryStatistics;
import com.hazelcast.jet.stream.impl.reducers.DistributedCollectorImpl;
import com.hazelcast.jet.stream.impl.reducers.DistributedStringJoiner;
import com.hazelcast.jet.stream.impl.reducers.GroupingSinkReducer;
import com.hazelcast.jet.stream.impl.reducers.IListReducer;
import com.hazelcast.jet.stream.impl.reducers.MergingSinkReducer;
import com.hazelcast.jet.stream.impl.reducers.SinkReducer;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * {@code Serializable} variants of {@link Collectors
 * java.util.stream.Collectors}.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class DistributedCollectors {

    static final Set<Collector.Characteristics> CH_ID = unmodifiableSet(
            EnumSet.of(Collector.Characteristics.IDENTITY_FINISH)
    );

    static final Set<Collector.Characteristics> CH_NOID = emptySet();

    private static final Set<Collector.Characteristics> CH_UNORDERED_ID = unmodifiableSet(
            EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH)
    );


    @SuppressWarnings("unchecked")
    private static <T> DistributedSupplier<T[]> boxSupplier(T identity) {
        return () -> (T[]) new Object[]{identity};
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toCollection(Supplier)
     * java.util.stream.Collectors#toCollection(Supplier)}
     */
    public static <T, C extends Collection<T>> DistributedCollector<T, ?, C> toCollection(
            DistributedSupplier<C> collectionFactory
    ) {
        return new DistributedCollectorImpl<>(collectionFactory, Collection::add,
                (r1, r2) -> {
                    r1.addAll(r2);
                    return r1;
                },
                CH_ID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toList()
     * java.util.stream.Collectors#toList()}
     */
    public static <T> DistributedCollector<T, ?, List<T>> toList() {
        return new DistributedCollectorImpl<>(ArrayList::new, List::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                CH_ID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toSet()
     * java.util.stream.Collectors#toSet()}
     */
    public static <T> DistributedCollector<T, ?, Set<T>> toSet() {
        return new DistributedCollectorImpl<>((DistributedSupplier<Set<T>>) HashSet::new, Set<T>::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                CH_UNORDERED_ID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#joining()
     * java.util.stream.Collectors#joining()}
     */
    public static DistributedCollector<CharSequence, ?, String> joining() {
        return new DistributedCollectorImpl<>(
                StringBuilder::new, StringBuilder::append,
                (r1, r2) -> {
                    r1.append(r2);
                    return r1;
                },
                StringBuilder::toString, CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#joining(CharSequence)
     * java.util.stream.Collectors#joining(CharSequence)}
     */
    public static DistributedCollector<CharSequence, ?, String> joining(CharSequence delimiter) {
        return joining(delimiter, "", "");
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#joining(CharSequence, CharSequence, CharSequence)
     * java.util.stream.Collectors#joining(CharSequence, CharSequence, CharSequence)}
     */
    public static DistributedCollector<CharSequence, ?, String> joining(
            CharSequence delimiter, CharSequence prefix, CharSequence suffix
    ) {
        return new DistributedCollectorImpl<>(
                () -> new DistributedStringJoiner(delimiter, prefix, suffix),
                DistributedStringJoiner::add, DistributedStringJoiner::merge,
                DistributedStringJoiner::toString, CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#mapping(Function, Collector)
     * java.util.stream.Collectors#mapping(Function, Collector)}
     */
    public static <T, U, A, R> DistributedCollector<T, ?, R> mapping(
            DistributedFunction<? super T, ? extends U> mapper, DistributedCollector<? super U, A, R> downstream
    ) {
        DistributedBiConsumer<A, ? super U> downstreamAccumulator = downstream.accumulator();
        return new DistributedCollectorImpl<>(downstream.supplier(),
                (r, t) -> downstreamAccumulator.accept(r, mapper.apply(t)),
                downstream.combiner(), downstream.finisher(),
                downstream.characteristics());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#collectingAndThen(Collector, Function)
     * java.util.stream.Collectors#collectingAndThen(Collector, Function)
     */
    public static <T, A, R, RR> DistributedCollector<T, A, RR> collectingAndThen(
            DistributedCollector<T, A, R> downstream,
            DistributedFunction<R, RR> finisher
    ) {
        Set<Collector.Characteristics> characteristics = downstream.characteristics();
        if (characteristics.contains(Collector.Characteristics.IDENTITY_FINISH)) {
            if (characteristics.size() == 1) {
                characteristics = DistributedCollectors.CH_NOID;
            } else {
                characteristics = EnumSet.copyOf(characteristics);
                characteristics.remove(Collector.Characteristics.IDENTITY_FINISH);
                characteristics = unmodifiableSet(characteristics);
            }
        }
        return new DistributedCollectorImpl<>(downstream.supplier(),
                downstream.accumulator(),
                downstream.combiner(),
                downstream.finisher().andThen(finisher),
                characteristics);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#counting()
     * java.util.stream.Collectors#counting()}
     */
    public static <T> DistributedCollector<T, ?, Long> counting() {
        return reducing(0L, e -> 1L, Long::sum);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#minBy(Comparator)
     * java.util.stream.Collectors#minBy(Comparator)}
     */
    public static <T> DistributedCollector<T, ?, DistributedOptional<T>> minBy(
            DistributedComparator<? super T> comparator
    ) {
        return reducing(DistributedBinaryOperator.minBy(comparator));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#maxBy(Comparator)
     * java.util.stream.Collectors#maxBy(Comparator)}
     */
    public static <T> DistributedCollector<T, ?, DistributedOptional<T>> maxBy(
            DistributedComparator<? super T> comparator
    ) {
        return reducing(DistributedBinaryOperator.maxBy(comparator));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summingInt(ToIntFunction)
     * java.util.stream.Collectors#summingInt(ToIntFunction)}
     */
    public static <T> DistributedCollector<T, ?, Integer> summingInt(DistributedToIntFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                () -> new int[1],
                (a, t) -> a[0] += mapper.applyAsInt(t),
                (a, b) -> {
                    a[0] += b[0];
                    return a;
                },
                a -> a[0], CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summingLong(ToLongFunction)
     * java.util.stream.Collectors#summingLong(ToLongFunction)}
     */
    public static <T> DistributedCollector<T, ?, Long> summingLong(DistributedToLongFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                () -> new long[1],
                (a, t) -> {
                    a[0] += mapper.applyAsLong(t);
                },
                (a, b) -> {
                    a[0] += b[0];
                    return a;
                },
                a -> a[0], CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summingDouble(ToDoubleFunction)
     * java.util.stream.Collectors#summingDouble(ToDoubleFunction)}
     */
    public static <T> DistributedCollector<T, ?, Double> summingDouble(DistributedToDoubleFunction<? super T> mapper) {
        /*
         * In the arrays allocated for the collect operation, index 0
         * holds the high-order bits of the running sum, index 1 holds
         * the low-order bits of the sum computed via compensated
         * summation, and index 2 holds the simple sum used to compute
         * the proper result if the stream contains infinite values of
         * the same sign.
         */
        return new DistributedCollectorImpl<>(
                () -> new double[3],
                (a, t) -> {
                    sumWithCompensation(a, mapper.applyAsDouble(t));
                    a[2] += mapper.applyAsDouble(t);
                },
                (a, b) -> {
                    sumWithCompensation(a, b[0]);
                    a[2] += b[2];
                    return sumWithCompensation(a, b[1]);
                },
                DistributedCollectors::computeFinalSum,
                CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#averagingInt(ToIntFunction)
     * java.util.stream.Collectors#averagingInt(ToIntFunction)}
     */
    public static <T> DistributedCollector<T, ?, Double> averagingInt(DistributedToIntFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                () -> new long[2],
                (a, t) -> {
                    a[0] += mapper.applyAsInt(t);
                    a[1]++;
                },
                (a, b) -> {
                    a[0] += b[0];
                    a[1] += b[1];
                    return a;
                },
                a -> (a[1] == 0) ? 0.0d : (double) a[0] / a[1], CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#averagingLong(ToLongFunction)
     * java.util.stream.Collectors#averagingLong(ToLongFunction)}
     */
    public static <T> DistributedCollector<T, ?, Double> averagingLong(DistributedToLongFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                () -> new long[2],
                (a, t) -> {
                    a[0] += mapper.applyAsLong(t);
                    a[1]++;
                },
                (a, b) -> {
                    a[0] += b[0];
                    a[1] += b[1];
                    return a;
                },
                a -> (a[1] == 0) ? 0.0d : (double) a[0] / a[1], CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#averagingDouble(ToDoubleFunction)
     * java.util.stream.Collectors#averagingDouble(ToDoubleFunction)}
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static <T> DistributedCollector<T, ?, Double> averagingDouble(
            DistributedToDoubleFunction<? super T> mapper
    ) {
        /*
         * In the arrays allocated for the collect operation, index 0
         * holds the high-order bits of the running sum, index 1 holds
         * the low-order bits of the sum computed via compensated
         * summation, and index 2 holds the number of values seen.
         */
        return new DistributedCollectorImpl<>(
                () -> new double[4],
                (a, t) -> {
                    sumWithCompensation(a, mapper.applyAsDouble(t));
                    a[2]++;
                    a[3] += mapper.applyAsDouble(t);
                },
                (a, b) -> {
                    sumWithCompensation(a, b[0]);
                    sumWithCompensation(a, b[1]);
                    a[2] += b[2];
                    a[3] += b[3];
                    return a;
                },
                a -> (a[2] == 0) ? 0.0d : (computeFinalSum(a) / a[2]),
                CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#reducing(Object, BinaryOperator)}
     * java.util.stream.Collectors#reducing(Object, BinaryOperator)}
     */
    public static <T> DistributedCollector<T, ?, T> reducing(T identity, DistributedBinaryOperator<T> op) {
        return new DistributedCollectorImpl<>(
                boxSupplier(identity),
                (a, t) -> a[0] = op.apply(a[0], t),
                (a, b) -> {
                    a[0] = op.apply(a[0], b[0]);
                    return a;
                },
                a -> a[0],
                CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#reducing(BinaryOperator)
     * java.util.stream.Collectors#reducing(BinaryOperator)}
     */
    public static <T> DistributedCollector<T, ?, DistributedOptional<T>> reducing(
            DistributedBinaryOperator<T> op
    ) {
        class OptionalBox implements DistributedConsumer<T> {
            private T value;
            private boolean present;

            @Override
            public void accept(T t) {
                if (present) {
                    value = op.apply(value, t);
                } else {
                    value = t;
                    present = true;
                }
            }
        }

        return new DistributedCollectorImpl<>(
                OptionalBox::new, OptionalBox::accept,
                (a, b) -> {
                    if (b.present) {
                        a.accept(b.value);
                    }
                    return a;
                },
                a -> DistributedOptional.ofNullable(a.value), CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#reducing(Object, Function, BinaryOperator)
     * java.util.stream.Collectors#reducing(Object, Function, BinaryOperator)}
     */
    public static <T, U> DistributedCollector<T, ?, U> reducing(
            U identity, DistributedFunction<? super T, ? extends U> mapper, DistributedBinaryOperator<U> op
    ) {
        return new DistributedCollectorImpl<>(
                boxSupplier(identity),
                (a, t) -> a[0] = op.apply(a[0], mapper.apply(t)),
                (a, b) -> {
                    a[0] = op.apply(a[0], b[0]);
                    return a;
                },
                a -> a[0], CH_NOID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#groupingBy(Function)
     * java.util.stream.Collectors#groupingBy(Function)}
     */
    public static <T, K> DistributedCollector<T, ?, Map<K, List<T>>> groupingBy(
            DistributedFunction<? super T, ? extends K> classifier
    ) {
        return groupingBy(classifier, toList());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)}
     */
    public static <T, K, A, D> DistributedCollector<T, ?, Map<K, D>> groupingBy(
            DistributedFunction<? super T, ? extends K> classifier,
            DistributedCollector<? super T, A, D> downstream
    ) {
        return groupingBy(classifier, HashMap::new, downstream);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#groupingBy(Function, Supplier, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Supplier, Collector)}
     */
    public static <T, K, D, A, M extends Map<K, D>> DistributedCollector<T, ?, M> groupingBy(
            DistributedFunction<? super T, ? extends K> classifier,
            DistributedSupplier<M> mapFactory,
            DistributedCollector<? super T, A, D> downstream
    ) {
        DistributedSupplier<A> downstreamSupplier = downstream.supplier();
        DistributedBiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        DistributedBiConsumer<Map<K, A>, T> accumulator = (m, t) -> {
            K key = Objects.requireNonNull(classifier.apply(t), "element cannot be mapped to a null key");
            A container = m.computeIfAbsent(key, k -> downstreamSupplier.get());
            downstreamAccumulator.accept(container, t);
        };
        DistributedBinaryOperator<Map<K, A>> merger =
                DistributedCollectors.<K, A, Map<K, A>>mapMerger(downstream.combiner());
        @SuppressWarnings("unchecked")
        DistributedSupplier<Map<K, A>> mangledFactory = (DistributedSupplier<Map<K, A>>) mapFactory;

        if (downstream.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
            return new DistributedCollectorImpl<>(mangledFactory, accumulator, merger, CH_ID);
        } else {
            @SuppressWarnings("unchecked")
            DistributedFunction<A, A> downstreamFinisher = (DistributedFunction<A, A>) downstream.finisher();
            DistributedFunction<Map<K, A>, M> finisher = intermediate -> {
                intermediate.replaceAll((k, v) -> downstreamFinisher.apply(v));
                @SuppressWarnings("unchecked")
                M castResult = (M) intermediate;
                return castResult;
            };
            return new DistributedCollectorImpl<>(mangledFactory, accumulator, merger, finisher, CH_NOID);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#partitioningBy(Predicate)
     * java.util.stream.Collectors#partitioningBy(Predicate)}
     */
    public static <T> Collector<T, ?, Map<Boolean, List<T>>> partitioningBy(
            DistributedPredicate<? super T> predicate
    ) {
        return partitioningBy(predicate, toList());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#partitioningBy(Predicate, Collector)
     * java.util.stream.Collectors#partitioningBy(Predicate, Collector)}
     */
    public static <T, D, A> Collector<T, ?, Map<Boolean, D>> partitioningBy(
            DistributedPredicate<? super T> predicate, DistributedCollector<? super T, A, D> downstream
    ) {
        DistributedBiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        DistributedBiConsumer<Partition<A>, T> accumulator = (result, t) ->
                downstreamAccumulator.accept(predicate.test(t) ? result.forTrue : result.forFalse, t);
        DistributedBinaryOperator<A> op = downstream.combiner();
        DistributedBinaryOperator<Partition<A>> merger = (left, right) ->
                new Partition<>(op.apply(left.forTrue, right.forTrue),
                        op.apply(left.forFalse, right.forFalse));
        DistributedSupplier<Partition<A>> supplier = () ->
                new Partition<>(downstream.supplier().get(),
                        downstream.supplier().get());
        if (downstream.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
            return new DistributedCollectorImpl<>(supplier, accumulator, merger, CH_ID);
        } else {
            DistributedFunction<Partition<A>, Map<Boolean, D>> finisher = par ->
                    new Partition<>(downstream.finisher().apply(par.forTrue),
                            downstream.finisher().apply(par.forFalse));
            return new DistributedCollectorImpl<>(supplier, accumulator, merger, finisher, CH_NOID);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)}
     */
    public static <T, K, U> DistributedCollector<T, ?, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper
    ) {
        return toMap(keyMapper, valueMapper, throwingMerger(), HashMap::new);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toMap(Function, Function, BinaryOperator)
     * java.util.stream.Collectors#toMap(Function, Function, BinaryOperator)}
     */
    public static <T, K, U> Collector<T, ?, Map<K, U>> toMap(
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper,
            DistributedBinaryOperator<U> mergeFunction
    ) {
        return toMap(keyMapper, valueMapper, mergeFunction, HashMap::new);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toMap(Function, Function, BinaryOperator, Supplier)
     * java.util.stream.Collectors#toMap(Function, Function, BinaryOperator, Supplier)}
     */
    public static <T, K, U, M extends Map<K, U>> DistributedCollector<T, ?, M> toMap(
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper,
            DistributedBinaryOperator<U> mergeFunction,
            DistributedSupplier<M> mapSupplier
    ) {
        DistributedBiConsumer<M, T> accumulator
                = (map, element) -> map.merge(keyMapper.apply(element),
                valueMapper.apply(element), mergeFunction);
        return new DistributedCollectorImpl<>(mapSupplier, accumulator, mapMerger(mergeFunction), CH_ID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summarizingInt(ToIntFunction)
     * java.util.stream.Collectors#summarizingInt(ToIntFunction)}
     */
    public static <T> DistributedCollector<T, ?, IntSummaryStatistics> summarizingInt(
            DistributedToIntFunction<? super T> mapper
    ) {
        return new DistributedCollectorImpl<>(
                DistributedIntSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsInt(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summarizingLong(ToLongFunction)
     * java.util.stream.Collectors#summarizingLong(ToLongFunction)}
     */
    public static <T> DistributedCollector<T, ?, LongSummaryStatistics> summarizingLong(
            DistributedToLongFunction<? super T> mapper
    ) {
        return new DistributedCollectorImpl<>(
                DistributedLongSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsLong(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summarizingDouble(ToDoubleFunction)
     * java.util.stream.Collectors#summarizingDouble(ToDoubleFunction)}
     */
    public static <T> DistributedCollector<T, ?, DoubleSummaryStatistics> summarizingDouble(
            DistributedToDoubleFunction<? super T> mapper
    ) {
        return new DistributedCollectorImpl<>(
                DistributedDoubleSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsDouble(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    private static <K, V, M extends Map<K, V>> DistributedBinaryOperator<M> mapMerger(
            DistributedBinaryOperator<V> mergeFunction
    ) {
        return (m1, m2) -> {
            for (Map.Entry<K, V> e : m2.entrySet()) {
                m1.merge(e.getKey(), e.getValue(), mergeFunction);
            }
            return m1;
        };
    }

    private static <T> DistributedBinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    private static double[] sumWithCompensation(double[] intermediateSum, double value) {
        double tmp = value - intermediateSum[1];
        double sum = intermediateSum[0];
        // Little wolf of rounding error
        double velvel = sum + tmp;
        intermediateSum[1] = (velvel - sum) - tmp;
        intermediateSum[0] = velvel;
        return intermediateSum;
    }

    private static double computeFinalSum(double[] summands) {
        // Better error bounds to add both terms as the final sum
        double tmp = summands[0] + summands[1];
        double simpleSum = summands[summands.length - 1];
        if (Double.isNaN(tmp) && Double.isInfinite(simpleSum)) {
            return simpleSum;
        } else {
            return tmp;
        }
    }

    private static final class Partition<T>
            extends AbstractMap<Boolean, T>
            implements Map<Boolean, T>, Serializable {
        final T forTrue;
        final T forFalse;

        Partition(T forTrue, T forFalse) {
            this.forTrue = forTrue;
            this.forFalse = forFalse;
        }

        @Override @Nonnull
        public Set<Map.Entry<Boolean, T>> entrySet() {
            return new AbstractSet<Entry<Boolean, T>>() {
                @Override @Nonnull
                public Iterator<Entry<Boolean, T>> iterator() {
                    Map.Entry<Boolean, T> falseEntry = entry(false, forFalse);
                    Map.Entry<Boolean, T> trueEntry = entry(true, forTrue);
                    return Arrays.asList(falseEntry, trueEntry).iterator();
                }

                @Override
                public int size() {
                    return 2;
                }
            };
        }
    }

    //                 JET-SPECIFIC REDUCERS

    /**
     * Variant of {@link
     * Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function,Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link IStreamMap} with the given name.
     */
    public static <T, K, U> Reducer<T, IStreamMap<K, U>> toIMap(
            String mapName, DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper
    ) {
        return new SinkReducer<>("write-map-" + mapName, jetInstance -> jetInstance.getMap(mapName),
                keyMapper, valueMapper, writeMapP(mapName));
    }

    /**
     * Variant of {@link
     * Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function,Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link IStreamMap} with the given name.
     * <p>
     * The key and value of the will be used as the respective key and value to
     * put to the target {@code IStreamMap}.
     */
    public static <K, U> Reducer<Entry<K, U>, IStreamMap<K, U>> toIMap(String mapName) {
        return toIMap(mapName, Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Variant of {@link
     * Collectors#toMap(Function, Function, BinaryOperator)
     * java.util.stream.Collectors#toMap(Function, Function, BinaryOperator)}
     * which, instead of returning a map as the result, writes the results
     * to the distributed {@link IStreamMap} with the given name.
     */
    public static <T, K, U> Reducer<T, IStreamMap<K, U>> toIMap(
            String mapName, DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper,
            DistributedBinaryOperator<U> mergeFunction
    ) {
        return new MergingSinkReducer<>("write-map-" + mapName, jetInstance -> jetInstance.getMap(mapName),
                keyMapper, valueMapper, mergeFunction, writeMapP(mapName));
    }

    /**
     * Variant of {@link
     * Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link IStreamCache} with the given name.
     */
    public static <T, K, U> Reducer<T, IStreamCache<K, U>> toICache(
            String cacheName,
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper
    ) {
        return new SinkReducer<>("write-cache-" + cacheName, CacheGetter.getCacheFn(cacheName),
                keyMapper, valueMapper, writeCacheP(cacheName));
    }

    /**
     * Variant of {@link
     * Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link IStreamCache} with the given name.
     */
    public static <K, U> Reducer<Map.Entry<K, U>, IStreamCache<K, U>> toICache(String cacheName) {
        return toICache(cacheName, Entry::getKey, Entry::getValue);
    }

    /**
     * Variant of {@link
     * Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link IStreamCache} with the given name.
     */
    public static <T, K, U> Reducer<T, IStreamCache<K, U>> toICache(
            String cacheName,
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper,
            DistributedBinaryOperator<U> mergeFunction
    ) {
        return new MergingSinkReducer<>("write-cache-" + cacheName, CacheGetter.getCacheFn(cacheName),
                keyMapper, valueMapper, mergeFunction, writeCacheP(cacheName));
    }

    /**
     * Variant of {@link
     * Collectors#toList()
     * java.util.stream.Collectors#toList()} which, instead of returning a list
     * as the result, writes the results to the distributed {@link
     * IStreamList} with the given name.
     */
    public static <T> Reducer<T, IStreamList<T>> toIList(String listName) {
        return new IListReducer<>(listName);
    }

    /**
     * Variant of {@link
     * Collectors#groupingBy(Function)
     * java.util.stream.Collectors#groupingBy(Function)} which, instead of
     * returning a map as the result, writes the grouped results to the
     * distributed {@link IStreamMap} with the given name.
     */
    public static <T, K> Reducer<T, IStreamMap<K, List<T>>> groupingByToIMap(
            String mapName, DistributedFunction<? super T, ? extends K> classifier
    ) {
        return groupingByToIMap(mapName, classifier, toList());
    }

    /**
     * Variant of {@link
     * Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)} which, instead of
     * returning a map as the result, writes the grouped results to the distributed
     * {@link IStreamMap} with the given name.
     */
    public static <T, K, A, D> Reducer<T, IStreamMap<K, D>> groupingByToIMap(
            String mapName, DistributedFunction<? super T, ? extends K> classifier,
            DistributedCollector<? super T, A, D> downstream
    ) {
        return new GroupingSinkReducer<>("write-map-" + mapName, jetInstance -> jetInstance.getMap(mapName),
                classifier, downstream, writeMapP(mapName));
    }

    /**
     * Variant of {@link
     * Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)} which, instead of
     * returning a map as the result, writes the grouped results to the distributed
     * {@link IStreamCache} with the given name.
     */
    public static <T, K> Reducer<T, IStreamCache<K, List<T>>> groupingByToICache(
            String cacheName, DistributedFunction<? super T, ? extends K> classifier
    ) {
        return groupingByToICache(cacheName, classifier, toList());
    }

    /**
     * Variant of {@link
     * Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)} which, instead of
     * returning a map as the result, writes the grouped results to the distributed
     * {@link IStreamCache} with the given name.
     */
    public static <T, K, A, D> Reducer<T, IStreamCache<K, D>> groupingByToICache(
            String cacheName, DistributedFunction<? super T, ? extends K> classifier,
            DistributedCollector<? super T, A, D> downstream
    ) {
        return new GroupingSinkReducer<>("write-cache-" + cacheName, CacheGetter.getCacheFn(cacheName),
                classifier, downstream, writeCacheP(cacheName));
    }

    /**
     * A class that allows the {@code DistributedCollectors} class to be
     * loaded without immediately requiring the JCache types (which may not be
     * on the classpath).
     */
    private static class CacheGetter {

        private static <K, V> DistributedFunction<JetInstance, IStreamCache<K, V>> getCacheFn(String name) {
            return instance -> instance.getCacheManager().getCache(name);
        }
    }
}
