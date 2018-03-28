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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.ICacheJet;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
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
import com.hazelcast.jet.stream.impl.reducers.GroupingSinkReducer;
import com.hazelcast.jet.stream.impl.reducers.IListReducer;
import com.hazelcast.jet.stream.impl.reducers.MergingSinkReducer;
import com.hazelcast.jet.stream.impl.reducers.SinkReducer;

import java.util.Collection;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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

import static com.hazelcast.jet.core.processor.SinkProcessors.writeCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;

/**
 * {@code Serializable} variants of {@link Collectors
 * java.util.stream.Collectors}.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class DistributedCollectors {

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toCollection(Supplier)
     * java.util.stream.Collectors#toCollection(Supplier)}
     */
    public static <T, C extends Collection<T>> DistributedCollector<T, ?, C> toCollection(
            DistributedSupplier<C> collectionSupplier
    ) {
        return aggregating(AggregateOperations.toCollection(collectionSupplier));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toList()
     * java.util.stream.Collectors#toList()}
     */
    public static <T> DistributedCollector<T, ?, List<T>> toList() {
        return aggregating(AggregateOperations.toList());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#toSet()
     * java.util.stream.Collectors#toSet()}
     */
    public static <T> DistributedCollector<T, ?, Set<T>> toSet() {
        return aggregating(AggregateOperations.toSet());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#joining()
     * java.util.stream.Collectors#joining()}
     */
    public static DistributedCollector<CharSequence, ?, String> joining() {
        return aggregating(AggregateOperations.concatenating());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#joining(CharSequence)
     * java.util.stream.Collectors#joining(CharSequence)}
     */
    public static DistributedCollector<CharSequence, ?, String> joining(CharSequence delimiter) {
        return aggregating(AggregateOperations.concatenating(delimiter));
    }


    /**
     * {@code Serializable} variant of {@link
     * Collectors#joining(CharSequence, CharSequence, CharSequence)
     * java.util.stream.Collectors#joining(CharSequence, CharSequence, CharSequence)}
     */
    public static DistributedCollector<CharSequence, ?, String> joining(
            CharSequence delimiter, CharSequence prefix, CharSequence suffix
    ) {
        return aggregating(AggregateOperations.concatenating(delimiter, prefix, suffix));
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
        return new DistributedCollectorImpl<>(downstream.supplier(), (acc, item) ->
                downstreamAccumulator.accept(acc, mapper.apply(item)),
                downstream.combiner(), downstream.finisher());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#collectingAndThen(Collector, Function)
     * java.util.stream.Collectors#collectingAndThen(Collector, Function)
     */
    public static <T, A, I, R> DistributedCollector<T, A, R> collectingAndThen(
            DistributedCollector<T, A, I> downstream,
            DistributedFunction<I, R> finisher
    ) {
        return new DistributedCollectorImpl<>(
                downstream.supplier(),
                downstream.accumulator(),
                downstream.combiner(),
                downstream.finisher().andThen(finisher)
        );
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#counting()
     * java.util.stream.Collectors#counting()}
     */
    public static <T> DistributedCollector<T, ?, Long> counting() {
        return aggregating(AggregateOperations.counting());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#minBy(Comparator)
     * java.util.stream.Collectors#minBy(Comparator)}
     */
    public static <T> DistributedCollector<T, ?, Optional<T>> minBy(
            DistributedComparator<? super T> comparator
    ) {
        return reducing(DistributedBinaryOperator.minBy(comparator));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#maxBy(Comparator)
     * java.util.stream.Collectors#maxBy(Comparator)}
     */
    public static <T> DistributedCollector<T, ?, Optional<T>> maxBy(
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
                LongAccumulator::new,
                (a, t) -> a.addAllowingOverflow(mapper.applyAsInt(t)),
                LongAccumulator::addAllowingOverflow,
                a -> Math.toIntExact(a.get()));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summingLong(ToLongFunction)
     * java.util.stream.Collectors#summingLong(ToLongFunction)}
     */
    public static <T> DistributedCollector<T, ?, Long> summingLong(DistributedToLongFunction<? super T> mapper) {
        return aggregating(AggregateOperations.summingLong(mapper));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#summingDouble(ToDoubleFunction)
     * java.util.stream.Collectors#summingDouble(ToDoubleFunction)}
     */
    public static <T> DistributedCollector<T, ?, Double> summingDouble(DistributedToDoubleFunction<? super T> mapper) {
        return aggregating(AggregateOperations.summingDouble(mapper));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#averagingInt(ToIntFunction)
     * java.util.stream.Collectors#averagingInt(ToIntFunction)}
     */
    public static <T> DistributedCollector<T, ?, Double> averagingInt(DistributedToIntFunction<? super T> mapper) {
        return aggregating(AggregateOperations.averagingLong(mapper::applyAsInt));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#averagingLong(ToLongFunction)
     * java.util.stream.Collectors#averagingLong(ToLongFunction)}
     */
    public static <T> DistributedCollector<T, ?, Double> averagingLong(DistributedToLongFunction<? super T> mapper) {
        return aggregating(AggregateOperations.averagingLong(mapper));
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
        return aggregating(AggregateOperations.averagingDouble(mapper));
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#reducing(Object, BinaryOperator)}
     * java.util.stream.Collectors#reducing(Object, BinaryOperator)}
     */
    public static <T> DistributedCollector<T, ?, T> reducing(T identity, DistributedBinaryOperator<T> op) {
        return new DistributedCollectorImpl<>(
                refSupplier(identity),
                (ref, t) -> ref.set(op.apply(ref.get(), t)),
                (l, r) -> {
                    l.set(op.apply(l.get(), r.get()));
                    return l;
                },
                MutableReference::get);
    }

    /**
     * {@code Serializable} variant of {@link
     * Collectors#reducing(BinaryOperator)
     * java.util.stream.Collectors#reducing(BinaryOperator)}
     */
    public static <T> DistributedCollector<T, ?, Optional<T>> reducing(
            DistributedBinaryOperator<T> op
    ) {
        return new DistributedCollectorImpl<>(
                (DistributedSupplier<MutableReference<T>>) MutableReference::new,
                (ref, t) -> {
                    T lt = ref.get();
                    if (lt == null) {
                        ref.set(t);
                    } else {
                        ref.set(op.apply(lt, t));
                    }
                },
                (l, r) -> {
                    T lt = l.get();
                    T rt = r.get();
                    if (lt == null) {
                        l.set(rt);
                    } else if (rt != null) {
                        l.set(op.apply(lt, rt));
                    }
                    return l;
                },
                a -> Optional.ofNullable(a.get()));
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
                refSupplier(identity),
                (a, t) -> a.set(op.apply(a.get(), mapper.apply(t))),
                (a, b) -> {
                    a.set(op.apply(a.get(), b.get()));
                    return a;
                },
                MutableReference::get);
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
    public static <T, K, R, A, M extends Map<K, R>> DistributedCollector<T, ?, M> groupingBy(
            DistributedFunction<? super T, ? extends K> classifier,
            DistributedSupplier<M> mapFactory,
            DistributedCollector<? super T, A, R> downstream
    ) {
        return aggregating(AggregateOperations.groupingBy(classifier, mapFactory, toAggregateOp(downstream)));
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
        return groupingBy(predicate::test, downstream);
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
        return aggregating(AggregateOperations.toMap(keyMapper, valueMapper));
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
        return aggregating(AggregateOperations.toMap(keyMapper, valueMapper, mergeFunction));
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
        return aggregating(AggregateOperations.toMap(keyMapper, valueMapper, mergeFunction, mapSupplier));
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
                (a, t) -> a.accept(mapper.applyAsInt(t)),
                (s1, s2) -> {
                    s1.combine(s2);
                    return s1;
                });
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
                (a, t) -> a.accept(mapper.applyAsLong(t)),
                (s1, s2) -> {
                    s1.combine(s2);
                    return s1;
                });
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
                (a, t) -> a.accept(mapper.applyAsDouble(t)),
                (s1, s2) -> {
                    s1.combine(s2);
                    return s1;
                });
    }


    /**
     * Returns a collector which performs a distributed aggregation on all the
     * input values using the given {@link AggregateOperation1}.
     */
    public static <T, A, R> DistributedCollector<T, A, R> aggregating(AggregateOperation1<T, A, R> aggregateOp) {
        return new DistributedCollectorImpl<>(
                aggregateOp.createFn(),
                (acc, t) -> aggregateOp.accumulateFn().accept(acc, t),
                (l, r) -> {
                    aggregateOp.combineFn().accept(l, r);
                    return l;
                },
                a -> aggregateOp.finishFn().apply(a));
    }


    // adapter for converting Collector to AggregateOp
    private static <T, A, R> AggregateOperation1<? super T, A, R> toAggregateOp(
            DistributedCollector<? super T, A, R> collector
    ) {
        return AggregateOperation.withCreate(collector.supplier())
                                 .andAccumulate(collector.accumulator())
                                 .andCombine((l, r) -> collector.combiner().apply(l, r))
                                 .andFinish(collector.finisher());
    }

    private static <T> DistributedSupplier<MutableReference<T>> refSupplier(T obj) {
        return () -> new MutableReference<>(obj);
    }

    //                 JET-SPECIFIC REDUCERS

    /**
     * Returns a collector similar to {@link Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function,Function)} which, instead of
     * creating a regular Java map, writes the results to the distributed
     * {@link IMapJet} with the given name. The stream expression will return the
     * {@code IMapJet} in question.
     */
    public static <T, K, U> Reducer<T, IMapJet<K, U>> toIMap(
            String mapName,
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper
    ) {
        return new SinkReducer<>("write-map-" + mapName, jetInstance -> jetInstance.getMap(mapName),
                keyMapper, valueMapper, writeMapP(mapName));
    }

    /**
     * Returns a collector similar to {@link Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function,Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link IMapJet} with the given name.
     * <p>
     * This collector expects to receive {@code Map.Entry}s and it will put them
     * as key/value pairs to the target map.
     */
    public static <K, U> Reducer<Entry<K, U>, IMapJet<K, U>> toIMap(String mapName) {
        return toIMap(mapName, Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Variant of {@link Collectors#toMap(Function, Function, BinaryOperator)
     * java.util.stream.Collectors#toMap(Function, Function, BinaryOperator)}
     * which, instead of returning a map as the result, writes the results
     * to the distributed {@link IMapJet} with the given name.
     */
    public static <T, K, U> Reducer<T, IMapJet<K, U>> toIMap(
            String mapName, DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper,
            DistributedBinaryOperator<U> mergeFunction
    ) {
        return new MergingSinkReducer<>("write-map-" + mapName, jetInstance -> jetInstance.getMap(mapName),
                keyMapper, valueMapper, mergeFunction, writeMapP(mapName));
    }

    /**
     * Variant of {@link Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link ICacheJet} with the given name.
     */
    public static <T, K, U> Reducer<T, ICacheJet<K, U>> toICache(
            String cacheName,
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper
    ) {
        return new SinkReducer<>("write-cache-" + cacheName, CacheGetter.getCacheFn(cacheName),
                keyMapper, valueMapper, writeCacheP(cacheName));
    }

    /**
     * Variant of {@link Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link ICacheJet} with the given name.
     */
    public static <K, U> Reducer<Map.Entry<K, U>, ICacheJet<K, U>> toICache(String cacheName) {
        return toICache(cacheName, Entry::getKey, Entry::getValue);
    }

    /**
     * Variant of {@link Collectors#toMap(Function, Function)
     * java.util.stream.Collectors#toMap(Function, Function)} which, instead of
     * returning a map as the result, writes the results to the distributed
     * {@link ICacheJet} with the given name.
     */
    public static <T, K, U> Reducer<T, ICacheJet<K, U>> toICache(
            String cacheName,
            DistributedFunction<? super T, ? extends K> keyMapper,
            DistributedFunction<? super T, ? extends U> valueMapper,
            DistributedBinaryOperator<U> mergeFunction
    ) {
        return new MergingSinkReducer<>("write-cache-" + cacheName, CacheGetter.getCacheFn(cacheName),
                keyMapper, valueMapper, mergeFunction, writeCacheP(cacheName));
    }

    /**
     * Variant of {@link Collectors#toList() java.util.stream.Collectors#toList()}
     * which, instead of returning a list as the result, writes the results to the
     * distributed {@link IListJet} with the given name.
     */
    public static <T> Reducer<T, IListJet<T>> toIList(String listName) {
        return new IListReducer<>(listName);
    }

    /**
     * Variant of {@link Collectors#groupingBy(Function)
     * java.util.stream.Collectors#groupingBy(Function)} which, instead of
     * returning a map as the result, writes the grouped results to the
     * distributed {@link IMapJet} with the given name.
     */
    public static <T, K> Reducer<T, IMapJet<K, List<T>>> groupingByToIMap(
            String mapName, DistributedFunction<? super T, ? extends K> classifier
    ) {
        return groupingByToIMap(mapName, classifier, toList());
    }

    /**
     * Variant of {@link Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)} which,
     * instead of returning a map as the result, writes the grouped results to
     * the distributed {@link IMapJet} with the given name. The downstream
     * collector you provide will produce the value to be stored under each
     * grouping key. The value must be serializable.
     */
    public static <T, K, A, D> Reducer<T, IMapJet<K, D>> groupingByToIMap(
            String mapName, DistributedFunction<? super T, ? extends K> classifier,
            DistributedCollector<? super T, A, D> downstream
    ) {
        return new GroupingSinkReducer<>("write-map-" + mapName, jetInstance -> jetInstance.getMap(mapName),
                classifier, downstream, writeMapP(mapName));
    }

    /**
     * Variant of {@link Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)} which, instead of
     * returning a map as the result, writes the grouped results to the distributed
     * {@link ICacheJet} with the given name.
     */
    public static <T, K> Reducer<T, ICacheJet<K, List<T>>> groupingByToICache(
            String cacheName, DistributedFunction<? super T, ? extends K> classifier
    ) {
        return groupingByToICache(cacheName, classifier, toList());
    }

    /**
     * Variant of {@link Collectors#groupingBy(Function, Collector)
     * java.util.stream.Collectors#groupingBy(Function, Collector)} which,
     * instead of returning a map as the result, writes the grouped results to
     * the distributed {@link ICacheJet} with the given name. The downstream
     * collector you provide will produce the value to be stored under each
     * grouping key. The value must be serializable.
     */
    public static <T, K, A, D> Reducer<T, ICacheJet<K, D>> groupingByToICache(
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

        private static <K, V> DistributedFunction<JetInstance, ICacheJet<K, V>> getCacheFn(String name) {
            return instance -> instance.getCacheManager().getCache(name);
        }
    }
}
