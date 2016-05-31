/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.impl.collectors.DistributedCollectorImpl;
import com.hazelcast.jet.stream.impl.collectors.DistributedStringJoiner;
import com.hazelcast.jet.stream.impl.collectors.HazelcastGroupingMapCollector;
import com.hazelcast.jet.stream.impl.collectors.HazelcastListCollector;
import com.hazelcast.jet.stream.impl.collectors.HazelcastMapCollector;
import com.hazelcast.jet.stream.impl.collectors.HazelcastMergingMapCollector;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collector;

@SuppressWarnings("checkstyle:methodcount")
public abstract class DistributedCollectors {

    static final Set<Collector.Characteristics> CH_ID
            = Collections.unmodifiableSet(EnumSet.of(java.util.stream.Collector.Characteristics.IDENTITY_FINISH));

    static final Set<Collector.Characteristics> CH_UNORDERED_ID
            = Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.UNORDERED,
            Collector.Characteristics.IDENTITY_FINISH));

    static final Set<Collector.Characteristics> CH_NOID = Collections.emptySet();


    @SuppressWarnings("unchecked")
    static <T> Distributed.Supplier<T[]> boxSupplier(T identity) {
        return () -> (T[]) new Object[]{identity};
    }

    public static <T, C extends Collection<T>>
    Collector<T, ?, C> toCollection(Distributed.Supplier<C> collectionFactory) {
        return new DistributedCollectorImpl<>(collectionFactory, Collection::add,
                (r1, r2) -> {
                    r1.addAll(r2);
                    return r1;
                },
                CH_ID);
    }

    public static <T> Distributed.Collector<T, ?, List<T>> toList() {
        return new DistributedCollectorImpl<>(ArrayList::new, List::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                CH_ID);
    }

    public static <T>
    Distributed.Collector<T, ?, Set<T>> toSet() {
        return new DistributedCollectorImpl<>((Distributed.Supplier<Set<T>>) HashSet::new, Set<T>::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                CH_UNORDERED_ID);
    }

    public static Distributed.Collector<CharSequence, ?, String> joining() {
        return new DistributedCollectorImpl<>(
                StringBuilder::new, StringBuilder::append,
                (r1, r2) -> {
                    r1.append(r2);
                    return r1;
                },
                StringBuilder::toString, CH_NOID);
    }

    public static Distributed.Collector<CharSequence, ?, String> joining(CharSequence delimiter) {
        return joining(delimiter, "", "");
    }

    public static Distributed.Collector<CharSequence, ?, String> joining(CharSequence delimiter,
                                                                         CharSequence prefix,
                                                                         CharSequence suffix) {
        return new DistributedCollectorImpl<>(
                () -> new DistributedStringJoiner(delimiter, prefix, suffix),
                DistributedStringJoiner::add, DistributedStringJoiner::merge,
                DistributedStringJoiner::toString, CH_NOID);
    }

    public static <T, U, A, R>
    Distributed.Collector<T, ?, R> mapping(Distributed.Function<? super T, ? extends U> mapper,
                                           Distributed.Collector<? super U, A, R> downstream) {
        BiConsumer<A, ? super U> downstreamAccumulator = downstream.accumulator();
        return new DistributedCollectorImpl<>(downstream.supplier(),
                (r, t) -> downstreamAccumulator.accept(r, mapper.apply(t)),
                downstream.combiner(), downstream.finisher(),
                downstream.characteristics());
    }

    public static <T, A, R, RR> Distributed.Collector<T, A, RR> collectingAndThen(Distributed.Collector<T, A, R> downstream,
                                                                                  Distributed.Function<R, RR> finisher) {
        Set<Collector.Characteristics> characteristics = downstream.characteristics();
        if (characteristics.contains(Collector.Characteristics.IDENTITY_FINISH)) {
            if (characteristics.size() == 1) {
                characteristics = DistributedCollectors.CH_NOID;
            } else {
                characteristics = EnumSet.copyOf(characteristics);
                characteristics.remove(Collector.Characteristics.IDENTITY_FINISH);
                characteristics = Collections.unmodifiableSet(characteristics);
            }
        }
        return new DistributedCollectorImpl<>(downstream.supplier(),
                downstream.accumulator(),
                downstream.combiner(),
                downstream.finisher().andThen(finisher),
                characteristics);
    }

    public static <T> Distributed.Collector<T, ?, Long> counting() {
        return reducing(0L, e -> 1L, Long::sum);
    }

    public static <T> Distributed.Collector<T, ?, Distributed.Optional<T>>
    minBy(Distributed.Comparator<? super T> comparator) {
        return reducing(Distributed.BinaryOperator.minBy(comparator));
    }

    public static <T> Distributed.Collector<T, ?, Distributed.Optional<T>>
    maxBy(Distributed.Comparator<? super T> comparator) {
        return reducing(Distributed.BinaryOperator.maxBy(comparator));
    }

    public static <T> Distributed.Collector<T, ?, Integer>
    summingInt(Distributed.ToIntFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                () -> new int[1],
                (a, t) -> {
                    a[0] += mapper.applyAsInt(t);
                },
                (a, b) -> {
                    a[0] += b[0];
                    return a;
                },
                a -> a[0], CH_NOID);
    }

    public static <T> Distributed.Collector<T, ?, Long>
    summingLong(Distributed.ToLongFunction<? super T> mapper) {
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

    public static <T> Distributed.Collector<T, ?, Double>
    summingDouble(Distributed.ToDoubleFunction<? super T> mapper) {
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
                a -> computeFinalSum(a),
                CH_NOID);
    }

    public static <T> Distributed.Collector<T, ?, Double>
    averagingInt(Distributed.ToIntFunction<? super T> mapper) {
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

    public static <T> Distributed.Collector<T, ?, Double>
    averagingLong(Distributed.ToLongFunction<? super T> mapper) {
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

    @SuppressWarnings("checkstyle:magicnumber")
    public static <T> Distributed.Collector<T, ?, Double>
    averagingDouble(Distributed.ToDoubleFunction<? super T> mapper) {
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

    public static <T> Distributed.Collector<T, ?, T> reducing(T identity, Distributed.BinaryOperator<T> op) {
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

    public static <T> Distributed.Collector<T, ?, Distributed.Optional<T>>
    reducing(Distributed.BinaryOperator<T> op) {
        class OptionalBox implements Distributed.Consumer<T> {
            T value;
            boolean present;

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
                a -> Distributed.Optional.ofNullable(a.value), CH_NOID);
    }

    public static <T, U> Distributed.Collector<T, ?, U> reducing(U identity,
                                                                 Distributed.Function<? super T, ? extends U> mapper,
                                                                 Distributed.BinaryOperator<U> op) {
        return new DistributedCollectorImpl<>(
                boxSupplier(identity),
                (a, t) -> a[0] = op.apply(a[0], mapper.apply(t)),
                (a, b) -> {
                    a[0] = op.apply(a[0], b[0]);
                    return a;
                },
                a -> a[0], CH_NOID);
    }

    public static <T, K> Distributed.Collector<T, ?, Map<K, List<T>>>
    groupingBy(Distributed.Function<? super T, ? extends K> classifier) {
        return groupingBy(classifier, toList());
    }

    public static <T, K, A, D>
    Distributed.Collector<T, ?, Map<K, D>> groupingBy(Distributed.Function<? super T, ? extends K> classifier,
                                                      Distributed.Collector<? super T, A, D> downstream) {
        return groupingBy(classifier, HashMap::new, downstream);
    }

    public static <T, K, D, A, M extends Map<K, D>>
    Distributed.Collector<T, ?, M> groupingBy(Distributed.Function<? super T, ? extends K> classifier,
                                              Distributed.Supplier<M> mapFactory,
                                              Distributed.Collector<? super T, A, D> downstream) {
        Supplier<A> downstreamSupplier = downstream.supplier();
        BiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        Distributed.BiConsumer<Map<K, A>, T> accumulator = (m, t) -> {
            K key = Objects.requireNonNull(classifier.apply(t), "element cannot be mapped to a null key");
            A container = m.computeIfAbsent(key, k -> downstreamSupplier.get());
            downstreamAccumulator.accept(container, t);
        };
        Distributed.BinaryOperator<Map<K, A>> merger = DistributedCollectors.<K, A, Map<K, A>>mapMerger(downstream.combiner());
        @SuppressWarnings("unchecked")
        Distributed.Supplier<Map<K, A>> mangledFactory = (Distributed.Supplier<Map<K, A>>) mapFactory;

        if (downstream.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
            return new DistributedCollectorImpl<>(mangledFactory, accumulator, merger, CH_ID);
        } else {
            @SuppressWarnings("unchecked")
            Distributed.Function<A, A> downstreamFinisher = (Distributed.Function<A, A>) downstream.finisher();
            Distributed.Function<Map<K, A>, M> finisher = intermediate -> {
                intermediate.replaceAll((k, v) -> downstreamFinisher.apply(v));
                @SuppressWarnings("unchecked")
                M castResult = (M) intermediate;
                return castResult;
            };
            return new DistributedCollectorImpl<>(mangledFactory, accumulator, merger, finisher, CH_NOID);
        }
    }

    // skip groupingByConcurrent

    public static <T>
    Collector<T, ?, Map<Boolean, List<T>>> partitioningBy(Distributed.Predicate<? super T> predicate) {
        return partitioningBy(predicate, toList());
    }

    public static <T, D, A>
    Collector<T, ?, Map<Boolean, D>> partitioningBy(Distributed.Predicate<? super T> predicate,
                                                    Distributed.Collector<? super T, A, D> downstream) {
        Distributed.BiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        Distributed.BiConsumer<Partition<A>, T> accumulator = (result, t) ->
                downstreamAccumulator.accept(predicate.test(t) ? result.forTrue : result.forFalse, t);
        Distributed.BinaryOperator<A> op = downstream.combiner();
        Distributed.BinaryOperator<Partition<A>> merger = (left, right) ->
                new Partition<>(op.apply(left.forTrue, right.forTrue),
                        op.apply(left.forFalse, right.forFalse));
        Distributed.Supplier<Partition<A>> supplier = () ->
                new Partition<>(downstream.supplier().get(),
                        downstream.supplier().get());
        if (downstream.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
            return new DistributedCollectorImpl<>(supplier, accumulator, merger, CH_ID);
        } else {
            Distributed.Function<Partition<A>, Map<Boolean, D>> finisher = par ->
                    new Partition<>(downstream.finisher().apply(par.forTrue),
                            downstream.finisher().apply(par.forFalse));
            return new DistributedCollectorImpl<>(supplier, accumulator, merger, finisher, CH_NOID);
        }
    }


    public static <T, K, U>
    Distributed.Collector<T, ?, Map<K, U>> toMap(Distributed.Function<? super T, ? extends K> keyMapper,
                                                 Distributed.Function<? super T, ? extends U> valueMapper) {
        return toMap(keyMapper, valueMapper, throwingMerger(), HashMap::new);
    }

    public static <T, K, U>
    Collector<T, ?, Map<K, U>> toMap(Distributed.Function<? super T, ? extends K> keyMapper,
                                     Distributed.Function<? super T, ? extends U> valueMapper,
                                     Distributed.BinaryOperator<U> mergeFunction) {
        return toMap(keyMapper, valueMapper, mergeFunction, HashMap::new);
    }

    public static <T, K, U, M extends Map<K, U>>
    Distributed.Collector<T, ?, M> toMap(Distributed.Function<? super T, ? extends K> keyMapper,
                                         Distributed.Function<? super T, ? extends U> valueMapper,
                                         Distributed.BinaryOperator<U> mergeFunction,
                                         Distributed.Supplier<M> mapSupplier) {
        Distributed.BiConsumer<M, T> accumulator
                = (map, element) -> map.merge(keyMapper.apply(element),
                valueMapper.apply(element), mergeFunction);
        return new DistributedCollectorImpl<>(mapSupplier, accumulator, mapMerger(mergeFunction), CH_ID);
    }

    // to concurrent map ? skip?

    public static <T>
    Distributed.Collector<T, ?, Distributed.IntSummaryStatistics>
    summarizingInt(Distributed.ToIntFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                Distributed.IntSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsInt(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    public static <T>
    Distributed.Collector<T, ?, Distributed.LongSummaryStatistics>
    summarizingLong(Distributed.ToLongFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                Distributed.LongSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsLong(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    public static <T>
    Distributed.Collector<T, ?, Distributed.DoubleSummaryStatistics>
    summarizingDouble(Distributed.ToDoubleFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                Distributed.DoubleSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsDouble(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    //** JET SPECIFIC **//

    public static <T, K, U> Distributed.Collector<T, ?, IMap<K, U>>
    toIMap(Distributed.Function<? super T, ? extends K> keyMapper,
           Distributed.Function<? super T, ? extends U> valueMapper) {
        return new HazelcastMapCollector<>(keyMapper, valueMapper);
    }

    public static <K, U> Distributed.Collector<Map.Entry<K, U>, ?, IMap<K, U>> toIMap() {
        return toIMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    public static <T, K, U> Distributed.Collector<T, ?, IMap<K, U>>
    toIMap(Distributed.Function<? super T, ? extends K> keyMapper,
           Distributed.Function<? super T, ? extends U> valueMapper,
           Distributed.BinaryOperator<U> mergeFunction) {
        return new HazelcastMergingMapCollector<>(keyMapper, valueMapper, mergeFunction);
    }

    public static <T> Distributed.Collector<T, ?, IList<T>> toIList(String listName) {
        return new HazelcastListCollector<>(listName);
    }

    public static <T> Distributed.Collector<T, ?, IList<T>> toIList() {
        return new HazelcastListCollector<>();
    }

    public static <T, K> Distributed.Collector<T, ?, IMap<K, List<T>>>
    groupingByToIMap(Distributed.Function<? super T, ? extends K> classifier) {
        return groupingByToIMap(classifier, toList());
    }

    public static <T, K, A, D>
    Distributed.Collector<T, ?, IMap<K, D>> groupingByToIMap(Distributed.Function<? super T, ? extends K> classifier,
                                                             Distributed.Collector<? super T, A, D> downstream) {
        return new HazelcastGroupingMapCollector<>(classifier, downstream);
    }

    private static <K, V, M extends Map<K, V>>
    Distributed.BinaryOperator<M> mapMerger(Distributed.BinaryOperator<V> mergeFunction) {
        return (m1, m2) -> {
            for (Map.Entry<K, V> e : m2.entrySet()) {
                m1.merge(e.getKey(), e.getValue(), mergeFunction);
            }
            return m1;
        };
    }

    private static <T> Distributed.BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    static <I, R> Distributed.Function<I, R> castingIdentity() {
        return i -> (R) i;
    }

    static double[] sumWithCompensation(double[] intermediateSum, double value) {
        double tmp = value - intermediateSum[1];
        double sum = intermediateSum[0];
        // Little wolf of rounding error
        double velvel = sum + tmp;
        intermediateSum[1] = (velvel - sum) - tmp;
        intermediateSum[0] = velvel;
        return intermediateSum;
    }

    static double computeFinalSum(double[] summands) {
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

        @Override
        public Set<Map.Entry<Boolean, T>> entrySet() {
            return new AbstractSet<Entry<Boolean, T>>() {
                @Override
                public Iterator<Entry<Boolean, T>> iterator() {
                    Map.Entry<Boolean, T> falseEntry = new SimpleImmutableEntry<>(false, forFalse);
                    Map.Entry<Boolean, T> trueEntry = new SimpleImmutableEntry<>(true, forTrue);
                    return Arrays.asList(falseEntry, trueEntry).iterator();
                }

                @Override
                public int size() {
                    return 2;
                }
            };
        }
    }
}
