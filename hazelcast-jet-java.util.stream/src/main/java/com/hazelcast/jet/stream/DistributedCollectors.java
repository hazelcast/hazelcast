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
import com.hazelcast.jet.stream.impl.distributed.DistributedDoubleSummaryStatistics;
import com.hazelcast.jet.stream.impl.distributed.DistributedIntSummaryStatistics;
import com.hazelcast.jet.stream.impl.collectors.DistributedCollectorImpl;
import com.hazelcast.jet.stream.impl.collectors.DistributedStringJoiner;
import com.hazelcast.jet.stream.impl.collectors.HazelcastGroupingMapCollector;
import com.hazelcast.jet.stream.impl.collectors.HazelcastListCollector;
import com.hazelcast.jet.stream.impl.collectors.HazelcastMapCollector;
import com.hazelcast.jet.stream.impl.collectors.HazelcastMergingMapCollector;
import com.hazelcast.jet.stream.impl.distributed.DistributedLongSummaryStatistics;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;

/**
 * Distributed implementations of {@link java.util.stream.Collectors}
 *
 * Implementations of {@link Distributed.Collector} that implement various useful reduction
 * operations, such as accumulating elements into collections, summarizing
 * elements according to various criteria, etc.
 */
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

    /**
     * Returns a {@code Distributed.Collector} that accumulates the input elements into a
     * new {@code Collection}, in encounter order.  The {@code Collection} is
     * created by the provided factory.
     *
     * @param <T> the type of the input elements
     * @param <C> the type of the resulting {@code Collection}
     * @param collectionFactory a {@code Distributed.Supplier} which returns a new, empty
     * {@code Collection} of the appropriate type
     * @return a {@code Distributed.Collector} which collects all the input elements into a
     * {@code Collection}, in encounter order
     */
    public static <T, C extends Collection<T>>
    Distributed.Collector<T, ?, C> toCollection(Distributed.Supplier<C> collectionFactory) {
        return new DistributedCollectorImpl<>(collectionFactory, Collection::add,
                (r1, r2) -> {
                    r1.addAll(r2);
                    return r1;
                },
                CH_ID);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates the input elements into a
     * new {@code List}. There are no guarantees on the type, mutability,
     * serializability, or thread-safety of the {@code List} returned; if more
     * control over the returned {@code List} is required, use {@link #toCollection(Distributed.Supplier)}.
     *
     * @param <T> the type of the input elements
     * @return a {@code Distributed.Collector} which collects all the input elements into a
     * {@code List}, in encounter order
     */
    public static <T> Distributed.Collector<T, ?, List<T>> toList() {
        return new DistributedCollectorImpl<>(ArrayList::new, List::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                CH_ID);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates the input elements into a
     * new {@code Set}. There are no guarantees on the type, mutability,
     * serializability, or thread-safety of the {@code Set} returned; if more
     * control over the returned {@code Set} is required, use
     * {@link #toCollection(Distributed.Supplier)}.
     *
     * <p>This is an {@link Collector.Characteristics#UNORDERED unordered}
     * Collector.
     *
     * @param <T> the type of the input elements
     * @return a {@code  Distributed.Collector} which collects all the input elements into a
     * {@code Set}
     */
    public static <T>
    Distributed.Collector<T, ?, Set<T>> toSet() {
        return new DistributedCollectorImpl<>((Distributed.Supplier<Set<T>>) HashSet::new, Set<T>::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                CH_UNORDERED_ID);
    }

    /**
     * Returns a {@code Distributed.Collector} that concatenates the input elements into a
     * {@code String}, in encounter order.
     *
     * @return a {@code Distributed.Collector} that concatenates the input elements into a
     * {@code String}, in encounter order
     */
    public static Distributed.Collector<CharSequence, ?, String> joining() {
        return new DistributedCollectorImpl<>(
                StringBuilder::new, StringBuilder::append,
                (r1, r2) -> {
                    r1.append(r2);
                    return r1;
                },
                StringBuilder::toString, CH_NOID);
    }

    /**
     * Returns a {@code Distributed.Collector} that concatenates the input elements,
     * separated by the specified delimiter, in encounter order.
     *
     * @param delimiter the delimiter to be used between each element
     * @return A {@code Distributed.Collector} which concatenates CharSequence elements,
     * separated by the specified delimiter, in encounter order
     */
    public static Distributed.Collector<CharSequence, ?, String> joining(CharSequence delimiter) {
        return joining(delimiter, "", "");
    }

    /**
     * Returns a {@code Distributed.Collector} that concatenates the input elements,
     * separated by the specified delimiter, with the specified prefix and
     * suffix, in encounter order.
     *
     * @param delimiter the delimiter to be used between each element
     * @param  prefix the sequence of characters to be used at the beginning
     *                of the joined result
     * @param  suffix the sequence of characters to be used at the end
     *                of the joined result
     * @return A {@code Distributed.Collector} which concatenates CharSequence elements,
     * separated by the specified delimiter, in encounter order
     */
    public static Distributed.Collector<CharSequence, ?, String> joining(CharSequence delimiter,
                                                                         CharSequence prefix,
                                                                         CharSequence suffix) {
        return new DistributedCollectorImpl<>(
                () -> new DistributedStringJoiner(delimiter, prefix, suffix),
                DistributedStringJoiner::add, DistributedStringJoiner::merge,
                DistributedStringJoiner::toString, CH_NOID);
    }

    /**
     * Adapts a {@code Distributed.Collector} accepting elements of type {@code U} to one
     * accepting elements of type {@code T} by applying a mapping function to
     * each input element before accumulation.
     *
     *
     * @param <T> the type of the input elements
     * @param <U> type of elements accepted by downstream collector
     * @param <A> intermediate accumulation type of the downstream collector
     * @param <R> result type of collector
     * @param mapper a function to be applied to the input elements
     * @param downstream a collector which will accept mapped values
     * @return a collector which applies the mapping function to the input
     * elements and provides the mapped results to the downstream collector
     */
    public static <T, U, A, R>
    Distributed.Collector<T, ?, R> mapping(Distributed.Function<? super T, ? extends U> mapper,
                                           Distributed.Collector<? super U, A, R> downstream) {
        Distributed.BiConsumer<A, ? super U> downstreamAccumulator = downstream.accumulator();
        return new DistributedCollectorImpl<>(downstream.supplier(),
                (r, t) -> downstreamAccumulator.accept(r, mapper.apply(t)),
                downstream.combiner(), downstream.finisher(),
                downstream.characteristics());
    }

    /**
     * Adapts a {@code Distributed.Collector} to perform an additional finishing
     * transformation.  For example, one could adapt the {@link #toList()}
     * collector to always produce an immutable list with:
     * <pre>{@code
     *     List<String> people
     *         = people.stream().collect(collectingAndThen(toList(), Collections::unmodifiableList));
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @param <A> intermediate accumulation type of the downstream collector
     * @param <R> result type of the downstream collector
     * @param <RR> result type of the resulting collector
     * @param downstream a collector
     * @param finisher a function to be applied to the final result of the downstream collector
     * @return a collector which performs the action of the downstream collector,
     * followed by an additional finishing step
     */
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

    /**
     * Returns a {@code Distributed.Collector} accepting elements of type {@code T} that
     * counts the number of input elements.  If no elements are present, the
     * result is 0.
     *
     * @implSpec
     * This produces a result equivalent to:
     * <pre>{@code
     *     reducing(0L, e -> 1L, Long::sum)
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @return a {@code Distributed.Collector} that counts the input elements
     */
    public static <T> Distributed.Collector<T, ?, Long> counting() {
        return reducing(0L, e -> 1L, Long::sum);
    }

    /**
     * Returns a {@code Distributed.Collector} that produces the minimal element according
     * to a given {@code Distributed.Comparator}, described as an {@code Optional<T>}.
     *
     * @implSpec
     * This produces a result equivalent to:
     * <pre>{@code
     *     reducing(Distributed.BinaryOperator.minBy(comparator))
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @param comparator a {@code Distributed.Comparator} for comparing elements
     * @return a {@code Distributed.Collector} that produces the minimal value
     */
    public static <T> Distributed.Collector<T, ?, Distributed.Optional<T>>
    minBy(Distributed.Comparator<? super T> comparator) {
        return reducing(Distributed.BinaryOperator.minBy(comparator));
    }

    /**
     * Returns a {@code Distributed.Collector} that produces the maximal element according
     * to a given {@code Distributed.Comparator}, described as an {@code Optional<T>}.
     *
     * @implSpec
     * This produces a result equivalent to:
     * <pre>{@code
     *     reducing(Distributed.BinaryOperator.maxBy(comparator))
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @param comparator a {@code Distributed.Comparator} for comparing elements
     * @return a {@code Distributed.Collector} that produces the maximal value
     */
    public static <T> Distributed.Collector<T, ?, Distributed.Optional<T>>
    maxBy(Distributed.Comparator<? super T> comparator) {
        return reducing(Distributed.BinaryOperator.maxBy(comparator));
    }

    /**
     * Returns a {@code Distributed.Collector} that produces the sum of a integer-valued
     * function applied to the input elements.  If no elements are present,
     * the result is 0.
     *
     * @param <T> the type of the input elements
     * @param mapper a function extracting the property to be summed
     * @return a {@code Distributed.Collector} that produces the sum of a derived property
     */
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

    /**
     * Returns a {@code Distributed.Collector} that produces the sum of a long-valued
     * function applied to the input elements.  If no elements are present,
     * the result is 0.
     *
     * @param <T> the type of the input elements
     * @param mapper a function extracting the property to be summed
     * @return a {@code Distributed.Collector} that produces the sum of a derived property
     */
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

    /**
     * Returns a {@code Distributed.Collector} that produces the sum of a double-valued
     * function applied to the input elements.  If no elements are present,
     * the result is 0.
     *
     * <p>The sum returned can vary depending upon the order in which
     * values are recorded, due to accumulated rounding error in
     * addition of values of differing magnitudes. Values sorted by increasing
     * absolute magnitude tend to yield more accurate results.  If any recorded
     * value is a {@code NaN} or the sum is at any point a {@code NaN} then the
     * sum will be {@code NaN}.
     *
     * @param <T> the type of the input elements
     * @param mapper a function extracting the property to be summed
     * @return a {@code Distributed.Collector} that produces the sum of a derived property
     */
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

    /**
     * Returns a {@code Distributed.Collector} that produces the arithmetic mean of an integer-valued
     * function applied to the input elements.  If no elements are present,
     * the result is 0.
     *
     * @param <T> the type of the input elements
     * @param mapper a function extracting the property to be summed
     * @return a {@code Distributed.Collector} that produces the sum of a derived property
     */
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
    /**
     * Returns a {@code Distributed.Collector} that produces the arithmetic mean of a long-valued
     * function applied to the input elements.  If no elements are present,
     * the result is 0.
     *
     * @param <T> the type of the input elements
     * @param mapper a function extracting the property to be summed
     * @return a {@code Distributed.Collector} that produces the sum of a derived property
     */
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

    /**
     * Returns a {@code Distributed.Collector} that produces the arithmetic mean of a double-valued
     * function applied to the input elements.  If no elements are present,
     * the result is 0.
     *
     * <p>The average returned can vary depending upon the order in which
     * values are recorded, due to accumulated rounding error in
     * addition of values of differing magnitudes. Values sorted by increasing
     * absolute magnitude tend to yield more accurate results.  If any recorded
     * value is a {@code NaN} or the sum is at any point a {@code NaN} then the
     * average will be {@code NaN}.
     *
     * @param <T> the type of the input elements
     * @param mapper a function extracting the property to be summed
     * @return a {@code Distributed.Collector} that produces the sum of a derived property
     */
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

    /**
     * Returns a {@code Distributed.Collector} which performs a reduction of its
     * input elements under a specified {@code Distributed.BinaryOperator} using the
     * provided identity.
     *
     * @param <T> element type for the input and output of the reduction
     * @param identity the identity value for the reduction (also, the value
     *                 that is returned when there are no input elements)
     * @param op a {@code Distributed.BinaryOperator<T>} used to reduce the input elements
     * @return a {@code Distributed.Collector} which implements the reduction operation
     *
     * @see #reducing(Distributed.BinaryOperator)
     * @see #reducing(Object, Distributed.Function, Distributed.BinaryOperator)
     */
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

    /**
     * Returns a {@code Distributed.Collector} which performs a reduction of its
     * input elements under a specified {@code Distributed.BinaryOperator}.  The result
     * is described as an {@code Optional<T>}.
     *
     * @apiNote
     * The {@code reducing()} collectors are most useful when used in a
     * multi-level reduction, downstream of {@code groupingBy} or
     * {@code partitioningBy}.  To perform a simple reduction on a stream,
     * use {@link DistributedStream#reduce(Distributed.BinaryOperator)} instead.
     **
     * @param <T> element type for the input and output of the reduction
     * @param op a {@code Distributed.BinaryOperator<T>} used to reduce the input elements
     * @return a {@code Distributed.Collector} which implements the reduction operation
     *
     * @see #reducing(Object, Distributed.BinaryOperator)
     * @see #reducing(Object, Distributed.Function, Distributed.BinaryOperator)
     */
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

    /**
     * Returns a {@code Distributed.Collector} which performs a reduction of its
     * input elements under a specified mapping function and
     * {@code Distributed.BinaryOperator}. This is a generalization of
     * {@link #reducing(Object, Distributed.BinaryOperator)} which allows a transformation
     * of the elements before reduction.
     *
     * @apiNote
     * The {@code reducing()} collectors are most useful when used in a
     * multi-level reduction, downstream of {@code groupingBy} or
     * {@code partitioningBy}.  To perform a simple map-reduce on a stream,
     * use {@link DistributedStream#map(Distributed.Function)} and
     * {@link DistributedStream#reduce(Object, Distributed.BinaryOperator)}
     * instead.
     *
     * @param <T> the type of the input elements
     * @param <U> the type of the mapped values
     * @param identity the identity value for the reduction (also, the value
     *                 that is returned when there are no input elements)
     * @param mapper a mapping function to apply to each input value
     * @param op a {@code Distributed.BinaryOperator<U>} used to reduce the mapped values
     * @return a {@code Distributed.Collector} implementing the map-reduce operation
     *
     * @see #reducing(Object, Distributed.BinaryOperator)
     * @see #reducing(Distributed.BinaryOperator)
     */
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

    /**
     * Returns a {@code Distributed.Collector} implementing a "group by" operation on
     * input elements of type {@code T}, grouping elements according to a
     * classification function, and returning the results in a {@code Map}.
     *
     * <p>The classification function maps elements to some key type {@code K}.
     * The collector produces a {@code Map<K, List<T>>} whose keys are the
     * values resulting from applying the classification function to the input
     * elements, and whose corresponding values are {@code List}s containing the
     * input elements which map to the associated key under the classification
     * function.
     *
     * <p>There are no guarantees on the type, mutability, serializability, or
     * thread-safety of the {@code Map} or {@code List} objects returned.
     *
     * @param <T> the type of the input elements
     * @param <K> the type of the keys
     * @param classifier the classifier function mapping input elements to keys
     * @return a {@code Distributed.Collector} implementing the group-by operation
     *
     * @see #groupingBy(Distributed.Function, Distributed.Collector)
     * @see #groupingBy(Distributed.Function, Distributed.Supplier, Distributed.Collector)
     */
    public static <T, K> Distributed.Collector<T, ?, Map<K, List<T>>>
    groupingBy(Distributed.Function<? super T, ? extends K> classifier) {
        return groupingBy(classifier, toList());
    }

    /**
     * Returns a {@code Distributed.Collector} implementing a cascaded "group by" operation
     * on input elements of type {@code T}, grouping elements according to a
     * classification function, and then performing a reduction operation on
     * the values associated with a given key using the specified downstream
     * {@code Distributed.Collector}.
     *
     * <p>The classification function maps elements to some key type {@code K}.
     * The downstream collector operates on elements of type {@code T} and
     * produces a result of type {@code D}. The resulting collector produces a
     * {@code Map<K, D>}.
     *
     * <p>There are no guarantees on the type, mutability,
     * serializability, or thread-safety of the {@code Map} returned.
     *
     * <p>For example, to compute the set of last names of people in each city:
     * <pre>{@code
     *     Map<City, Set<String>> namesByCity
     *         = people.stream().collect(groupingBy(Person::getCity,
     *                                              mapping(Person::getLastName, toSet())));
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @param <K> the type of the keys
     * @param <A> the intermediate accumulation type of the downstream collector
     * @param <D> the result type of the downstream reduction
     * @param classifier a classifier function mapping input elements to keys
     * @param downstream a {@code Distributed.Collector} implementing the downstream reduction
     * @return a {@code Distributed.Collector} implementing the cascaded group-by operation
     * @see #groupingBy(Distributed.Function)
     * @see #groupingBy(Distributed.Function, Distributed.Supplier, Distributed.Collector)
     */
    public static <T, K, A, D>
    Distributed.Collector<T, ?, Map<K, D>> groupingBy(Distributed.Function<? super T, ? extends K> classifier,
                                                      Distributed.Collector<? super T, A, D> downstream) {
        return groupingBy(classifier, HashMap::new, downstream);
    }

    /**
     * Returns a {@code Distributed.Collector} implementing a cascaded "group by" operation
     * on input elements of type {@code T}, grouping elements according to a
     * classification function, and then performing a reduction operation on
     * the values associated with a given key using the specified downstream
     * {@code Distributed.Collector}.  The {@code Map} produced by the Collector is created
     * with the supplied factory function.
     *
     * <p>The classification function maps elements to some key type {@code K}.
     * The downstream collector operates on elements of type {@code T} and
     * produces a result of type {@code D}. The resulting collector produces a
     * {@code Map<K, D>}.
     *
     * <p>For example, to compute the set of last names of people in each city,
     * where the city names are sorted:
     * <pre>{@code
     *     Map<City, Set<String>> namesByCity
     *         = people.stream().collect(groupingBy(Person::getCity, TreeMap::new,
     *                                              mapping(Person::getLastName, toSet())));
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @param <K> the type of the keys
     * @param <A> the intermediate accumulation type of the downstream collector
     * @param <D> the result type of the downstream reduction
     * @param <M> the type of the resulting {@code Map}
     * @param classifier a classifier function mapping input elements to keys
     * @param downstream a {@code Distributed.Collector} implementing the downstream reduction
     * @param mapFactory a function which, when called, produces a new empty
     *                   {@code Map} of the desired type
     * @return a {@code Distributed.Collector} implementing the cascaded group-by operation
     *
     * @see #groupingBy(Distributed.Function, Distributed.Collector)
     * @see #groupingBy(Distributed.Function)
     */
    public static <T, K, D, A, M extends Map<K, D>>
    Distributed.Collector<T, ?, M> groupingBy(Distributed.Function<? super T, ? extends K> classifier,
                                              Distributed.Supplier<M> mapFactory,
                                              Distributed.Collector<? super T, A, D> downstream) {
        Distributed.Supplier<A> downstreamSupplier = downstream.supplier();
        Distributed.BiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
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

    /**
     * Returns a {@code Distributed.Collector} which partitions the input elements according
     * to a {@code Distributed.Predicate}, and organizes them into a
     * {@code Map<Boolean, List<T>>}.
     *
     * There are no guarantees on the type, mutability,
     * serializability, or thread-safety of the {@code Map} returned.
     *
     * @param <T> the type of the input elements
     * @param predicate a predicate used for classifying input elements
     * @return a {@code Distributed.Collector} implementing the partitioning operation
     *
     * @see #partitioningBy(Distributed.Predicate, Distributed.Collector)
     */
    public static <T>
    Collector<T, ?, Map<Boolean, List<T>>> partitioningBy(Distributed.Predicate<? super T> predicate) {
        return partitioningBy(predicate, toList());
    }

    /**
     * Returns a {@code Distributed.Collector} which partitions the input elements according
     * to a {@code Distributed.Predicate}, reduces the values in each partition according to
     * another {@code Distributed.Collector}, and organizes them into a
     * {@code Map<Boolean, D>} whose values are the result of the downstream
     * reduction.
     *
     * <p>There are no guarantees on the type, mutability,
     * serializability, or thread-safety of the {@code Map} returned.
     *
     * @param <T> the type of the input elements
     * @param <A> the intermediate accumulation type of the downstream collector
     * @param <D> the result type of the downstream reduction
     * @param predicate a predicate used for classifying input elements
     * @param downstream a {@code Distributed.Collector} implementing the downstream
     *                   reduction
     * @return a {@code Distributed.Collector} implementing the cascaded partitioning
     *         operation
     *
     * @see #partitioningBy(Distributed.Predicate)
     */
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

    /**
     * Returns a {@code Distributed.Collector} that accumulates elements into a
     * {@code Map} whose keys and values are the result of applying the provided
     * mapping functions to the input elements.
     *
     * <p>If the mapped keys contains duplicates (according to
     * {@link Object#equals(Object)}), an {@code IllegalStateException} is
     * thrown when the collection operation is performed.  If the mapped keys
     * may have duplicates, use {@link #toMap(Distributed.Function, Distributed.Function,
     * Distributed.BinaryOperator)}
     * instead.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @return a {@code Distributed.Collector} which collects elements into a {@code Map}
     * whose keys and values are the result of applying mapping functions to
     * the input elements
     *
     * @see #toMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator)
     * @see #toMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator,
     * Distributed.Supplier)
     */
    public static <T, K, U>
    Distributed.Collector<T, ?, Map<K, U>> toMap(Distributed.Function<? super T, ? extends K> keyMapper,
                                                 Distributed.Function<? super T, ? extends U> valueMapper) {
        return toMap(keyMapper, valueMapper, throwingMerger(), HashMap::new);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates elements into a
     * {@code Map} whose keys and values are the result of applying the provided
     * mapping functions to the input elements.
     *
     * <p>If the mapped
     * keys contains duplicates (according to {@link Object#equals(Object)}),
     * the value mapping function is applied to each equal element, and the
     * results are merged using the provided merging function.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @param mergeFunction a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     * @return a {@code Distributed.Collector} which collects elements into a {@code Map}
     * whose keys are the result of applying a key mapping function to the input
     * elements, and whose values are the result of applying a value mapping
     * function to all input elements equal to the key and combining them
     * using the merge function
     *
     * @see #toMap(Distributed.Function, Distributed.Function)
     * @see #toMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator,
     * Distributed.Supplier)
     */
    public static <T, K, U>
    Collector<T, ?, Map<K, U>> toMap(Distributed.Function<? super T, ? extends K> keyMapper,
                                     Distributed.Function<? super T, ? extends U> valueMapper,
                                     Distributed.BinaryOperator<U> mergeFunction) {
        return toMap(keyMapper, valueMapper, mergeFunction, HashMap::new);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates elements into a
     * {@code Map} whose keys and values are the result of applying the provided
     * mapping functions to the input elements.
     *
     * <p>If the mapped
     * keys contains duplicates (according to {@link Object#equals(Object)}),
     * the value mapping function is applied to each equal element, and the
     * results are merged using the provided merging function.  The {@code Map}
     * is created by a provided supplier function.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param <M> the type of the resulting {@code Map}
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @param mergeFunction a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object, java.util.function.BiFunction)}
     * @param mapSupplier a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     * @return a {@code Distributed.Collector} which collects elements into a {@code Map}
     * whose keys are the result of applying a key mapping function to the input
     * elements, and whose values are the result of applying a value mapping
     * function to all input elements equal to the key and combining them
     * using the merge function
     *
     * @see #toMap(Distributed.Function, Distributed.Function)
     * @see #toMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator)
     */
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

    /**
     * Returns a {@code Distributed.Collector} which applies an {@code int}-producing
     * mapping function to each input element, and returns summary statistics
     * for the resulting values.
     *
     * @param <T> the type of the input elements
     * @param mapper a mapping function to apply to each element
     * @return a {@code Distributed.Collector} implementing the summary-statistics reduction
     *
     * @see #summarizingDouble(Distributed.ToDoubleFunction)
     * @see #summarizingLong(Distributed.ToLongFunction)
     */
    public static <T>
    Distributed.Collector<T, ?, IntSummaryStatistics>
    summarizingInt(Distributed.ToIntFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                DistributedIntSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsInt(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    /**
     * Returns a {@code Distributed.Collector} which applies an {@code long}-producing
     * mapping function to each input element, and returns summary statistics
     * for the resulting values.
     *
     * @param <T> the type of the input elements
     * @param mapper the mapping function to apply to each element
     * @return a {@code Distributed.Collector} implementing the summary-statistics reduction
     *
     * @see #summarizingDouble(Distributed.ToDoubleFunction)
     * @see #summarizingInt(Distributed.ToIntFunction)
     */
    public static <T>
    Distributed.Collector<T, ?, LongSummaryStatistics>
    summarizingLong(Distributed.ToLongFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                DistributedLongSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsLong(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    /**
     * Returns a {@code Distributed.Collector} which applies an {@code double}-producing
     * mapping function to each input element, and returns summary statistics
     * for the resulting values.
     *
     * @param <T> the type of the input elements
     * @param mapper a mapping function to apply to each element
     * @return a {@code Distributed.Collector} implementing the summary-statistics reduction
     *
     * @see #summarizingLong(Distributed.ToLongFunction)
     * @see #summarizingInt(Distributed.ToIntFunction)
     */
    public static <T>
    Distributed.Collector<T, ?, DoubleSummaryStatistics>
    summarizingDouble(Distributed.ToDoubleFunction<? super T> mapper) {
        return new DistributedCollectorImpl<>(
                DistributedDoubleSummaryStatistics::new,
                (r, t) -> r.accept(mapper.applyAsDouble(t)),
                (l, r) -> {
                    l.combine(r);
                    return l;
                }, CH_ID);
    }

    //** JET SPECIFIC **//

    /**
     * Returns a {@code Distributed.Collector} that accumulates elements into a
     * new Hazelcast {@code IMap} whose keys and values are the result of applying the provided
     * mapping functions to the input elements.
     *
     * <p>If the mapped keys contains duplicates (according to
     * {@link Object#equals(Object)}), only one of the mapped values will be in the final map,
     * and the others will be dropped. If the mapped keys may have duplicates, use
     * {@link #toMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator)}
     * instead.
     *
     * The returned collector may not be used as a downstream collector.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @return a {@code Distributed.Collector} which collects elements into a {@code IMap}
     * whose keys and values are the result of applying mapping functions to
     * the input elements
     *
     * @see #toIMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator)
     */
    public static <T, K, U> Distributed.Collector<T, ?, IMap<K, U>>
    toIMap(Distributed.Function<? super T, ? extends K> keyMapper,
           Distributed.Function<? super T, ? extends U> valueMapper) {
        return new HazelcastMapCollector<>(keyMapper, valueMapper);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates elements into a
     * new distributed Hazelcast {@code IMap} whose keys and values are the keys and values of
     * the corresponding {@code Map.Entry}.
     *
     * * <p>If the mapped keys contains duplicates (according to
     * {@link Object#equals(Object)}), only one of the mapped values will be in the final map,
     * and the others will be dropped. If the mapped keys may have duplicates, use
     * {@link #toMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator)}
     * instead.
     *
     * The returned collector may not be used as a downstream collector.
     *
     * @param <K> The type of the key in {@code Map.Entry}
     * @param <U> The type of the value in {@code Map.Entry}
     * @return a {@code Distributed.Collector} that accumulates elements into a
     * Hazelcast {@code IMap} whose keys and values are the keys and values of the corresponding
     * {@code Map.Entry}.
     *
     * @see #toIMap(Distributed.Function, Distributed.Function)
     * @see #toIMap(Distributed.Function, Distributed.Function, Distributed.BinaryOperator)
     */
    public static <K, U> Distributed.Collector<Map.Entry<K, U>, ?, IMap<K, U>> toIMap() {
        return toIMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates elements into a
     * new distributed Hazelcast {@code IMap} whose keys and values are the result of applying
     * the provided mapping functions to the input elements.
     *
     * <p>If the mapped
     * keys contains duplicates (according to {@link Object#equals(Object)}),
     * the value mapping function is applied to each equal element, and the
     * results are merged using the provided merging function.
     *
     * The returned collector may not be used as a downstream collector.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @param mergeFunction a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     * @return a {@code Distributed.Collector} which collects elements into a distributed
     * {@code IMap} whose keys are the result of applying a key mapping function to the input
     * elements, and whose values are the result of applying a value mapping
     * function to all input elements equal to the key and combining them
     * using the merge function
     *
     * @see #toIMap(Distributed.Function, Distributed.Function)
     */
    public static <T, K, U> Distributed.Collector<T, ?, IMap<K, U>>
    toIMap(Distributed.Function<? super T, ? extends K> keyMapper,
           Distributed.Function<? super T, ? extends U> valueMapper,
           Distributed.BinaryOperator<U> mergeFunction) {
        return new HazelcastMergingMapCollector<>(keyMapper, valueMapper, mergeFunction);
    }

    /**
     * Returns a {@code Distributed.Collector} that accumulates the input elements into a
     * new Hazelcast {@code IList}.
     *
     * The returned collector may not be used as a downstream collector.
     *
     * @param <T> the type of the input elements
     * @return a {@code Distributed.Collector} which collects all the input elements into a
     * Hazelcast {@code IList}, in encounter order
     */
    public static <T> Distributed.Collector<T, ?, IList<T>> toIList() {
        return new HazelcastListCollector<>();
    }

    /**
     * Returns a {@code Distributed.Collector} implementing a "group by" operation on
     * input elements of type {@code T}, grouping elements according to a
     * classification function, and returning the results in a
     * new distributed Hazelcast {@code IMap}.
     *
     * <p>The classification function maps elements to some key type {@code K}.
     * The collector produces a {@code Map<K, List<T>>} whose keys are the
     * values resulting from applying the classification function to the input
     * elements, and whose corresponding values are {@code List}s containing the
     * input elements which map to the associated key under the classification
     * function.
     *
     * The returned collector may not be used as a downstream collector.
     *
     * @param <T> the type of the input elements
     * @param <K> the type of the keys
     * @param classifier the classifier function mapping input elements to keys
     * @return a {@code Distributed.Collector} implementing the group-by operation
     *
     * @see #groupingByToIMap(Distributed.Function, Distributed.Collector)
     */
    public static <T, K> Distributed.Collector<T, ?, IMap<K, List<T>>>
    groupingByToIMap(Distributed.Function<? super T, ? extends K> classifier) {
        return groupingByToIMap(classifier, toList());
    }

    /**
     * Returns a {@code Distributed.Collector} implementing a cascaded "group by" operation
     * on input elements of type {@code T}, grouping elements according to a
     * classification function, and then performing a reduction operation on
     * the values associated with a given key using the specified downstream
     * {@code Distributed.Collector}.
     *
     * <p>The classification function maps elements to some key type {@code K}.
     * The downstream collector operates on elements of type {@code T} and
     * produces a result of type {@code D}. The resulting collector produces a new
     * Hazelcast distributed {@code IMap<K, D>}.
     *
     * <p>For example, to compute the set of last names of people in each city:
     * <pre>{@code
     *     IMap<City, Set<String>> namesByCity
     *         = people.stream().collect(groupingBy(Person::getCity,
     *                                              mapping(Person::getLastName, toSet())));
     * }</pre>
     *
     * @param <T> the type of the input elements
     * @param <K> the type of the keys
     * @param <A> the intermediate accumulation type of the downstream collector
     * @param <D> the result type of the downstream reduction
     * @param classifier a classifier function mapping input elements to keys
     * @param downstream a {@code Distributed.Collector} implementing the downstream reduction
     * @return a {@code Distributed.Collector} implementing the cascaded group-by operation
     * @see #groupingByToIMap(Distributed.Function)
     */
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
