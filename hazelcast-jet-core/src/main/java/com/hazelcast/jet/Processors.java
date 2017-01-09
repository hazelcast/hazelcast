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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.impl.connector.AbstractProducer;
import com.hazelcast.jet.impl.connector.IListReader;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.jet.impl.connector.IMapWriter;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.Distributed.BiFunction;
import com.hazelcast.jet.stream.Distributed.Function;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.hazelcast.jet.Suppliers.lazyIterate;
import static com.hazelcast.jet.Suppliers.map;

/**
 * Static utility class with factory methods for predefined processors.
 */
public final class Processors {

    private Processors() {
    }

    /**
     * @return processors for partitioned reading from a Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapReader(String mapName) {
        return IMapReader.supplier(mapName);
    }

    /**
     * @return processors for partitioned reading from a remote Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapReader(String mapName, ClientConfig clientConfig) {
        return IMapReader.supplier(mapName, clientConfig);
    }

    /**
     * @return processors for  writing to a Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapWriter(String mapName) {
        return IMapWriter.supplier(mapName);
    }

    /**
     * @return processors for  writing to a remote Hazelcast IMap
     */
    public static ProcessorMetaSupplier mapWriter(String mapName, ClientConfig clientConfig) {
        return IMapWriter.supplier(mapName, clientConfig);
    }

    /**
     * @return processors for reading from a Hazelcast IList
     */
    public static ProcessorMetaSupplier listReader(String listName) {
        return IListReader.supplier(listName);
    }

    /**
     * @return processors for reading from a remote Hazelcast IList
     */
    public static ProcessorMetaSupplier listReader(String listName, ClientConfig clientConfig) {
        return IListReader.supplier(listName, clientConfig);
    }

    /**
     * @return a processor for writing to a Hazelcast IList
     */
    public static ProcessorSupplier listWriter(String listName) {
        return IListWriter.supplier(listName);
    }

    /**
     * @return a processor for writing to a remote Hazelcast IList
     */
    public static ProcessorSupplier listWriter(String listName, ClientConfig clientConfig) {
        return IListWriter.supplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     Applies the key extractor to each item and obtains the key of type {@code K}.
     * </li><li>
     *     Stores for each key the result of applying the accumulator function to
     *     the previously accumulated value and the current item. The initial
     *     accumulated value for all keys is {@code null}.
     * </li><li>
     *     When all the input is consumed, begins emitting the accumulated results.
     * </li><li>
     *     Emits items of type {@code R} obtained by applying the finisher function
     *     to each seen key and its accumulated value.
     * </li></ul>
     *
     * @param keyExtractor computes the key from the entry
     * @param accumulator accumulates the result value across all entries under the same key
     * @param <T> type of item on the inbound edge
     * @param <K> type of the key
     * @param <A> type of the accumulated value
     * @param <R> type of the emitted value
     */
    public static <T, K, A, R> ProcessorSupplier groupAndAccumulate(
            Distributed.Function<? super T, ? extends K> keyExtractor,
            Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
            Distributed.BiFunction<? super K, ? super A, ? extends R> finisher
    ) {
        return ProcessorSupplier.of(() -> new GroupingAccumulatorP<>(keyExtractor, accumulator, finisher));
    }

    /**
     * Convenience over {@link #groupAndAccumulate(Function, BiFunction, BiFunction)}
     * with the constructor of {@code SimpleImmutableEntry} as the finisher function,
     * which means the processor emits items of type
     * {@code java.util.Map.Entry<K, A>}. Note that {@code K} isn't a part of the
     * method's signature since nothing in the processor depends on it. The
     * receiving processor will in any case have to perform an unchecked cast to
     * {@code Entry<K, A>}.
     *
     * @param keyExtractor computes the key from the entry
     * @param accumulator accumulates the result value across all entries under the same key
     * @param <T> type of item on the inbound edge
     * @param <A> type of the accumulated value
     */
    public static <T, A> ProcessorSupplier groupAndAccumulate(
            Distributed.Function<? super T, ?> keyExtractor,
            Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return ProcessorSupplier.of(() -> new GroupingAccumulatorP<>(
                keyExtractor, accumulator, SimpleImmutableEntry::new));
    }

    /**
     * Convenience over {@link #groupAndAccumulate(Function, BiFunction, BiFunction)}
     * with identity function as the key extractor and constructor of
     * {@code SimpleImmutableEntry} as the finisher function, which means the
     * processor emits items of type {@code java.util.Map.Entry<T, A>}.
     *
     * @param accumulator accumulates the result value across all entries under the same key
     * @param <T> type of item on the inbound edge
     * @param <A> type of the accumulated value
     */
    public static <T, A> ProcessorSupplier groupAndAccumulate(
            Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(x -> x, accumulator);
    }

    /**
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     Stores the result of applying the accumulator function to the previously
     *     accumulated value and the current item. The initial accumulated value is
     *     {@code null}.
     * </li><li>
     *     When all the input is consumed, emits the accumulated result.
     * </li><li>
     *     Emits an item of type {@code R} obtained by applying the finisher function
     *     to the accumulated value.
     * </li></ul>
     *
     * @param accumulator accumulates the result value across all the input items
     * @param <T> type of input item
     * @param <A> type of the accumulated value
     * @param <R> type of the emitted item
     */
    public static <T, A, R> ProcessorSupplier accumulate(
            Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
            Distributed.Function<? super A, ? extends R> finisher
    ) {
        return groupAndAccumulate(x -> true, accumulator, (dummyTrueBoolean, a) -> finisher.apply(a));
    }

    /**
     * Convenience over {@link #accumulate(BiFunction, Function)} with identity function
     * as the finisher, which means the processor emits an item of type {@code A}.
     *
     * @param accumulator accumulates the result value across all the input items
     * @param <T> type of input item
     * @param <A> type of the accumulated value
     */
    public static <T, A> ProcessorSupplier accumulate(
            Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(x -> true, accumulator, (dummyTrueBoolean, a) -> a);
    }

    /**
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     Computes the key of type {@code K} by applying the key extractor
     *     to the item.
     * </li><li>
     *     Maintains a set of all seen keys.
     * </li><li>
     *     Emits the size of the set (the number of seen distinct keys) as a
     *     {@code Long} value.
     * </li></ul>
     */
    public static <T, K> ProcessorSupplier countDistinct(Distributed.Function<T, K> keyExtractor) {
        return ProcessorSupplier.of(() -> new DistinctCounterP<>(keyExtractor));
    }

    /**
     * Convenience over {@link #countDistinct(Function)} with identity function
     * as the key extractor, which means the processor will emit the number of
     * distinct items it has seen in the input.
     *
     * @param <T> the input item type.
     */
    public static <T> ProcessorSupplier countDistinct() {
        return ProcessorSupplier.of(() -> new DistinctCounterP<>(x -> x));
    }

    /**
     * A producer that won't produce any data items.
     */
    public static class NoopProducer extends AbstractProducer {
    }

    static class GroupingAccumulatorP<T, K, A, R> extends AbstractProcessor {
        private final Function<? super T, ? extends K> keyExtractor;
        private BiFunction<? super A, ? super T, ? extends A> accumulator;
        private Map<K, A> cache = new HashMap<>();
        private Supplier<R> cacheEntrySupplier;

        GroupingAccumulatorP(Distributed.Function<? super T, ? extends K> keyExtractor,
                             Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
                             Distributed.BiFunction<? super K, ? super A, ? extends R> finisher) {
            this.keyExtractor = keyExtractor;
            this.accumulator = accumulator;
            this.cacheEntrySupplier = map(
                    lazyIterate(() -> cache.entrySet().iterator()),
                    entry -> finisher.apply(entry.getKey(), entry.getValue()));
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            cache.compute(
                    keyExtractor.apply((T) item),
                    (x, a) -> accumulator.apply(a, (T) item));
            return true;
        }

        @Override
        public boolean complete() {
            final boolean done = emitCooperatively(cacheEntrySupplier);
            if (done) {
                cache = null;
                accumulator = null;
                cacheEntrySupplier = null;
            }
            return done;
        }
    }

    static class DistinctCounterP<I, K> extends AbstractProcessor {
        private Distributed.Function<I, K> extractKey;
        private Set<K> seenItems = new HashSet<>();

        DistinctCounterP(Distributed.Function<I, K> extractKey) {
            this.extractKey = extractKey;
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            assert ordinal == 0;
            seenItems.add(extractKey.apply((I) item));
            return true;
        }

        @Override
        public boolean complete() {
            if (!seenItems.isEmpty()) {
                emit((long) seenItems.size());
            }
            extractKey = null;
            seenItems = null;
            return true;
        }
    }
}
