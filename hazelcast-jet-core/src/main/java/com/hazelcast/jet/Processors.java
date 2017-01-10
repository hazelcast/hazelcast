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
import com.hazelcast.jet.Traversers.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.connector.AbstractProducer;
import com.hazelcast.jet.impl.connector.IListReader;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.jet.impl.connector.IMapWriter;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.Distributed.BiFunction;
import com.hazelcast.jet.stream.Distributed.Function;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;

/**
 * Static utility class with factory methods for predefined processors.
 */
public final class Processors {

    private Processors() {
    }

    /**
     * Returns a meta-supplier of processors that will fetch entries from the
     * Hazelcast {@code IMap} with the specified name. The processors will only
     * access data local to the member and, if {@code localParallelism} for the
     * vertex is above one, processors will divide the labor within the member
     * so that each one gets a subset of all local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     */
    public static ProcessorMetaSupplier mapReader(String mapName) {
        return IMapReader.supplier(mapName);
    }

    /**
     * Returns a meta-supplier of processors that will fetch entries from a
     * Hazelcast {@code IMap} in a remote cluster.
     */
    public static ProcessorMetaSupplier mapReader(String mapName, ClientConfig clientConfig) {
        return IMapReader.supplier(mapName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processors that will put data into a Hazelcast {@code IMap}.
     */
    public static ProcessorMetaSupplier mapWriter(String mapName) {
        return IMapWriter.supplier(mapName);
    }

    /**
     * Returns a meta-supplier of processors that will put data into a Hazelcast {@code IMap} in
     * a remote cluster.
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
     * Returns a supplier of processor which, for each received item, emits the result
     * of applying the given mapping function to it. If the result is {@code null},
     * nothing will be emitted. Therefore this processor can be used to implement
     * filtering semantics as well.
     *
     * @param mapper the mapping function
     * @param <T> type of received item
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, R> ProcessorSupplier map(@Nonnull Distributed.Function<? super T, ? extends R> mapper) {
        return ProcessorSupplier.of(() -> {
            final ResettableSingletonTraverser<R> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, R>(item -> {
                trav.item = mapper.apply(item);
                return trav;
            });
        });
    }

    /**
     * Returns a supplier of processor which emits the same items it receives,
     * but only those that pass the given predicate.
     *
     * @param predicate the predicate to test each received item aginst
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> ProcessorSupplier filter(@Nonnull Distributed.Predicate<? super T> predicate) {
        return ProcessorSupplier.of(() -> {
            final ResettableSingletonTraverser<T> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, T>(item -> {
                trav.item = predicate.test(item) ? item : null;
                return trav;
            });
        });
    }

    /**
     * Returns a supplier of {@link TransformP} processors with the given
     * item-to-traverser function.
     *
     * @param mapper function that maps the received item to a traverser over output items
     * @param <T> received item type
     * @param <R> emitted item type
     */
    @Nonnull
    public static <T, R> ProcessorSupplier flatMap(
            @Nonnull Distributed.Function<? super T, ? extends Traverser<? extends R>> mapper
    ) {
        return ProcessorSupplier.of(() -> new TransformP<T, R>(mapper));
    }

    /**
     * Returns a supplier of {@link GroupAndAccumulateP} processors.
     *
     * @param keyExtractor computes the key from the entry
     * @param accumulator accumulates the result value across all entries under the same key
     *
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, K, A, R> ProcessorSupplier groupAndAccumulate(
            @Nonnull Distributed.Function<? super T, ? extends K> keyExtractor,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
            @Nonnull Distributed.BiFunction<? super K, ? super A, ? extends R> finisher
    ) {
        return ProcessorSupplier.of(() -> new GroupAndAccumulateP<>(keyExtractor, accumulator, finisher));
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
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier groupAndAccumulate(
            @Nonnull Distributed.Function<? super T, ?> keyExtractor,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return ProcessorSupplier.of(() -> new GroupAndAccumulateP<>(
                keyExtractor, accumulator, SimpleImmutableEntry::new));
    }

    /**
     * Convenience over {@link #groupAndAccumulate(Function, BiFunction, BiFunction)}
     * with identity function as the key extractor and constructor of
     * {@code SimpleImmutableEntry} as the finisher function, which means the
     * processor emits items of type {@code java.util.Map.Entry<T, A>}.
     *
     * @param accumulator accumulates the result value across all entries under the same key
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier groupAndAccumulate(
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
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
     * @param finisher transforms the accumulated value to the item to emit
     *
     * @param <T> type of received item
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, A, R> ProcessorSupplier accumulate(
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
            @Nonnull Distributed.Function<? super A, ? extends R> finisher
    ) {
        return groupAndAccumulate(x -> true, accumulator, (dummyTrueBoolean, a) -> finisher.apply(a));
    }

    /**
     * Convenience over {@link #accumulate(BiFunction, Function)} with identity function
     * as the finisher, which means the processor emits an item of type {@code A}.
     *
     * @param accumulator accumulates the result value across all the input items
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier accumulate(
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(x -> true, accumulator, (dummyTrueBoolean, a) -> a);
    }

    /**
     * Returns a supplier of {@link CountDistinctP} processors.
     *
     * @param keyExtractor the key extractor function
     * @param <T> received item type
     * @param <K> key type
     */
    @Nonnull
    public static <T, K> ProcessorSupplier countDistinct(@Nonnull Distributed.Function<T, K> keyExtractor) {
        return ProcessorSupplier.of(() -> new CountDistinctP<>(keyExtractor));
    }

    /**
     * Convenience over {@link #countDistinct(Function)} with identity function
     * as the key extractor, which means the processor will emit the number of
     * distinct items it has seen in the input.
     *
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> ProcessorSupplier countDistinct() {
        return ProcessorSupplier.of(() -> new CountDistinctP<>(x -> x));
    }

    /**
     * A producer that won't produce any data items.
     */
    public static class NoopProducer extends AbstractProducer {
    }

    /**
     * Processor which, for each received item, emits all the items from the
     * traverser returned by the given item-to-traverser function.
     *
     * @param <T> received item type
     * @param <R> emitted item type
     */
    public static class TransformP<T, R> extends AbstractProcessor {
        private TryProcessor<T, R> tryProcessor;

        /**
         * Constructs a processor with the given mapping function.
         */
        public TransformP(@Nonnull  Distributed.Function<? super T, ? extends Traverser<? extends R>> mapper) {
            this.tryProcessor = tryProcessor(mapper);
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            return tryProcessor.tryProcess((T) item);
        }

        @Override
        public boolean complete() {
            tryProcessor = null;
            return true;
        }
    }

    /**
     * Processor with the following semantics:
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
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    public static class GroupAndAccumulateP<T, K, A, R> extends AbstractProcessor {
        private final Function<? super T, ? extends K> keyExtractor;
        private BiFunction<? super A, ? super T, ? extends A> accumulator;
        private Map<K, A> groups = new HashMap<>();
        private Traverser<R> resultTraverser;

        /**
         * Creates the processor from the given key extractor, accumulator, and finisher
         * functions.
         */
        public GroupAndAccumulateP(@Nonnull Distributed.Function<? super T, ? extends K> keyExtractor,
                                   @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
                                   @Nonnull Distributed.BiFunction<? super K, ? super A, ? extends R> finisher) {
            this.keyExtractor = keyExtractor;
            this.accumulator = accumulator;
            this.resultTraverser = lazy(() -> traverseStream(groups
                    .entrySet().stream()
                    .map(entry -> finisher.apply(entry.getKey(), entry.getValue()))
            ));
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            groups.compute(
                    keyExtractor.apply((T) item),
                    (x, a) -> accumulator.apply(a, (T) item));
            return true;
        }

        @Override
        public boolean complete() {
            final boolean done = emitCooperatively(resultTraverser);
            if (done) {
                groups = null;
                accumulator = null;
                resultTraverser = null;
            }
            return done;
        }
    }

    /**
     * Processor with the following semantics:
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
     *
     * @param <T> type of received item
     * @param <K> type of grouping key
     */
    public static class CountDistinctP<T, K> extends AbstractProcessor {
        private Distributed.Function<T, K> extractKey;
        private Set<K> seenItems = new HashSet<>();

        /**
         * Constructs the processor with the given key extractor function.
         */
        public CountDistinctP(@Nonnull Distributed.Function<T, K> extractKey) {
            this.extractKey = extractKey;
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            assert ordinal == 0;
            seenItems.add(extractKey.apply((T) item));
            return true;
        }

        @Override
        public boolean complete() {
            emit((long) seenItems.size());
            extractKey = null;
            seenItems = null;
            return true;
        }
    }
}
