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
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.ReadFileP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadSocketTextStreamP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.WriteBufferedP;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Static utility class with factory methods for predefined processors.
 */
public final class Processors {

    private Processors() {
    }

    /**
     * Returns a meta-supplier of processors that will fetch entries from the
     * Hazelcast {@code IMap} with the specified name and will emit them as {@code Map.Entry}.
     * The processors will only access data local to the member and,
     * if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that
     * each one gets a subset of all local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * If the underlying map is concurrently being modified, there are no guarantees
     * given with respect to missing or duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName) {
        return ReadWithPartitionIteratorP.readMap(mapName);
    }

    /**
     * Returns a meta-supplier of processors that will fetch entries from a
     * Hazelcast {@code IMap} in a remote cluster.
     * Processors will emit the entries as {@code Map.Entry}.
     * <p>
     * If the underlying map is concurrently being modified, there are no guarantees
     * given with respect to missing or duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readMap(mapName, clientConfig);
    }

    /**
     * Returns a processor supplier that will put data into a Hazelcast {@code IMap}.
     * Processors expect items of type {@code Map.Entry}.
     */
    @Nonnull
    public static ProcessorSupplier writeMap(@Nonnull String mapName) {
        return HazelcastWriters.writeMap(mapName);
    }

    /**
     * Returns a processor supplier that will put data into a Hazelcast {@code IMap} in
     * a remote cluster.
     * Processors expect items of type {@code Map.Entry}.
     */
    @Nonnull
    public static ProcessorSupplier writeMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeMap(mapName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processors that will fetch entries from the
     * Hazelcast {@code ICache} with the specified name and will emit them as {@code Cache.Entry}.
     * The processors will only access data local to the member and,
     * if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that
     * each one gets a subset of all local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * If the underlying cache is concurrently being modified, there are no guarantees
     * given with respect to missing or duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCache(cacheName);
    }

    /**
     * Returns a meta-supplier of processors that will fetch entries from a
     * Hazelcast {@code ICache} in a remote cluster.
     * Processors will emit the entries as {@code Cache.Entry}.
     * <p>
     * If the underlying cache is concurrently being modified, there are no guarantees
     * given with respect to missing or duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readCache(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors that will put data into a Hazelcast {@code ICache}.
     * Processors expect items of type {@code Map.Entry}
     */
    @Nonnull
    public static ProcessorSupplier writeCache(@Nonnull String cacheName) {
        return HazelcastWriters.writeCache(cacheName);
    }

    /**
     * Returns a processor supplier that will put data into a Hazelcast {@code ICache} in
     * a remote cluster.
     * Processors expect items of type {@code Map.Entry}.
     */
    @Nonnull
    public static ProcessorSupplier writeCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeCache(cacheName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processors that emit items retrieved from an IMDG IList.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName) {
        return ReadIListP.supplier(listName);
    }

    /**
     * Returns a meta-supplier of processors that emit items retrieved from an IMDG IList
     * in a remote cluster.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.supplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors that write received items to an IMDG IList.
     */
    @Nonnull
    public static ProcessorSupplier writeList(@Nonnull String listName) {
        return HazelcastWriters.writeList(listName);
    }

    /**
     * Returns a supplier of processors that write received items to an IMDG IList in
     * a remote cluster.
     */
    @Nonnull
    public static ProcessorSupplier writeList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeList(listName, clientConfig);
    }


    /**
     * Returns a supplier of processor which drains all items from
     * an inbox to a intermediate buffer and then flushes that buffer.
     *
     * Useful for implementing sinks where the data should only be flushed
     * after the inbox has been drained.
     *
     * @param newBuffer supplies the buffer
     * @param addToBuffer adds item to buffer
     * @param flushBuffer flushes the buffer
     * @param <B> type of buffer
     * @param <T> type of received item
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBuffered(@Nonnull Distributed.Supplier<B> newBuffer,
                                                         @Nonnull Distributed.BiConsumer<B, T> addToBuffer,
                                                         @Nonnull Distributed.Consumer<B> flushBuffer) {
        return WriteBufferedP.writeBuffered(newBuffer, addToBuffer, flushBuffer, noopConsumer());
    }

    /**
     * Returns a supplier of processor which drains all items from
     * an inbox to a intermediate buffer and then flushes that buffer.
     *
     * The buffer will be disposed via {@code disposeBuffer} once the processor
     * is completed
     * Useful for implementing sinks where the data should only be flushed
     * after the inbox has been drained.
     *
     * @param newBuffer supplies the buffer
     * @param addToBuffer adds item to buffer
     * @param flushBuffer flushes the buffer
     * @param disposeBuffer disposes of the buffer
     * @param <B> type of buffer
     * @param <T> type of received item
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBuffered(@Nonnull Distributed.Supplier<B> newBuffer,
                                                         @Nonnull Distributed.BiConsumer<B, T> addToBuffer,
                                                         @Nonnull Distributed.Consumer<B> flushBuffer,
                                                         @Nonnull Distributed.Consumer<B> disposeBuffer) {
        return WriteBufferedP.writeBuffered(newBuffer, addToBuffer, flushBuffer, disposeBuffer);
    }

    /**
     * Returns a supplier of processors that connect to specified socket and write the items as text
     */
    public static ProcessorSupplier writeSocket(@Nonnull String host, int port) {
        return writeBuffered(
                () -> createBufferedWriter(host, port),
                (bufferedWriter, item) -> uncheckRun(() -> bufferedWriter.write(item.toString())),
                bufferedWriter -> uncheckRun(bufferedWriter::flush),
                bufferedWriter -> uncheckRun(bufferedWriter::close)
        );
    }

    private static BufferedWriter createBufferedWriter(@Nonnull String host, int port) {
        return uncheckCall(() -> new BufferedWriter(new OutputStreamWriter(new Socket(host, port).getOutputStream(), "UTF-8")));
    }

    /**
     * Returns a supplier of processors that connect to the specified socket and read and emit text line by line.
     */
    @Nonnull
    public static ProcessorSupplier readSocket(@Nonnull String host, int port) {
        return ReadSocketTextStreamP.supplier(host, port);
    }

    /**
     * Returns a supplier of processors, that read all files in a directory and emit them
     * line by line. Files should be in UTF-8 encoding.
     *
     * @param directory Parent directory of the files.
     *
     * @see #readFile(String, Charset, String)
     */
    @Nonnull
    public static ProcessorSupplier readFile(@Nonnull String directory) {
        return readFile(directory, StandardCharsets.UTF_8, null);
    }

    /**
     * Returns a supplier of source processor designed to process files in a directory
     * in a batch. It processes all files in a directory (optionally filtering with a
     * {@code glob}). Contents of the files are emitted line by line. There is no
     * indication, which file a particular line comes from. Contents of subdirectories
     * are not processed.
     * <p>
     * The same directory must be available on all members, but it should not
     * contain the same files (i.e. it should not be a network shared directory, but
     * files local to the machine).
     * <p>
     * If directory contents are changed while processing, the behavior is
     * undefined: the changed contents might or might not be processed.
     *
     * @param directory Parent directory of the files.
     * @param charset Character set used when reading the files. If null, utf-8 is used.
     * @param glob The filtering pattern, see
     *          {@link java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}
     */
    @Nonnull
    public static ProcessorSupplier readFile(@Nonnull String directory, @Nullable Charset charset,
            @Nullable String glob) {
        return ReadFileP.supplier(directory, charset == null ? "utf-8" : charset.name(), glob);
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
     * Returns a supplier of {@link TransformP} processor with the given
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
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     Applies the key extractor to each item and obtains the key of type {@code K}.
     * </li><li>
     *     Stores for each key the result of applying the accumulator function to
     *     the previously accumulated value and the current item. The initial
     *     accumulated value is obtained from the supplier function.
     * </li><li>
     *     When all the input is consumed, begins emitting the accumulated results.
     * </li><li>
     *     Emits items of type {@code R} obtained by applying the finisher function
     *     to each seen key and its accumulated value.
     * </li></ul>
     *
     * @param keyExtractor computes the key from the entry
     * @param supplier supplies the initial accumulated value
     * @param accumulator accumulates the result value across all entries under the same key
     * @param finisher transforms a key and its accumulated value into the item to emit
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, K, A, R> ProcessorSupplier groupAndAccumulate(
            @Nonnull Distributed.Function<? super T, ? extends K> keyExtractor,
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
            @Nonnull Distributed.BiFunction<? super K, ? super A, ? extends R> finisher
    ) {
        return ProcessorSupplier.of(() -> new GroupAndAccumulateP<>(keyExtractor, supplier, accumulator, finisher));
    }

    /**
     * Convenience over {@link #groupAndAccumulate(Distributed.Function, Distributed.Supplier,
     * Distributed.BiFunction, Distributed.BiFunction) groupAndAccumulate(keyExtractor,
     * supplier, accumulator, finisher)} with the constructor of
     * {@code SimpleImmutableEntry} as the finisher function, which means the
     * processor emits items of type {@code java.util.Map.Entry<K, A>}. Note that
     * {@code K} isn't a part of the method's signature since nothing in the
     * processor depends on it. The receiving processor will in any case have to
     * perform an unchecked cast to {@code Entry<K, A>}.
     *
     * @param keyExtractor computes the key from the entry
     * @param supplier supplies the initial accumulated value
     * @param accumulator accumulates the result value across all entries under the same key
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier groupAndAccumulate(
            @Nonnull Distributed.Function<? super T, ?> keyExtractor,
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(keyExtractor, supplier, accumulator, Util::entry);
    }

    /**
     * Convenience over {@link #groupAndAccumulate(Distributed.Function, Distributed.Supplier,
     * Distributed.BiFunction, Distributed.BiFunction)
     * groupAndAccumulate(keyExtractor, supplier, accumulator, finisher)} with identity
     * function as the key extractor and constructor of {@code SimpleImmutableEntry}
     * as the finisher function, which means the processor emits items of type
     * {@code java.util.Map.Entry<T, A>}.
     *
     * @param supplier supplies the initial accumulated value
     * @param accumulator accumulates the result value across all entries under the same key
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier groupAndAccumulate(
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(Distributed.Function.identity(), supplier, accumulator);
    }

    /**
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     Applies the key extractor to each item and obtains the key of type {@code K}.
     * </li><li>
     *     Stores for each seen key an accumulated value container obtained from the
     *     supplier function.
     * </li><li>
     *     For each received item, applies the collector function to the accumulated
     *     value container and the item.
     * </li><li>
     *     When all the input is consumed, begins emitting the accumulated results.
     * </li><li>
     *     Emits items of type {@code R} obtained by applying the finisher function
     *     to each seen key and its accumulated value.
     * </li></ul>
     *
     * @param keyExtractor computes the key from the entry
     * @param supplier supplies the mutable result container
     * @param collector collects the results of all entries under the same key
     *                  into the mutable container
     * @param finisher transforms a key and its result container into the item to emit
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, K, A, R> ProcessorSupplier groupAndCollect(
            @Nonnull Distributed.Function<? super T, ? extends K> keyExtractor,
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiConsumer<? super A, ? super T> collector,
            @Nonnull Distributed.BiFunction<? super K, ? super A, ? extends R> finisher
    ) {
        return ProcessorSupplier.of(() -> new GroupAndCollectP<>(keyExtractor, supplier, collector, finisher));
    }

    /**
     * Convenience over {@link #groupAndCollect(Distributed.Function, Distributed.Supplier,
     * Distributed.BiConsumer, Distributed.BiFunction) groupAndCollect(keyExtractor,
     * supplier, collector, finisher)} with the constructor of
     * {@code SimpleImmutableEntry} as the finisher function, which means the
     * processor emits items of type {@code java.util.Map.Entry<K, A>}. Note that
     * {@code K} isn't a part of the method's signature since nothing in the
     * processor depends on it. The receiving processor will in any case have to
     * perform an unchecked cast to {@code Entry<K, A>}.
     *
     * @param keyExtractor computes the key from the entry
     * @param supplier supplies the mutable result container
     * @param collector collects the results of all entries under the same key
     *                  into the mutable container
     * @param <T> type of received item
     * @param <A> type of result container
     */
    @Nonnull
    public static <T, A> ProcessorSupplier groupAndCollect(
            @Nonnull Distributed.Function<? super T, ?> keyExtractor,
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiConsumer<? super A, ? super T> collector
    ) {
        return groupAndCollect(keyExtractor, supplier, collector, Util::entry);
    }

    /**
     * Convenience over {@link #groupAndCollect(Distributed.Function, Distributed.Supplier,
     * Distributed.BiConsumer, Distributed.BiFunction)
     * groupAndCollect(keyExtractor, supplier, collector, finisher)} with identity
     * function as the key extractor and constructor of {@code SimpleImmutableEntry}
     * as the finisher function, which means the processor emits items of type
     * {@code java.util.Map.Entry<T, A>}.
     *
     * @param supplier supplies the mutable result container
     * @param collector collects the results of all entries under the same key
     *                  into the mutable container
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier groupAndCollect(
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiConsumer<? super A, ? super T> collector
    ) {
        return groupAndCollect(Distributed.Function.identity(), supplier, collector);
    }

    /**
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Calls the {@code supplier} function to obtain the initial accumulated value.
     * </li><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     Stores the result of applying the {@code accumulator} function to the previously
     *     accumulated value and the current item.
     * </li><li>
     *     When all the input is consumed, emits the result as a single item of type
     *     {@code R}, obtained by applying the {@code finisher} function to the
     *     accumulated value.
     * </li></ul>
     *
     * @param supplier supplies the initial accumulated value
     * @param accumulator accumulates the result value across all the input items
     * @param finisher transforms the accumulated value into the item to emit
     * @param <T> type of received item
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, A, R> ProcessorSupplier accumulate(
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator,
            @Nonnull Distributed.Function<? super A, ? extends R> finisher
    ) {
        return groupAndAccumulate(x -> true, supplier, accumulator, (dummyTrueBoolean, a) -> finisher.apply(a));
    }

    /**
     * Convenience over {@link #accumulate(Distributed.Supplier, Distributed.BiFunction,
     * Distributed.Function) accumulate(supplier, accumulator, finisher)}
     * with identity function as the finisher, which means the processor emits an
     * item of type {@code A}.
     *
     * @param supplier supplies the initial accumulated value
     * @param accumulator accumulates the result value across all the input items
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> ProcessorSupplier accumulate(
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(x -> true, supplier, accumulator, (dummyTrueBoolean, a) -> a);
    }

    /**
     * Returns a supplier of processor with the following semantics:
     * <ul><li>
     *     Calls the {@code supplier} function to obtain the mutable result container.
     * </li><li>
     *     Accepts items of type {@code T}.
     * </li><li>
     *     For each received item, calls the {@code collector} function with the
     *     result container and the current item.
     * </li><li>
     *     When all the input is consumed, emits the result as a single item of type
     *     {@code R}, obtained by applying the {@code finisher} function to the
     *     result container.
     * </li></ul>
     *
     * @param supplier supplies the mutable result container
     * @param collector collects the result across all the input items
     *                  into the result container
     * @param finisher transforms the result container into the item to emit
     * @param <T> type of received item
     * @param <A> type of accumulated value
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, A, R> ProcessorSupplier collect(
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiConsumer<? super A, ? super T> collector,
            @Nonnull Distributed.Function<? super A, ? extends R> finisher
    ) {
        return groupAndCollect(x -> true, supplier, collector, (dummyTrueBoolean, a) -> finisher.apply(a));
    }

    /**
     * Convenience over {@link #collect(Distributed.Supplier, Distributed.BiConsumer,
     * Distributed.Function) collect(supplier, collector, finisher)} with
     * identity function as the finisher, which means the processor emits an
     * item of type {@code A}.
     *
     * @param supplier supplies the mutable result container
     * @param collector collects the result across all the input items
     *                  into the result container
     * @param <T> type of received item
     * @param <A> type of result container
     */
    @Nonnull
    public static <T, A> ProcessorSupplier collect(
            @Nonnull Distributed.Supplier<? extends A> supplier,
            @Nonnull Distributed.BiConsumer<? super A, ? super T> collector
    ) {
        return groupAndCollect(x -> true, supplier, collector, (dummyTrueBoolean, a) -> a);
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
     * Convenience over {@link #countDistinct(Distributed.Function)} with identity
     * function as the key extractor, which means the processor will emit the number
     * of distinct items it has seen in the input.
     *
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> ProcessorSupplier countDistinct() {
        return ProcessorSupplier.of(() -> new CountDistinctP<>(x -> x));
    }

    /**
     * Decorates a {@code ProcessorSupplier} into one that will declare all its
     * processors non-cooperative. The wrapped supplier must return processors
     * that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static ProcessorSupplier nonCooperative(ProcessorSupplier wrapped) {
        return count -> {
            final Collection<? extends Processor> ps = wrapped.get(count);
            ps.forEach(p -> ((AbstractProcessor) p).setCooperative(false));
            return ps;
        };
    }

    /**
     * A processor that does nothing.
     */
    public static class NoopP implements Processor {
    }

    /**
     * Processor which, for each received item, emits all the items from the
     * traverser returned by the given item-to-traverser function.
     *
     * @param <T> received item type
     * @param <R> emitted item type
     */
    private static class TransformP<T, R> extends AbstractProcessor {
        private final FlatMapper<T, R> flatMapper;

        /**
         * Constructs a processor with the given mapping function.
         */
        TransformP(@Nonnull Distributed.Function<? super T, ? extends Traverser<? extends R>> mapper) {
            this.flatMapper = flatMapper(mapper);
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            return flatMapper.tryProcess((T) item);
        }
    }

    private abstract static class ReducingProcessorBase<T, K, A, R> extends AbstractProcessor {
        final Function<? super T, ? extends K> keyExtractor;
        final Supplier<? extends A> supplier;
        final Map<K, A> groups = new HashMap<>();
        final Traverser<R> resultTraverser;

        ReducingProcessorBase(@Nonnull Function<? super T, ? extends K> keyExtractor,
                              @Nonnull Supplier<? extends A> supplier,
                              @Nonnull BiFunction<? super K, ? super A, ? extends R> finisher
        ) {
            this.keyExtractor = keyExtractor;
            this.supplier = supplier;
            this.resultTraverser = lazy(() -> traverseStream(groups
                    .entrySet().stream()
                    .map(entry -> finisher.apply(entry.getKey(), entry.getValue()))
            ));
        }

        @Override
        public boolean complete() {
            return emitCooperatively(resultTraverser);
        }
    }

    private static class GroupAndAccumulateP<T, K, A, R> extends ReducingProcessorBase<T, K, A, R> {
        private final BiFunction<? super A, ? super T, ? extends A> accumulator;

        GroupAndAccumulateP(@Nonnull Function<? super T, ? extends K> keyExtractor,
                            @Nonnull Supplier<? extends A> supplier,
                            @Nonnull BiFunction<? super A, ? super T, ? extends A> accumulator,
                            @Nonnull BiFunction<? super K, ? super A, ? extends R> finisher
        ) {
            super(keyExtractor, supplier, finisher);
            this.accumulator = accumulator;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            groups.compute(
                    keyExtractor.apply((T) item),
                    (x, a) -> accumulator.apply(a != null ? a : supplier.get(), (T) item));
            return true;
        }
    }

    private static class GroupAndCollectP<T, K, A, R> extends ReducingProcessorBase<T, K, A, R> {
        private final BiConsumer<? super A, ? super T> collector;

        GroupAndCollectP(@Nonnull Function<? super T, ? extends K> keyExtractor,
                         @Nonnull Supplier<? extends A> supplier,
                         @Nonnull BiConsumer<? super A, ? super T> collector,
                         @Nonnull BiFunction<? super K, ? super A, ? extends R> finisher
        ) {
            super(keyExtractor, supplier, finisher);
            this.collector = collector;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            final A acc = groups.computeIfAbsent(keyExtractor.apply((T) item), k -> supplier.get());
            collector.accept(acc, (T) item);
            return true;
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
    private static class CountDistinctP<T, K> extends AbstractProcessor {
        private final Distributed.Function<T, K> extractKey;
        private final Set<K> seenItems = new HashSet<>();

        /**
         * Constructs the processor with the given key extractor function.
         */
        CountDistinctP(@Nonnull Distributed.Function<T, K> extractKey) {
            this.extractKey = extractKey;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            assert ordinal == 0;
            seenItems.add(extractKey.apply((T) item));
            return true;
        }

        @Override
        public boolean complete() {
            emit((long) seenItems.size());
            return true;
        }
    }
}
