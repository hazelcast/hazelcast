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
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamTextSocketP;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import com.hazelcast.jet.impl.connector.WriteFileP;
import com.hazelcast.jet.impl.connector.WriteSystemOutP;
import com.hazelcast.jet.impl.util.PeekWrappedP;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.WrappingProcessorSupplier;

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

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
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
     * <p>
     * Note, that all elements from the list are emitted on single member, as the IMDG's list
     * structure isn't distributed.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName) {
        return ReadIListP.supplier(listName);
    }

    /**
     * Returns a meta-supplier of processors that emit items retrieved from an IMDG IList
     * in a remote cluster.
     * <p>
     * Note, that all elements from the list are emitted on single member, as the IMDG's list
     * structure isn't distributed.
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
     *  @param <B> type of buffer
     * @param <T> type of received item
     * @param newBuffer supplies the buffer. Supplier argument is the global processor index
     * @param addToBuffer adds item to buffer
     * @param flushBuffer flushes the buffer
     */
    @Nonnull
    public static <B, T> DistributedSupplier<Processor> writeBuffered(
            @Nonnull DistributedIntFunction<B> newBuffer,
            @Nonnull DistributedBiConsumer<B, T> addToBuffer,
            @Nonnull DistributedConsumer<B> flushBuffer
    ) {
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
     *  @param <B> type of buffer
     * @param <T> type of received item
     * @param newBuffer supplies the buffer. Supplier argument is the global processor index
     * @param addToBuffer adds item to buffer
     * @param flushBuffer flushes the buffer
     * @param disposeBuffer disposes of the buffer
     */
    @Nonnull
    public static <B, T> DistributedSupplier<Processor> writeBuffered(
            @Nonnull DistributedIntFunction<B> newBuffer,
            @Nonnull DistributedBiConsumer<B, T> addToBuffer,
            @Nonnull DistributedConsumer<B> flushBuffer,
            @Nonnull DistributedConsumer<B> disposeBuffer
    ) {
        return WriteBufferedP.writeBuffered(newBuffer, addToBuffer, flushBuffer, disposeBuffer);
    }

    /**
     * Returns a supplier of processors that connect to specified socket and write the items as text
     */
    public static DistributedSupplier<Processor> writeSocket(@Nonnull String host, int port) {
        return writeBuffered(
                index -> createBufferedWriter(host, port),
                (bufferedWriter, item) -> uncheckRun(() -> bufferedWriter.write(item.toString())),
                bufferedWriter -> uncheckRun(bufferedWriter::flush),
                bufferedWriter -> uncheckRun(bufferedWriter::close)
        );
    }

    private static BufferedWriter createBufferedWriter(@Nonnull String host, int port) {
        return uncheckCall(
                () -> new BufferedWriter(new OutputStreamWriter(new Socket(host, port).getOutputStream(), "UTF-8")));
    }

    /**
     * Create processor with UTF-8 character set.
     * @see #streamTextSocket(String, int, Charset)
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamTextSocket(@Nonnull String host, int port) {
        return streamTextSocket(host, port, StandardCharsets.UTF_8);
    }

    /**
     * A reader that connects to a specified socket and reads and emits text line by line.
     * This processor expects a server-side socket to be available to connect to.
     * <p>
     * Each processor instance will create a socket connection to the configured [host:port],
     * so there will be {@code clusterSize * localParallelism} connections. The server
     * should do the load-balancing.
     * <p>
     * The processor will complete, when the socket is closed by the server. No reconnection
     * is attempted.
     *
     * @param host The host name to connect to
     * @param port The port number to connect to
     * @param charset Character set used to decode the stream
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamTextSocket(@Nonnull String host, int port,
                                                                  @Nonnull Charset charset
    ) {
        return StreamTextSocketP.supplier(host, port, charset.name());
    }

    /**
     * Convenience for {@link #readFiles(String, Charset, String)} with default
     * charset (UTF-8) and default glob (match all files).
     */
    @Nonnull
    public static ProcessorSupplier readFiles(@Nonnull String directory) {
        return readFiles(directory, StandardCharsets.UTF_8, "*");
    }

    /**
     * A processor that emits lines from all files in a directory (but not its
     * subdirectories), or only the files matching the supplied {@code glob}
     * expression. The files must not change while being read; if they do, the
     * behavior is unspecified. There will be no indication which file a
     * particular line comes from.
     * <p>
     * The same pathname should be available on all members, but it should not
     * contain the same files &mdash; (e.g., it shouldn't resolve to a
     * directory shared over the network).
     *
     * @param directory parent directory of the files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all files.
     */
    @Nonnull
    public static ProcessorSupplier readFiles(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return ReadFilesP.supplier(directory, charset.name(), glob);
    }

    /**
     * Convenience for {@link #streamFiles(String, Charset, String)} with
     * default charset (UTF-8) and default glob (match all files).
     */
    public static ProcessorSupplier streamFiles(@Nonnull String watchedDirectory) {
        return streamFiles(watchedDirectory, StandardCharsets.UTF_8, "*");
    }

    /**
     * A source processor that generates a stream of lines of text coming from
     * files in the watched directory (but not its subdirectories). It will
     * pick up both newly created files and content appended to pre-existing
     * files. It processor expects the file content not to change once
     * appended. There will be no indication which file a particular line comes
     * from.
     * <p>
     * The processor will scan pre-existing files for file sizes on startup and
     * process them from that position. It will ignore the first line if the
     * starting offset is not immediately after a newline character (it is
     * assumed that another process is concurrently appending to the file).
     * <p>
     * The same pathname should be available on all members, but it should not
     * contain the same files &mdash; (e.g., it shouldn't resolve to a
     * directory shared over the network).
     * <p>
     * Multiple processor instances read multiple files in parallel. Single
     * file is always read by one processor instance. When a change is detected
     * the file is opened, appended lines are read and file is closed. This
     * process is repeated as necessary.
     * <p>
     * The processor completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * <strong>Note:</strong> the underlying JDK API, {@link
     * java.nio.file.WatchService}, has a history of unreliability and this
     * processor may experience infinite blocking, missed, or duplicate events
     * as a result. Such problems may be resolved by upgrading the JRE to the
     * latest version.
     *
     * @param watchedDirectory The directory where we watch files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all files.
     */
    public static ProcessorSupplier streamFiles(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return StreamFilesP.supplier(watchedDirectory, charset.name(), glob);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all nodes.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeFile(@Nonnull String directoryName) {
        return writeFile(directoryName, null, null, false);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all nodes.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFile(
            @Nonnull String directoryName, @Nullable DistributedFunction<T, String> toStringF
    ) {
        return writeFile(directoryName, toStringF, null, false);
    }

    /**
     * Returns a meta-supplier of a processor that writes all items to a local
     * file on each member. {@code item.toString()} is written to the file,
     * followed by a platform-specific line separator.
     * <p>
     * The same file must be available for writing on all nodes. The file on
     * each node will contain part of the data processed on that member.
     * <p>
     * Since the work of this processor is file IO-intensive, {@link
     * Vertex#localParallelism(int) local parallelism} of the vertex should be
     * set according to the performance characteristics of the underlying
     * storage system. Typical values are in the range of 1 to 4.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all nodes.
     * @param toStringF A function to convert items to String (a formatter), or
     *                {@code null} to use {@code toString()}
     * @param charset charset used to encode the file output, or {@code null}
     *                to use UTF-8
     * @param append whether to append or overwrite the file
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFile(
            @Nonnull String directoryName,
            @Nullable DistributedFunction<T, String> toStringF,
            @Nullable Charset charset,
            boolean append
    ) {
        return WriteFileP.supplier(directoryName, toStringF, charset == null ? null : charset.name(), append);
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
    public static <T, R> DistributedSupplier<Processor> map(
            @Nonnull DistributedFunction<? super T, ? extends R> mapper
    ) {
        return () -> {
            final ResettableSingletonTraverser<R> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, R>(item -> {
                trav.item = mapper.apply(item);
                return trav;
            });
        };
    }

    /**
     * Returns a supplier of processor which emits the same items it receives,
     * but only those that pass the given predicate.
     *
     * @param predicate the predicate to test each received item against
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> filter(@Nonnull DistributedPredicate<? super T> predicate) {
        return () -> {
            final ResettableSingletonTraverser<T> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, T>(item -> {
                trav.item = predicate.test(item) ? item : null;
                return trav;
            });
        };
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
    public static <T, R> DistributedSupplier<Processor> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> mapper
    ) {
        return () -> new TransformP<T, R>(mapper);
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
    public static <T, K, A, R> DistributedSupplier<Processor> groupAndAccumulate(
            @Nonnull DistributedFunction<? super T, ? extends K> keyExtractor,
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiFunction<? super A, ? super T, ? extends A> accumulator,
            @Nonnull DistributedBiFunction<? super K, ? super A, ? extends R> finisher
    ) {
        return () -> new GroupAndAccumulateP<>(keyExtractor, supplier, accumulator, finisher);
    }

    /**
     * Convenience over {@link #groupAndAccumulate(DistributedFunction, DistributedSupplier,
     * DistributedBiFunction, DistributedBiFunction) groupAndAccumulate(keyExtractor,
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
    public static <T, A> DistributedSupplier<Processor> groupAndAccumulate(
            @Nonnull DistributedFunction<? super T, ?> keyExtractor,
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(keyExtractor, supplier, accumulator, Util::entry);
    }

    /**
     * Convenience over {@link #groupAndAccumulate(DistributedFunction, DistributedSupplier,
     * DistributedBiFunction, DistributedBiFunction)
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
    public static <T, A> DistributedSupplier<Processor> groupAndAccumulate(
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiFunction<? super A, ? super T, ? extends A> accumulator
    ) {
        return groupAndAccumulate(DistributedFunction.identity(), supplier, accumulator);
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
    public static <T, K, A, R> DistributedSupplier<Processor> groupAndCollect(
            @Nonnull DistributedFunction<? super T, ? extends K> keyExtractor,
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiConsumer<? super A, ? super T> collector,
            @Nonnull DistributedBiFunction<? super K, ? super A, ? extends R> finisher
    ) {
        return () -> new GroupAndCollectP<>(keyExtractor, supplier, collector, finisher);
    }

    /**
     * Convenience over {@link #groupAndCollect(DistributedFunction, DistributedSupplier,
     * DistributedBiConsumer, DistributedBiFunction) groupAndCollect(keyExtractor,
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
    public static <T, A> DistributedSupplier<Processor> groupAndCollect(
            @Nonnull DistributedFunction<? super T, ?> keyExtractor,
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiConsumer<? super A, ? super T> collector
    ) {
        return groupAndCollect(keyExtractor, supplier, collector, Util::entry);
    }

    /**
     * Convenience over {@link #groupAndCollect(DistributedFunction, DistributedSupplier,
     * DistributedBiConsumer, DistributedBiFunction)
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
    public static <T, A> DistributedSupplier<Processor> groupAndCollect(
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiConsumer<? super A, ? super T> collector
    ) {
        return groupAndCollect(DistributedFunction.identity(), supplier, collector);
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
    public static <T, A, R> DistributedSupplier<Processor> accumulate(
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiFunction<? super A, ? super T, ? extends A> accumulator,
            @Nonnull DistributedFunction<? super A, ? extends R> finisher
    ) {
        return groupAndAccumulate(x -> true, supplier, accumulator, (dummyTrueBoolean, a) -> finisher.apply(a));
    }

    /**
     * Convenience over {@link #accumulate(DistributedSupplier, DistributedBiFunction,
     * DistributedFunction) accumulate(supplier, accumulator, finisher)}
     * with identity function as the finisher, which means the processor emits an
     * item of type {@code A}.
     *
     * @param supplier supplies the initial accumulated value
     * @param accumulator accumulates the result value across all the input items
     * @param <T> type of received item
     * @param <A> type of accumulated value
     */
    @Nonnull
    public static <T, A> DistributedSupplier<Processor> accumulate(
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiFunction<? super A, ? super T, ? extends A> accumulator
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
    public static <T, A, R> DistributedSupplier<Processor> collect(
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiConsumer<? super A, ? super T> collector,
            @Nonnull DistributedFunction<? super A, ? extends R> finisher
    ) {
        return groupAndCollect(x -> true, supplier, collector, (dummyTrueBoolean, a) -> finisher.apply(a));
    }

    /**
     * Convenience over {@link #collect(DistributedSupplier, DistributedBiConsumer,
     * DistributedFunction) collect(supplier, collector, finisher)} with
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
    public static <T, A> DistributedSupplier<Processor> collect(
            @Nonnull DistributedSupplier<? extends A> supplier,
            @Nonnull DistributedBiConsumer<? super A, ? super T> collector
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
    public static <T, K> DistributedSupplier<Processor> countDistinct(@Nonnull DistributedFunction<T, K> keyExtractor) {
        return () -> new CountDistinctP<>(keyExtractor);
    }

    /**
     * Convenience over {@link #countDistinct(DistributedFunction)} with identity
     * function as the key extractor, which means the processor will emit the number
     * of distinct items it has seen in the input.
     */
    @Nonnull
    public static DistributedSupplier<Processor> countDistinct() {
        return () -> new CountDistinctP<>(x -> x);
    }

    /**
     * Decorates a {@code ProcessorSupplier} into one that will declare all its
     * processors non-cooperative. The wrapped supplier must return processors
     * that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static ProcessorSupplier nonCooperative(@Nonnull ProcessorSupplier wrapped) {
        return count -> {
            final Collection<? extends Processor> ps = wrapped.get(count);
            ps.forEach(p -> ((AbstractProcessor) p).setCooperative(false));
            return ps;
        };
    }

    /**
     * Decorates a {@code Supplier<Processor>} into one that will declare
     * its processors non-cooperative. The wrapped supplier must return
     * processors that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static DistributedSupplier<Processor> nonCooperative(@Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> {
            final Processor p = wrapped.get();
            ((AbstractProcessor) p).setCooperative(false);
            return p;
        };
    }

    /**
     * A processor that does nothing. It consumes all input items and is done
     * after that, without emitting anything.
     */
    public static class NoopP implements Processor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            inbox.drain(noopConsumer());
        }
    }

    /**
     * Convenience for {@link #writeSystemOut(DistributedFunction)} without format.
     * It will use {@link Object#toString()}.
     */
    @Nonnull
    public static DistributedSupplier<Processor> writeSystemOut() {
        return writeSystemOut(Object::toString);
    }

    /**
     * Returns a supplier of processors, that sends all events to logger on the
     * INFO level.
     * <p>
     * Note, that the event will be logged on the nodes, not on the client.
     * Useful for testing.
     *
     * @param toStringF Function to convert item to String, if {@code null},
     *                  {@link Object#toString()} is used.
     */
    @Nonnull
    public static DistributedSupplier<Processor> writeSystemOut(
            @Nonnull DistributedFunction<Object, String> toStringF
    ) {
        return () -> new WriteSystemOutP(toStringF);
    }

    /**
     * See {@link #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekInput(@Nonnull DistributedSupplier<Processor> wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * See {@link #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> new PeekWrappedP(wrapped.get(), toStringF, shouldLogF, true, false);
    }

    /**
     * See {@link #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static ProcessorSupplier peekInput(@Nonnull ProcessorSupplier wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * See {@link #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static ProcessorSupplier peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, true, false));
    }

    /**
     * See {@link #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static ProcessorMetaSupplier peekInput(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Returns a supplier, that wraps the provided {@link
     * ProcessorMetaSupplier}, so that all input events are logged, when they
     * are removed from the {@link Inbox}. Events are logged at the INFO level
     * to the following logger: {@link PeekWrappedP}.
     *
     * @param toStringF Function to convert items to String, if {@code null},
     *                  {@link Object#toString()} is used.
     * @param shouldLogF Function to filter logged items. If {@code null}, all
     *                   items are logged. <b>Warning:</b> Function will see
     *                   both items and {@link Punctuation}
     * @param wrapped The wrapped supplier.
     *
     * @see #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)
     */
    @Nonnull
    public static ProcessorMetaSupplier peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, true, false));
    }

    /**
     * See {@link #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekOutput(@Nonnull DistributedSupplier<Processor> wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * See {@link #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> new PeekWrappedP(wrapped.get(), toStringF, shouldLogF, false, true);
    }

    /**
     * See {@link #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static ProcessorSupplier peekOutput(@Nonnull ProcessorSupplier wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * See {@link #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static ProcessorSupplier peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, false, true));
    }

    /**
     * See {@link #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)}
     */
    @Nonnull
    public static ProcessorMetaSupplier peekOutput(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Returns a supplier, that wraps the provided {@link
     * ProcessorMetaSupplier}, so that all output events are logged, when they
     * are accepted by the {@link Outbox}. Events are logged at the INFO level
     * to the following logger: {@link PeekWrappedP}.
     *
     * @param toStringF Function to convert items to String, if {@code null},
     *                  {@link Object#toString()} is used.
     * @param shouldLogF Function to filter logged items. If {@code null}, all
     *                   items are logged. <b>Warning:</b> Function will see
     *                   both items and {@link Punctuation}
     * @param wrapped The wrapped supplier.
     *
     * @see #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)
     */
    @Nonnull
    public static ProcessorMetaSupplier peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, false, true));
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
        TransformP(@Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> mapper) {
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
            return emitFromTraverser(resultTraverser);
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
        private final DistributedFunction<T, K> extractKey;
        private final Set<K> seenItems = new HashSet<>();

        /**
         * Constructs the processor with the given key extractor function.
         */
        CountDistinctP(@Nonnull DistributedFunction<T, K> extractKey) {
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
            return tryEmit((long) seenItems.size());
        }
    }
}
