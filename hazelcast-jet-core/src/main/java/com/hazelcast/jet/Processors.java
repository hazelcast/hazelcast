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
import com.hazelcast.jet.impl.connector.WriteLoggerP;
import com.hazelcast.jet.impl.processor.AggregateP;
import com.hazelcast.jet.impl.processor.GroupByKeyP;
import com.hazelcast.jet.impl.processor.PeekWrappedP;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.WrappingProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map.Entry;

import static com.hazelcast.jet.WindowingProcessors.withCombiningAccumulate;
import static com.hazelcast.jet.function.DistributedFunction.identity;
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
     * Returns a meta-supplier of processor that will fetch entries from the
     * Hazelcast {@code IMap} with the specified name and will emit them as
     * {@code Map.Entry}. The processors will only access data local to the
     * member and, if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that each one gets
     * a subset of all local partitions to read.
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
     * Returns a meta-supplier of processor that will fetch entries from a
     * Hazelcast {@code IMap} in a remote cluster. Processor will emit the
     * entries as {@code Map.Entry}.
     * <p>
     * If the underlying map is concurrently modified, there may be missing or
     * duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readMap(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processor that will put data into a Hazelcast
     * {@code IMap}. Processor expects items of type {@code Map.Entry}.
     */
    @Nonnull
    public static ProcessorSupplier writeMap(@Nonnull String mapName) {
        return HazelcastWriters.writeMap(mapName);
    }

    /**
     * Returns a supplier of processor that will put data into a Hazelcast
     * {@code IMap} in a remote cluster. Processor expects items of type {@code
     * Map.Entry}.
     */
    @Nonnull
    public static ProcessorSupplier writeMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeMap(mapName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from the
     * Hazelcast {@code ICache} with the specified name and will emit them as
     * {@code Cache.Entry}. The processors will only access data local to the
     * member and, if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that each one gets
     * a subset of all local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * If the underlying cache is concurrently modified, there may be missing
     * or duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCache(cacheName);
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from a
     * Hazelcast {@code ICache} in a remote cluster. Processor will emit the
     * entries as {@code Cache.Entry}.
     * <p>
     * If the underlying cache is concurrently modified, there may be missing
     * or duplicate items.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readCache(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processor which will put data into a Hazelcast
     * {@code ICache}. Processor expects items of type {@code Map.Entry}
     */
    @Nonnull
    public static ProcessorSupplier writeCache(@Nonnull String cacheName) {
        return HazelcastWriters.writeCache(cacheName);
    }

    /**
     * Returns a supplier of processor which will put data into a Hazelcast
     * {@code ICache} in a remote cluster. Processor expects items of type
     * {@code Map.Entry}.
     */
    @Nonnull
    public static ProcessorSupplier writeCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeCache(cacheName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processor that emits items retrieved from an
     * IMDG IList. Note that all elements from the list are emitted on a single
     * member &mdash; the one where the entire list is stored by the IMDG.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName) {
        return ReadIListP.supplier(listName);
    }

    /**
     * Returns a meta-supplier of processor that emits items retrieved from an
     * IMDG IList in a remote cluster. Note that all elements from the list are
     * emitted on a single member &mdash; the one where the entire list is
     * stored by the IMDG.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.supplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processor which writes received items to an IMDG
     * {@code IList}.
     */
    @Nonnull
    public static ProcessorSupplier writeList(@Nonnull String listName) {
        return HazelcastWriters.writeList(listName);
    }

    /**
     * Returns a supplier of processor which writes received items to an IMDG
     * {@code IList} in a remote cluster.
     */
    @Nonnull
    public static ProcessorSupplier writeList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeList(listName, clientConfig);
    }

    /**
     * Returns a supplier of processor which drains all items from its inbox
     * to an intermediate buffer and then flushes the buffer. This is a useful
     * building block to implement sinks with explicit control over buffering
     * and flushing.
     *
     * @param <B> type of buffer
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
     * Returns a supplier of processor which drains all items from the inbox
     * to an intermediate buffer and then flushes the buffer. The buffer will
     * be disposed via {@code disposeBuffer} once the processor is completed.
     * <p>
     * This is a useful building block to implement sinks with explicit control
     * over buffering and flushing.
     *
     * @param <B> type of buffer
     * @param <T> type of received item
     * @param newBuffer supplies the buffer. Supplier argument is the global processor index.
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
     * Returns a supplier of processor which connects to specified socket and
     * writes the items as text.
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
     * Convenience for {@link #streamTextSocket(String, int, Charset)} with
     * the UTF-8 character set.
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamTextSocket(@Nonnull String host, int port) {
        return streamTextSocket(host, port, StandardCharsets.UTF_8);
    }

    /**
     * Returns a supplier of processor which connects to a specified socket and
     * reads and emits text line by line. This processor expects a server-side
     * socket to be available to connect to.
     * <p>
     * Each processor instance will create a socket connection to the configured
     * [host:port], so there will be {@code clusterSize * localParallelism}
     * connections. The server should do the load-balancing.
     * <p>
     * The processor will complete when the socket is closed by the server.
     * No reconnection is attempted.
     *
     * @param host The host name to connect to
     * @param port The port number to connect to
     * @param charset Character set used to decode the stream
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamTextSocket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamTextSocketP.supplier(host, port, charset.name());
    }

    /**
     * Convenience for {@link #readFiles(String, Charset, String)} with the
     * default charset (UTF-8) and the default glob pattern (match all files).
     */
    @Nonnull
    public static ProcessorSupplier readFiles(@Nonnull String directory) {
        return readFiles(directory, StandardCharsets.UTF_8, "*");
    }

    /**
     * A source that emits lines from all files in a directory (but not its
     * subdirectories), or only the files matching the supplied {@code glob}
     * pattern. The files must not change while being read; if they do, the
     * behavior is unspecified. There will be no indication which file a
     * particular line comes from.
     * <p>
     * The same pathname should be available on all members, but it should not
     * contain the same files &mdash; (e.g., it shouldn't resolve to a
     * directory shared over the network).
     * <p>
     * Since the work of this processor is file IO-intensive, {@link
     * Vertex#localParallelism(int) local parallelism} of the vertex should be
     * set according to the performance characteristics of the underlying
     * storage system. Typical values are in the range of 1 to 4. If just a
     * single file is read, it is always read by single thread.
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
     * Convenience for {@link #streamFiles(String, Charset, String)} with the
     * default charset (UTF-8) and the default glob pattern (match all files).
     */
    public static ProcessorSupplier streamFiles(@Nonnull String watchedDirectory) {
        return streamFiles(watchedDirectory, StandardCharsets.UTF_8, "*");
    }

    /**
     * A source that generates a stream of lines of text coming from files in
     * the watched directory (but not its subdirectories). It will pick up both
     * newly created files and content appended to pre-existing files. It
     * expects the file content not to change once appended. There is no
     * indication which file a particular line comes from.
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
     * Since the work of this processor is file IO-intensive, {@link
     * Vertex#localParallelism(int) local parallelism} of the vertex should be
     * set according to the performance characteristics of the underlying
     * storage system. Typical values are in the range of 1 to 4. If just a
     * single file is read, it is always read by single thread.
     * <p>
     * When a change is detected the file is opened, appended lines are read
     * and file is closed. This process is repeated as necessary.
     * <p>
     * The processor completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     *
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the writer keeps the file open while
     * appending (which is typical), the processor may fail to observe the
     * changes. It will be notified if any process tries to open that file,
     * such as looking at the file in Explorer. This holds for Windows 10 with
     * the NTFS file system and might change in future. You are advised to do
     * your own testing on your target Windows platform.
     *
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this processor may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
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
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all nodes.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeFile(@Nonnull String directoryName) {
        return writeFile(directoryName, Object::toString, StandardCharsets.UTF_8, false);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all nodes.
     * @param toStringF a function to convert items to String (a formatter).
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFile(
            @Nonnull String directoryName, @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return writeFile(directoryName, toStringF, StandardCharsets.UTF_8, false);
    }

    /**
     * Returns a meta-supplier of a processor that writes all items to a local
     * file on each member. {@code item.toString()} is written to the file,
     * followed by a platform-specific line separator.
     * <p>
     * The same pathname must be available for writing on all nodes. The file
     * on each node will contain part of the data processed on that member.
     * <p>
     * Since the work of this processor is file IO-intensive, {@link
     * Vertex#localParallelism(int) local parallelism} of the vertex should be
     * set according to the performance characteristics of the underlying
     * storage system. Typical values are in the range of 1 to 4.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all nodes.
     * @param toStringF a function to convert items to String (a formatter)
     * @param charset charset used to encode the file output
     * @param append whether to append or overwrite the file
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFile(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull Charset charset,
            boolean append
    ) {
        return WriteFileP.supplier(directoryName, toStringF, charset.name(), append);
    }

    /**
     * Returns a supplier of processor which, for each received item, emits the
     * result of applying the given mapping function to it. If the result is
     * {@code null}, nothing will be emitted. Therefore this processor can be
     * used to implement filtering semantics as well.
     *
     * @param mapper the mapping function
     * @param <T> type of received item
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, R> DistributedSupplier<Processor> map(
            @Nonnull DistributedFunction<T, R> mapper
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
    public static <T> DistributedSupplier<Processor> filter(@Nonnull DistributedPredicate<T> predicate) {
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
            @Nonnull DistributedFunction<T, ? extends Traverser<? extends R>> mapper
    ) {
        return () -> new TransformP<T, R>(mapper);
    }

    /**
     * Returns a supplier of processor that groups items by key and performs
     * the provided aggregate operation on each group. After exhausting all
     * its input it emits one {@code Map.Entry<K, R>} per observed key.
     *
     * @param getKeyF computes the key from the entry
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> groupAndAggregate(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return () -> new GroupByKeyP<>(getKeyF, aggregateOperation);
    }

    /**
     * Returns a supplier of the first-stage processor in a two-stage
     * group-and-aggregate setup. The processor groups items by the grouping
     * key (as obtained from the given key-extracting function) and applies
     * the {@link AggregateOperation#accumulateItemF() accumulate} aggregation
     * primitive to each group. After exhausting all its input it emits one
     * {@code Map.Entry<K, A>} per observed key.
     *
     * @param getKeyF computes the key from the entry
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorF()}
     */
    @Nonnull
    public static <T, K, A> DistributedSupplier<Processor> groupAndAccumulate(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, ?> aggregateOperation
    ) {
        return () -> new GroupByKeyP<>(getKeyF, aggregateOperation.withFinish(identity()));
    }

    /**
     * Returns a supplier of the second-stage processor in a two-stage
     * group-and-aggregate setup. It applies the {@link
     * AggregateOperation#combineAccumulatorsF() combine} aggregation
     * primitive to the entries received from several upstream instances of
     * {@link #groupAndAccumulate(DistributedFunction, AggregateOperation)
     * groupAndAccumulate()}. After exhausting all its input it emits one
     * {@code Map.Entry<K, R>} per observed key.
     *
     * @param aggregateOperation the aggregate operation to perform
     * @param <A> type of accumulator returned from {@code
     *            aggregateOperation.createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <A, R> DistributedSupplier<Processor> combineAndFinish(
            @Nonnull AggregateOperation<?, A, R> aggregateOperation
    ) {
        return () -> new GroupByKeyP<>(Entry::getKey, withCombiningAccumulate(aggregateOperation));
    }

    /**
     * Returns a supplier of processor that performs the provided aggregate
     * operation on all the items it receives. After exhausting all its input
     * it emits a single item of type {@code R} &mdash;the result of the
     * aggegate operation.
     *
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggregateOperation.createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> aggregate(
            @Nonnull AggregateOperation<T, A, R> aggregateOperation
    ) {
        return () -> new AggregateP<>(aggregateOperation);
    }

    /**
     * Returns a supplier of processor that consumes all its input (if any) and
     * does nothing with it. It emits nothing.
     */
    @Nonnull
    public static DistributedSupplier<Processor> noop() {
        return NoopP::new;
    }

    /**
     * Decorates a {@code ProcessorSupplier} with one that will declare all its
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

    /** A no-operation processor. See {@link #noop()} */
    private static class NoopP implements Processor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            inbox.drain(noopConsumer());
        }
    }

    /**
     * Convenience for {@link #writeLogger(DistributedFunction)} without format.
     * It will use {@link Object#toString()}.
     */
    @Nonnull
    public static DistributedSupplier<Processor> writeLogger() {
        return writeLogger(Object::toString);
    }

    /**
     * Returns a supplier of processor that logs all items at the INFO level.
     * {@link Punctuation} items are not logged.
     * <p>
     * Note that the event will be logged on the cluster members, not on the
     * client, so it's primarily meant for testing.
     * <p>
     * {@link Vertex#localParallelism(int) Local parallelism} of 1 is
     * recommended for this vertex.
     *
     * @param toStringF Function to convert item to String.
     * @param <T> input item type
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> writeLogger(
            @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return () -> new WriteLoggerP(toStringF);
    }

    /**
     * Returns a meta-supplier that will add logging to the processors created
     * by the provided meta-supplier. Each item the processor removes from the
     * inbox will be logged. Items are logged at the INFO level to the
     * following logging category: {@link PeekWrappedP}.
     * <p>
     * <strong>Warning:</strong> The {@code toStringF} and {@code shouldLogF}
     * functions will see all items, including {@link Punctuation}s.
     *
     * @param toStringF function that returns the string representation of the item
     * @param shouldLogF function to filter logged items. {@link
     *                   com.hazelcast.jet.function.DistributedFunctions#alwaysTrue()} can be
     *                   used as a pass-through filter when no filtering is needed.
     * @param wrapped The wrapped meta-supplier.
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
     * Same as {@link #peekInput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekInput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
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
     * Same as {@link #peekInput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekInput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code DistributedSupplier} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull DistributedSupplier<Processor> wrapped
    ) {
        return () -> new PeekWrappedP(wrapped.get(), toStringF, shouldLogF, true, false);
    }

    /**
     * Convenience for {@link #peekInput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekInput(toStringF,
     * shouldLogF, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekInput(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekInput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekInput(toStringF,
     * shouldLogF, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekInput(@Nonnull ProcessorSupplier wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekInput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekInput(toStringF,
     * shouldLogF, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code DistributedSupplier} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekInput(@Nonnull DistributedSupplier<Processor> wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Returns a meta-supplier that will add logging to the processors created
     * by the provided meta-supplier. Each item the processor adds to the
     * outbox will be logged. Items are logged at the INFO level to the
     * following logging category: {@link PeekWrappedP}.
     * <p>
     * <strong>Warning:</strong> The {@code toStringF} and {@code shouldLogF}
     * functions will see all items, including {@link Punctuation}s.
     *
     * @param toStringF function that returns the string representation of the item
     * @param shouldLogF function to filter logged items. {@link
     *                   com.hazelcast.jet.function.DistributedFunctions#alwaysTrue()} can be
     *                   used as a pass-through filter when no filtering is needed.
     * @param wrapped The wrapped meta-supplier.
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
     * Same as {@link #peekOutput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekOutput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
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
     * Same as {@link #peekOutput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekOutput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code DistributedSupplier} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> new PeekWrappedP(wrapped.get(), toStringF, shouldLogF, false, true);
    }

    /**
     * Convenience for {@link #peekOutput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekOutput(toStringF,
     * shouldLogF, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekOutput(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekOutput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekOutput(toStringF,
     * shouldLogF, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekOutput(@Nonnull ProcessorSupplier wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekOutput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekOutput(toStringF,
     * shouldLogF, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code DistributedSupplier} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekOutput(@Nonnull DistributedSupplier<Processor> wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }
}
