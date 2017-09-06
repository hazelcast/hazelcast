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

package com.hazelcast.jet.processor;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.map.journal.EventJournalMapEvent;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Static utility class with factories of source processors (the DAG
 * entry points). For other kinds of processors refer to the {@link
 * com.hazelcast.jet.processor package-level documentation}.
 */
public final class SourceProcessors {
    private SourceProcessors() {
    }

    /**
     * Convenience for {@link #streamMap(String, DistributedPredicate, DistributedFunction, boolean)}
     * emitting output as a {@link EventJournalMapEvent}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMap(@Nonnull String mapName) {
        return StreamEventJournalP.streamMap(mapName, null, null, false);
    }

    /**
     * Returns a meta-supplier of processor that will stream the
     * {@link com.hazelcast.journal.EventJournal} events
     * of the Hazelcast {@code IMap} with the specified name.
     * Given predicate and projection will be applied to the events on the source.
     * <p>
     * The processors will only access data local to the
     * member and, if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that each one gets
     * a subset of all local partitions to stream.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * In order to stream from a map, event-journal should be configured
     * Please see {@link com.hazelcast.config.EventJournalConfig}
     *
     * @param mapName                 The name of the map
     * @param predicate               The predicate to filter the events
     * @param projection              The projection to map the events
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamMap(@Nonnull String mapName,
                                                      DistributedPredicate<EventJournalMapEvent> predicate,
                                                      DistributedFunction<EventJournalMapEvent, T> projection,
                                                      boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, predicate, projection, startFromLatestSequence);
    }

    /**
     * Convenience for {@link #streamMap(String, ClientConfig, DistributedPredicate, DistributedFunction, boolean)}
     * emitting output as a {@link EventJournalMapEvent}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return StreamEventJournalP.streamMap(mapName, clientConfig, null, null, false);
    }

    /**
     * Returns a meta-supplier of processor that will stream the
     * {@link com.hazelcast.journal.EventJournal} events
     * of the Hazelcast {@code IMap} with the specified name from a remote cluster.
     * Given predicate and projection will be applied to the events on the source.
     * <p>
     * In order to stream from a map, event-journal should be configured
     * Please see {@link com.hazelcast.config.EventJournalConfig}
     *
     * @param mapName                 The name of the map
     * @param clientConfig            configuration for the client to connect to the remote cluster
     * @param predicate               The predicate to filter the events
     * @param projection              The projection to map the events
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamMap(@Nonnull String mapName,
                                                      @Nonnull ClientConfig clientConfig,
                                                      DistributedPredicate<EventJournalMapEvent> predicate,
                                                      DistributedFunction<EventJournalMapEvent, T> projection,
                                                      boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, clientConfig, predicate, projection, startFromLatestSequence);
    }

    /**
     * Convenience for {@link #streamCache(String, DistributedPredicate, DistributedFunction, boolean)}
     * emitting output as a {@link EventJournalCacheEvent}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCache(@Nonnull String cacheName) {
        return StreamEventJournalP.streamCache(cacheName, null, null, false);
    }

    /**
     * Returns a meta-supplier of processor that will stream the
     * {@link com.hazelcast.journal.EventJournal} events
     * of the Hazelcast {@code ICache} with the specified name.
     * Given predicate and projection will be applied to the events on the source.
     * <p>
     * The processors will only access data local to the
     * member and, if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that each one gets
     * a subset of all local partitions to stream.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * In order to stream from a cache, event-journal should be configured
     * Please see {@link com.hazelcast.config.EventJournalConfig}
     *
     * @param cacheName               The name of the cache
     * @param predicate               The predicate to filter the events
     * @param projection              The projection to map the events
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamCache(@Nonnull String cacheName,
                                                        DistributedPredicate<EventJournalCacheEvent> predicate,
                                                        DistributedFunction<EventJournalCacheEvent, T> projection,
                                                        boolean startFromLatestSequence) {
        return StreamEventJournalP.streamCache(cacheName, predicate, projection, startFromLatestSequence);
    }

    /**
     * Convenience for {@link #streamCache(String, ClientConfig, DistributedPredicate, DistributedFunction, boolean)}
     * emitting output as a {@link EventJournalCacheEvent}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return StreamEventJournalP.streamCache(cacheName, clientConfig, null, null, false);
    }

    /**
     * Returns a meta-supplier of processor that will stream the
     * {@link com.hazelcast.journal.EventJournal} events
     * of the Hazelcast {@code ICache} with the specified name from a remote cluster.
     * Given predicate and projection will be applied to the events on the source.
     * <p>
     * In order to stream from a cache, event-journal should be configured
     * Please see {@link com.hazelcast.config.EventJournalConfig}
     *
     * @param cacheName               The name of the cache
     * @param clientConfig            configuration for the client to connect to the remote cluster
     * @param predicate               The predicate to filter the events
     * @param projection              The projection to map the events
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamCache(@Nonnull String cacheName,
                                                        @Nonnull ClientConfig clientConfig,
                                                        DistributedPredicate<EventJournalCacheEvent> predicate,
                                                        DistributedFunction<EventJournalCacheEvent, T> projection,
                                                        boolean startFromLatestSequence) {
        return StreamEventJournalP.streamCache(cacheName, clientConfig, predicate, projection, startFromLatestSequence);
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
     * Iterating the map should be done only when the {@link com.hazelcast.core.IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName) {
        return ReadWithPartitionIteratorP.readMap(mapName);
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from the
     * Hazelcast {@code IMap} with the specified name. Entries will be filtered
     * according to the given predicate and the result of the projection will be
     * emitted to downstream. The processors will only access data local to the
     * member and, if {@code localParallelism} for the vertex is above one,
     * processors will divide the labor within the member so that each one gets
     * a subset of all local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * Iterating the map should be done only when the {@link com.hazelcast.core.IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMap(@Nonnull String mapName,
                                                          @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
                                                          @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionF) {
        return ReadWithPartitionIteratorP.readMap(mapName, predicate, projectionF);
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from a
     * Hazelcast {@code IMap} in a remote cluster. Processor will emit the
     * entries as {@code Map.Entry}.
     * <p>
     * Iterating the map should be done only when the {@link com.hazelcast.core.IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readMap(mapName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processor that will fetch entries from a
     * Hazelcast {@code IMap} in a remote cluster.
     * Entries will be filtered according to the given predicate and the result of the
     * projection will be emitted to downstream.
     * <p>
     * Iterating the map should be done only when the {@link com.hazelcast.core.IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMap(@Nonnull String mapName,
                                                          @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
                                                          @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionF,
                                                          @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readMap(mapName, predicate, projectionF, clientConfig);
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
     * Iterating the map should be done only when the {@link com.hazelcast.cache.ICache} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
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
     * Iterating the map should be done only when the {@link com.hazelcast.cache.ICache} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readCache(cacheName, clientConfig);
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
     * Convenience for {@link #streamSocket(String, int, Charset)} with
     * the UTF-8 character set.
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamSocket(@Nonnull String host, int port) {
        return streamSocket(host, port, StandardCharsets.UTF_8);
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
     * @param host    The host name to connect to
     * @param port    The port number to connect to
     * @param charset Character set used to decode the stream
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamSocket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamSocketP.supplier(host, port, charset.name());
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
     * com.hazelcast.jet.Vertex#localParallelism(int) local parallelism} of the
     * vertex should be set according to the performance characteristics of the
     * underlying storage system. Modern high-end devices peak with 4-8 reading
     * threads, so if running a single Jet job with a single file-reading vertex,
     * the optimal value would be in the range of 4-8. Note that any one file
     * is only read by one thread, so extra parallelism won't improve the
     * performance if there aren't enough files to read.
     *
     * @param directory parent directory of the files
     * @param charset   charset to use to decode the files
     * @param glob      the globbing mask, see {@link
     *                  java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *                  Use {@code "*"} for all files.
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
     * the watched directory (but not its subdirectories). You can pass `*` as
     * the pattern to read all files in the directory. It will pick up both
     * newly created files and content appended to pre-existing files. It
     * expects the file contents not to change once appended. There is no
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
     * com.hazelcast.jet.Vertex#localParallelism(int) local parallelism} of the
     * vertex should be set according to the performance characteristics of the
     * underlying storage system. Modern high-end devices peak with 4-8 reading
     * threads, so if running a single Jet job with a single file-reading vertex,
     * the optimal value would be in the range of 4-8. Note that any one file
     * is only read by one thread, so extra parallelism won't improve the
     * performance if there aren't enough files to read.
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
     * <p>
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the writer keeps the file open while
     * appending (which is typical), the processor may fail to observe the
     * changes. It will be notified if any process tries to open that file,
     * such as looking at the file in Explorer. This holds for Windows 10 with
     * the NTFS file system and might change in future. You are advised to do
     * your own testing on your target Windows platform.
     * <p>
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this processor may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
     *
     * @param watchedDirectory The directory where we watch files
     * @param charset          charset to use to decode the files
     * @param glob             the globbing mask, see {@link
     *                         java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *                         Use {@code "*"} for all files.
     */
    public static ProcessorSupplier streamFiles(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return StreamFilesP.supplier(watchedDirectory, charset.name(), glob);
    }
}
