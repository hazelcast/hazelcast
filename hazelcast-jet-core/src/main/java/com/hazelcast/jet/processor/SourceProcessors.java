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
import java.util.Map;

/**
 * Static utility class with factories of source processors (the DAG
 * entry points). For other kinds for a vertexs refer to the {@link
 * com.hazelcast.jet.processor package-level documentation}.
 */
public final class SourceProcessors {

    private SourceProcessors() {
    }


    /**
     * Returns a meta-supplier of processors for a vertex that fetches entries
     * from the Hazelcast {@code IMap} with the specified name and emits them
     * as {@code Map.Entry}.
     * <p>
     * On each member the processors fetch only the locally stored entries. If
     * {@code localParallelism} for the vertex is more than one, they will
     * divide the labor within the member so that each one gets a subset of all
     * local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the processors
     * may miss and/or duplicate some entries.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName) {
        return ReadWithPartitionIteratorP.readMap(mapName);
    }

    /**
     * Returns a meta-supplier of processors for a vertex that fetches entries
     * from the Hazelcast {@code IMap} with the specified name, filters them
     * using the supplied predicate, transforms them using the supplied
     * projection function, and emits the results.
     * <p>
     * On each member the processors fetch only the locally stored entries. If
     * {@code localParallelism} for the vertex is more than one, they will
     * divide the labor within the member so that each one gets a subset of all
     * local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will have
     * no partitions assigned to them.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the processors
     * may miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMap(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
            @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMap(mapName, predicate, projectionFn);
    }

    /**
     * Convenience for {@link #streamMap(String, DistributedPredicate,
     * DistributedFunction, boolean)} with no projection or filtering. It
     * emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMap(@Nonnull String mapName, boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, null, null, startFromLatestSequence);
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
     * Returns a meta-supplier of processors for a vertex that fetches entries
     * from the Hazelcast {@code IMap} with the specified name in a remote
     * cluster identified by the supplied {@code ClientConfig} and emits them
     * as {@code Map.Entry}.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the processors
     * may miss and/or duplicate some entries.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readMap(mapName, clientConfig);
    }

    /**
     * Returns a meta-supplier of processors for a vertex that fetches entries
     * from the Hazelcast {@code IMap} with the specified name in a remote
     * cluster identified by the supplied {@code ClientConfig}, filters them
     * using the supplied predicate, transforms them using the supplied
     * projection function, and emits the results.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the processors
     * may miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMap(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
            @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn,
            @Nonnull ClientConfig clientConfig
    ) {
        return ReadWithPartitionIteratorP.readMap(mapName, predicate, projectionFn, clientConfig);
    }

    /**
     * Convenience for {@link #streamMap(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, boolean)} with no projection
     * or filtering. It emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMap(
            @Nonnull String mapName, @Nonnull ClientConfig clientConfig, boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, clientConfig, null, null, startFromLatestSequence);
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
     * Returns a meta-supplier of processors for a vertex that fetches entries
     * from the Hazelcast {@code ICache} with the specified name and emits them
     * as {@code Map.Entry}.
     * <p>
     * On each member the processors fetch only the locally stored entries. If
     * {@code localParallelism} for the vertex is more than one, they will
     * divide the labor within the member so that each one gets a subset of all
     * local partitions to read.
     * <p>
     * The number of Hazelcast partitions should be configured to at least
     * {@code localParallelism * clusterSize}, otherwise some processors will
     * have no partitions assigned to them.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the processors
     * may miss and/or duplicate some entries.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCache(cacheName);
    }

    /**
     * Convenience for {@link #streamCache(String, DistributedPredicate,
     * DistributedFunction, boolean)} with no projection or filtering. It emits
     * {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCache(@Nonnull String cacheName, boolean startFromLatestSequence) {
        return StreamEventJournalP.streamCache(cacheName, null, null, startFromLatestSequence);
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
    public static <T> ProcessorMetaSupplier streamCache(
            @Nonnull String cacheName,
            DistributedPredicate<EventJournalCacheEvent> predicate,
            DistributedFunction<EventJournalCacheEvent, T> projection,
            boolean startFromLatestSequence
    ) {
        return StreamEventJournalP.streamCache(cacheName, predicate, projection, startFromLatestSequence);
    }

    /**
     * Returns a meta-supplier of processors for a vertex that fetches entries
     * from the Hazelcast {@code ICache} with the specified name in a remote
     * cluster identified by the supplied {@code ClientConfig} and emits them
     * as {@code Map.Entry}.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the processors
     * may miss and/or duplicate some entries.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readCache(cacheName, clientConfig);
    }

    /**
     * Convenience for {@link #streamCache(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, boolean)} with no projection
     * or filtering. It emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCache(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig, boolean startFromLatestSequence
    ) {
        return StreamEventJournalP.streamCache(cacheName, clientConfig, null, null, startFromLatestSequence);
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
     * Returns a meta-supplier of processors for a vertex that emits items
     * retrieved from a Hazelcast {@code IList}. All elements are emitted on a
     * single member &mdash; the one where the entire list is stored by the
     * IMDG.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName) {
        return ReadIListP.supplier(listName);
    }

    /**
     * Returns a meta-supplier of processors for a vertex that emits items
     * retrieved from a Hazelcast {@code IList} in a remote cluster identified
     * by the supplied {@code ClientConfig}. All elements are emitted on a
     * single member &mdash; the one where the entire list is stored by the
     * IMDG.
     */
    @Nonnull
    public static ProcessorMetaSupplier readList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.supplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for a vertex which connects to the
     * specified socket and emits lines of text received from it. It decodes
     * the text using the supplied {@code Charset}.
     * <p>
     * Each processor opens its own TCP connection, so there will be {@code
     * clusterSize * localParallelism} open connections to the server.
     * <p>
     * Each processor completes when the server closes its connection. It never
     * attempts to reconnect.
     * <p><b>Note to jobs with snapshotting:</b>
     * Current implementation uses blocking socket API which blocks until there
     * are some data on the socket. Source processors have to return control to
     * be able to do the snapshot. If they block for too long, the snapshot
     * will be delayed. In {@link
     * com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE exactly-once}
     * mode the situation is even worse, because the job is stopped when one
     * parallel instance sees some data until all other do.
     */
    @Nonnull
    public static DistributedSupplier<Processor> streamSocket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamSocketP.supplier(host, port, charset.name());
    }

    /**
     * Returns a supplier of processors for a vertex that emits lines from
     * files in a directory (but not its subdirectories). The files must not
     * change while being read; if they do, the behavior is unspecified.
     * <p>
     * To be useful, the vertex should read files local to each member. For
     * example, if the pathname resolves to a shared network filesystem, it
     * will emit duplicate data.
     * <p>
     * Since the work of this vertex is file IO-intensive, its {@link
     * com.hazelcast.jet.Vertex#localParallelism(int) local parallelism}
     * should be set according to the performance characteristics of the
     * underlying storage system. Modern high-end devices peak with 4-8 reading
     * threads, so if running a single Jet job with a single file-reading
     * vertex, the optimal value would be in the range of 4-8. Note that any
     * one file is only read by one thread, so extra parallelism won't improve
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
     * Returns a supplier of processors for a vertex that emits a stream of
     * lines of text coming from files in the watched directory (but not its
     * subdirectories). It will emit only new contents added after startup:
     * both new files and new content appended to existing ones.
     * <p>
     * To be useful, the vertex should be configured to read data local to
     * each member. For example, if the pathname resolves to a shared network
     * filesystem, it will emit duplicate data.
     * <p>
     * If, during the scanning phase, the vertex observes a file that doesn't
     * end with a newline, it will assume that there is a line just being
     * written. This line won't appear in its output.
     * <p>
     * Since the work of this vertex is file IO-intensive, its {@link
     * com.hazelcast.jet.Vertex#localParallelism(int) local parallelism}
     * should be set according to the performance characteristics of the
     * underlying storage system. Modern high-end devices peak with 4-8 reading
     * threads, so if running a single Jet job with a single file-reading
     * vertex, the optimal value would be in the range of 4-8. Note that any
     * one file is only read by one thread, so extra parallelism won't improve
     * performance if there aren't enough files to read.
     * <p>
     * Each time the vertex detects a change in a file, it opens it, reads the
     * new content, and closes it. The vertex completes when the directory is
     * deleted. However, in order to delete the directory, all files in it must
     * be deleted and if you delete a file that is currently being read from,
     * the job may encounter an {@code IOException}. The directory must be
     * deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the writer keeps the file open while
     * appending (which is typical), the vertex may fail to observe the
     * changes. It will be notified if any process tries to open that file,
     * such as looking at the file in Explorer. This holds for Windows 10 with
     * the NTFS file system and might change in future. You are advised to do
     * your own testing on your target Windows platform.
     * <p>
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this vertex may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
     *
     * @param watchedDirectory The directory where we watch files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all (non-special) files.
     */
    public static ProcessorSupplier streamFiles(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return StreamFilesP.supplier(watchedDirectory, charset.name(), glob);
    }
}
