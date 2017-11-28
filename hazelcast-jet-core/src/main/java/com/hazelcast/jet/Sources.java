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

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.SourceImpl;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.processor.SourceProcessors.readCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readFilesP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteListP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteCacheP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamRemoteMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamSocketP;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sources. To start
 * building a pipeline, pass a source to {@link Pipeline#drawFrom(Source)}
 * and you will obtain the initial {@link ComputeStage}. You can then
 * attach further stages to it.
 * <p>
 * The same pipeline may contain more than one source, each starting its
 * own branch. The branches may be merged with multiple-input transforms
 * such as co-group and hash-join.
 */
public final class Sources {

    private static final String GLOB_WILDCARD = "*";

    private Sources() {
    }

    /**
     * Returns a source constructed directly from the given Core API processor
     * meta-supplier.
     *
     * @param sourceName user-friendly source name
     * @param metaSupplier the processor meta-supplier
     */
    public static <T> Source<T> fromProcessor(
            @Nonnull String sourceName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        return new SourceImpl<>(sourceName, metaSupplier);
    }

    /**
     * Returns a source that fetches entries from a local Hazelcast {@code IMap}
     * with the specified name and emits them as {@code Map.Entry}. It leverages
     * data locality by making each of the underlying processors fetch only those
     * entries that are stored on the member where it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    public static <K, V> Source<Map.Entry<K, V>> map(@Nonnull String mapName) {
        return fromProcessor("mapSource(" + mapName + ')', readMapP(mapName));
    }

    /**
     * Returns a source that fetches entries from a local Hazelcast {@code
     * IMap} with the specified name. By supplying a {@code predicate} and
     * {@code projection} here instead of in separate {@code map/filter}
     * transforms you allow the source to apply these functions early, before
     * generating any output, with the potential of significantly reducing
     * data traffic. If your data is stored in the IMDG using the <a href=
     *     "http://docs.hazelcast.org/docs/3.9/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link
     *     com.hazelcast.projection.Projections#singleAttribute(String)
     * Projections.singleAttribute()} and {@link
     *     com.hazelcast.projection.Projections#multiAttribute(String...)
     * Projections.multiAttribute()}) to create your projection instance and
     * using the {@link com.hazelcast.query.Predicates Predicates} factory or
     * {@link com.hazelcast.query.PredicateBuilder PredicateBuilder} to create
     * the predicate. In this case Jet can test the predicate and apply the
     * projection without deserializing the whole object.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    public static <K, V, T> Source<T> map(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projection
    ) {
        return fromProcessor("mapSource(" + mapName + ')', readMapP(mapName, predicate, projection));
    }

    /**
     * Convenience for {@link #map(String, Predicate, Projection)}
     * which uses a {@link DistributedFunction} as the projection function.
     */
    public static <K, V, T> Source<T> map(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn
    ) {
        return fromProcessor("mapSource(" + mapName + ')', readMapP(mapName, predicate, projectionFn));
    }

    /**
     * Returns a source that will stream the {@link EventJournalMapEvent}
     * events of the Hazelcast {@code IMap} with the specified name. By
     * supplying a {@code predicate} and {@code projection} here instead of
     * in separate {@code map/filter} transforms you allow the source to apply
     * these functions early, before generating any output, with the potential
     * of significantly reducing data traffic.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     *
     * <h4>Issue when "catching up"</h4>
     * The processor reads partitions one by one: it gets events from one
     * partition and then moves to the next one etc. This adds time disorder to
     * events: it might emit very recent event from partition1 while not yet
     * emitting an old event from partition2. If watermarks are added to the
     * stream later, the allowed event lag should accommodate this disorder.
     * Most notably, the "catching up" happens after the job is restarted, when
     * events since the last snapshot are reprocessed in a burst. In order to
     * not lose any events, the lag should be configured to at least {@code
     * snapshotInterval + timeToRestart + normalEventLag}.
     * We plan to address this issue in a future release.
     *
     * @param mapName the name of the map
     * @param predicateFn the predicate to filter the events, can be {@code null}
     * @param projectionFn the projection to map the events, can be {@code null}
     * @param startFromLatestSequence starting point of the events in event journal.
     *          {@code true} to start from the latest, {@code false} to start from the earliest
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> mapJournal(
            @Nonnull String mapName,
            @Nullable DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nullable DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("mapJournalSource(" + mapName + ')',
                streamMapP(mapName, predicateFn, projectionFn, startFromLatestSequence));
    }

    /**
     * Convenience for {@link #mapJournal(String, DistributedPredicate,
     * DistributedFunction, boolean)} with no projection or filtering. It
     * emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalMapEvent<K, V>> mapJournal(
            @Nonnull String mapName,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("mapJournalSource(" + mapName + ')', streamMapP(mapName, startFromLatestSequence));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code IMap}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig
    ) {
        return fromProcessor("remoteMapSource(" + mapName + ')', readRemoteMapP(mapName, clientConfig));
    }

    /**
     * Returns a source that fetches entries from a remote Hazelcast {@code
     * IMap} with the specified name in a remote cluster identified by the
     * supplied {@code ClientConfig}. By supplying a {@code predicate} and
     * {@code projection} here instead of in separate {@code map/filter}
     * transforms you allow the source to apply these functions early, before
     * generating any output, with the potential of significantly reducing
     * data traffic. If your data is stored in the IMDG using the <a href=
     *     "http://docs.hazelcast.org/docs/3.9/manual/html-single/index.html#implementing-portable-serialization">
     * portable serialization format</a>, there are additional optimizations
     * available when using {@link
     *     com.hazelcast.projection.Projections#singleAttribute(String)
     * Projections.singleAttribute()} and {@link
     *     com.hazelcast.projection.Projections#multiAttribute(String...)
     * Projections.multiAttribute()}) to create your projection instance and
     * using the {@link com.hazelcast.query.Predicates Predicates} factory or
     * {@link com.hazelcast.query.PredicateBuilder PredicateBuilder} to create
     * the predicate. In this case Jet can test the predicate and apply the
     * projection without deserializing the whole object.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code IMap} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    public static <K, V, T> Source<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projection
    ) {
        return fromProcessor("remoteMapSource(" + mapName + ')',
                readRemoteMapP(mapName, clientConfig, predicate, projection));
    }

    /**
     * Convenience for {@link #remoteMap(String, ClientConfig, Predicate, Projection)}
     * which use a {@link DistributedFunction} as the projection function.
     */
    public static <K, V, T> Source<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn
    ) {
        return fromProcessor("remoteMapSource(" + mapName + ')',
                readRemoteMapP(mapName, clientConfig, predicate, projectionFn));
    }

    /**
     * Returns a source that will stream the {@link EventJournalMapEvent}
     * events of the Hazelcast {@code IMap} with the specified name from a
     * remote cluster. By supplying a {@code predicate} and {@code projection}
     * here instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * To use an {@code IMap} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     *
     * @param mapName the name of the map
     * @param clientConfig configuration for the client to connect to the remote cluster
     * @param predicateFn the predicate to filter the events, can be {@code null}
     * @param projectionFn the projection to map the events, can be {@code null}
     * @param startFromLatestSequence starting point of the events in event journal.
     *          {@code true} to start from latest, {@code false} to start from oldest.
     * @param <K> type of key
     * @param <V> type of value
     * @param <T> type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nullable DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nullable DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("remoteMapJournalSource(" + mapName + ')',
                streamRemoteMapP(mapName, clientConfig, predicateFn, projectionFn, startFromLatestSequence));
    }

    /**
     * Convenience for {@link #remoteMapJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, boolean)} with no projection
     * or filtering. It emits {@link EventJournalMapEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalMapEvent<K, V>> remoteMapJournal(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("remoteMapJournalSource(" + mapName + ')',
                streamRemoteMapP(mapName, clientConfig, startFromLatestSequence));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name and emits them as {@code Map.Entry}. It
     * leverages data locality by making each of the underlying processors
     * fetch only those entries that are stored on the member where it is
     * running.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> cache(@Nonnull String cacheName) {
        return fromProcessor("cacheSource(" + cacheName + ')', readCacheP(cacheName));
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name. By
     * supplying a {@code predicate} and {@code projection} here instead of
     * in separate {@code map/filter} transforms you allow the source to apply
     * these functions early, before generating any output, with the potential
     * of significantly reducing data traffic.
     * <p>
     * The source leverages data locality by making each of the underlying
     * processors fetch only those entries that are stored on the member where
     * it is running.
     * <p>
     * To use an {@code ICache} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     *
     * @param cacheName               The name of the cache
     * @param predicateFn               The predicate to filter the events, can be null
     * @param projectionFn              The projection to map the events, can be null
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> cacheJournal(
            @Nonnull String cacheName,
            @Nullable DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nullable DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("cacheJournalSource(" + cacheName + ')',
                streamCacheP(cacheName, predicateFn, projectionFn, startFromLatestSequence)
        );
    }

    /**
     * Convenience for {@link #cacheJournal(String, DistributedPredicate,
     * DistributedFunction, boolean)} with no projection or filtering. It
     * emits {@link EventJournalCacheEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalCacheEvent<K, V>> cacheJournal(
            @Nonnull String cacheName,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("cacheJournalSource(" + cacheName + ')', streamCacheP(cacheName, startFromLatestSequence));
    }

    /**
     * Returns a source that fetches entries from the Hazelcast {@code ICache}
     * with the specified name in a remote cluster identified by the supplied
     * {@code ClientConfig} and emits them as {@code Map.Entry}.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * If the {@code ICache} is modified while being read, or if there is a
     * cluster topology change (triggering data migration), the source may
     * miss and/or duplicate some entries.
     */
    @Nonnull
    public static <K, V> Source<Map.Entry<K, V>> remoteCache(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        return fromProcessor("remoteCacheSource(" + cacheName + ')', readRemoteCacheP(cacheName, clientConfig)
        );
    }

    /**
     * Returns a source that will stream the {@link EventJournalCacheEvent}
     * events of the Hazelcast {@code ICache} with the specified name from a
     * remote cluster. By supplying a {@code predicate} and {@code projection}
     * here instead of in separate {@code map/filter} transforms you allow the
     * source to apply these functions early, before generating any output,
     * with the potential of significantly reducing data traffic.
     * <p>
     * To use an {@code ICache} as a streaming source, you must {@link
     * com.hazelcast.config.EventJournalConfig configure the event journal}
     * for it. The journal has fixed capacity and will drop events if it
     * overflows.
     * <p>
     * The source saves the journal offset to the snapshot. If the job
     * restarts, it starts emitting from the saved offset with an
     * exactly-once guarantee (unless the journal has overflowed).
     *
     * @param cacheName               The name of the cache
     * @param clientConfig            configuration for the client to connect to the remote cluster
     * @param predicateFn               The predicate to filter the events, can be null
     * @param projectionFn              The projection to map the events, can be null
     * @param startFromLatestSequence starting point of the events in event journal
     *                                {@code true} to start from latest, {@code false} to start from oldest
     * @param <T>                     type of emitted item
     */
    @Nonnull
    public static <K, V, T> Source<T> remoteCacheJournal(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nullable DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nullable DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("remoteCacheJournalSource(" + cacheName + ')',
                streamRemoteCacheP(cacheName, clientConfig, predicateFn, projectionFn, startFromLatestSequence));
    }

    /**
     * Convenience for {@link #remoteCacheJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, boolean)} with no projection
     * or filtering. It emits {@link EventJournalCacheEvent}s.
     */
    @Nonnull
    public static <K, V> Source<EventJournalCacheEvent<K, V>> remoteCacheJournal(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            boolean startFromLatestSequence
    ) {
        return fromProcessor("remoteCacheJournalSource(" + cacheName + ')',
                streamRemoteCacheP(cacheName, clientConfig, startFromLatestSequence));
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList}. All elements are emitted on a single member &mdash; the one
     * where the entire list is stored by the IMDG.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     */
    @Nonnull
    public static <E> Source<E> list(@Nonnull String listName) {
        return fromProcessor("listSource(" + listName + ')', readListP(listName));
    }

    /**
     * Returns a source that emits items retrieved from a Hazelcast {@code
     * IList} in a remote cluster identified by the supplied {@code
     * ClientConfig}. All elements are emitted on a single member.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     */
    @Nonnull
    public static <E> Source<E> remoteList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return fromProcessor("remoteListSource(" + listName + ')', readRemoteListP(listName, clientConfig));
    }

    /**
     * Returns a source which connects to the specified socket and emits lines
     * of text received from it. It decodes the text using the supplied {@code
     * charset}.
     * <p>
     * Each underlying processor opens its own TCP connection, so there will be
     * {@code clusterSize * localParallelism} open connections to the server.
     * <p>
     * The source completes when the server closes the socket. It never attempts
     * to reconnect. Any {@code IOException} will cause the job to fail.
     * <p>
     * The source does not save any state to snapshot. On job restart, it will
     * emit whichever items the server sends. The implementation uses
     * non-blocking API, the processor is cooperative.
     */
    @Nonnull
    public static Source<String> socket(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return fromProcessor("socketSourceSource(" + host + ':' + port + ')', streamSocketP(host, port, charset));
    }

    /**
     * A source that emits lines from files in a directory (but not its
     * subdirectories. The files must not change while being read; if they do,
     * the behavior is unspecified.
     * <p>
     * To be useful, the source should be configured to read data local to each
     * member. For example, if the pathname resolves to a shared network
     * filesystem visible by multiple members, they will emit duplicate data.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     *
     * @param directory parent directory of the files
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all files.
     */
    @Nonnull
    public static Source<String> files(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return fromProcessor("filesSource(" + new File(directory, glob) + ')', readFilesP(directory, charset, glob));
    }

    /**
     * Convenience for {@link #files(String, Charset, String) readFiles(directory, UTF_8, "*")}.
     */
    public static Source<String> files(@Nonnull String directory) {
        return files(directory, UTF_8, GLOB_WILDCARD);
    }

    /**
     * A source that emits a stream of lines of text coming from files in
     * the watched directory (but not its subdirectories). It will emit only
     * new contents added after startup: both new files and new content
     * appended to existing ones.
     * <p>
     * To be useful, the source should be configured to read data local to each
     * member. For example, if the pathname resolves to a shared network
     * filesystem visible by multiple members, they will emit duplicate data.
     * <p>
     * If, during the scanning phase, the source observes a file that doesn't
     * end with a newline, it will assume that there is a line just being
     * written. This line won't appear in its output.
     * <p>
     * The source completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * lines added after the restart will be emitted, which gives at-most-once
     * behavior.
     *
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the file-writing process keeps the file
     * open while appending, the processor may fail to observe the changes.
     * It will be notified if any process tries to open that file, such as
     * looking at the file in Explorer. This holds for Windows 10 with the NTFS
     * file system and might change in future. You are advised to do your own
     * testing on your target Windows platform.
     *
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this source may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
     *
     * @param watchedDirectory pathname to the source directory
     * @param charset charset to use to decode the files
     * @param glob the globbing mask, see {@link
     *             java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     *             Use {@code "*"} for all files.
     */
    public static Source<String> fileWatcher(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return fromProcessor("fileWatcherSource(" + watchedDirectory + '/' + glob + ')',
                streamFilesP(watchedDirectory, charset, glob)
        );
    }

    /**
     * Convenience for {@link #fileWatcher(String, Charset, String)
     * streamFiles(watchedDirectory, UTF_8, "*")}.
     */
    public static Source<String> fileWatcher(@Nonnull String watchedDirectory) {
        return fileWatcher(watchedDirectory, UTF_8, GLOB_WILDCARD);
    }
}
