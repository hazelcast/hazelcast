/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core.processor;

import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.ToResultSetFunction;
import com.hazelcast.jet.impl.connector.ConvenientSourceP;
import com.hazelcast.jet.impl.connector.ConvenientSourceP.SourceBufferConsumerSide;
import com.hazelcast.jet.impl.connector.HazelcastReaders;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadJdbcP;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamJmsP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.jet.impl.pipeline.SourceBufferImpl;
import com.hazelcast.jet.pipeline.FileSourceBuilder;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ConnectorPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.security.Permission;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.Util.cacheEventToEntry;
import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.impl.connector.StreamEventJournalP.streamRemoteCacheSupplier;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

/**
 * Static utility class with factories of source processors (the DAG
 * entry points). For other kinds for a vertices refer to the {@link
 * com.hazelcast.jet.core.processor package-level documentation}.
 *
 * @since Jet 3.0
 */
public final class SourceProcessors {

    private SourceProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#map(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMapP(@Nonnull String mapName) {
        return HazelcastReaders.readLocalMapSupplier(mapName);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#map(String, Predicate, Projection)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        return HazelcastReaders.readLocalMapSupplier(mapName, predicate, projection);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#mapJournal(String, JournalInitialPosition)} )}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamMapP(mapName, mapPutEvents(), mapEventToEntry(), initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#mapJournal(String, JournalInitialPosition, FunctionEx, PredicateEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return StreamEventJournalP.streamMapSupplier(mapName, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorSupplier readRemoteMapP(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return HazelcastReaders.readRemoteMapSupplier(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig, Predicate, Projection)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection
    ) {
        return HazelcastReaders.readRemoteMapSupplier(mapName, clientConfig, predicate, projection);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMapJournal(String, ClientConfig, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamRemoteMapP(mapName, clientConfig, mapPutEvents(), mapEventToEntry(), initialPos,
                eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#remoteMapJournal(String, ClientConfig, JournalInitialPosition, FunctionEx, PredicateEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull PredicateEx<? super EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull FunctionEx<? super EventJournalMapEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        String clientXml = asXmlString(clientConfig);
        return StreamEventJournalP.streamRemoteMapSupplier(
                mapName, clientXml, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName) {
        return HazelcastReaders.readLocalCacheSupplier(cacheName);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cacheJournal(String, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamCacheP(cacheName, cachePutEvents(), cacheEventToEntry(), initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cacheJournal(String, JournalInitialPosition, FunctionEx, PredicateEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull PredicateEx<? super EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull FunctionEx<? super EventJournalCacheEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return StreamEventJournalP.streamCacheSupplier(cacheName, predicateFn, projectionFn, initialPos,
                eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorSupplier readRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return HazelcastReaders.readRemoteCacheSupplier(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteCacheJournal(String, ClientConfig, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super Entry<K, V>> eventTimePolicy
    ) {
        return streamRemoteCacheP(
                cacheName, clientConfig, cachePutEvents(), cacheEventToEntry(), initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#remoteCacheJournal(String, ClientConfig, JournalInitialPosition, FunctionEx, PredicateEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull PredicateEx<? super EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull FunctionEx<? super EventJournalCacheEvent<K, V>, ? extends T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        String clientXml = asXmlString(clientConfig);
        return streamRemoteCacheSupplier(
                cacheName, clientXml, predicateFn, projectionFn, initialPos, eventTimePolicy);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#list(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readListP(@Nonnull String listName) {
        return HazelcastReaders.localOrRemoteListSupplier(listName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return HazelcastReaders.localOrRemoteListSupplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#socket(String, int, Charset)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamSocketP(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamSocketP.supplier(host, port, charset.name());
    }

    /**
     * Returns a supplier of processors for {@link Sources#filesBuilder}.
     * See {@link FileSourceBuilder#build} for more details.
     */
    @Nonnull
    public static <R> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull BiFunctionEx<? super String, ? super String, ? extends R> mapOutputFn
    ) {
        String charsetName = charset.name();
        return readFilesP(
                    directory,
                    glob,
                    sharedFileSystem,
                    SecuredFunctions.readFileFn(directory, charsetName, mapOutputFn)
                );
    }

    /**
     * Returns a supplier of processors for {@link Sources#filesBuilder}.
     * See {@link FileSourceBuilder#build} for more details.
     */
    @Nonnull
    public static <I> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull FunctionEx<? super Path, ? extends Stream<I>> readFileFn
    ) {
        return readFilesP(directory, glob, sharedFileSystem, true, readFileFn);
    }

    /**
     * Returns a supplier of processors for {@link FileSources#files(String)}
     * to read local files.
     */
    @Nonnull
    public static <I> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            boolean ignoreFileNotFound,
            @Nonnull FunctionEx<? super Path, ? extends Stream<I>> readFileFn
    ) {
        return ReadFilesP.metaSupplier(directory, glob, sharedFileSystem, ignoreFileNotFound, readFileFn);
    }

    /**
     * Returns a supplier of processors for {@link Sources#filesBuilder}.
     * See {@link FileSourceBuilder#buildWatcher} for more details.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamFilesP(
            @Nonnull String watchedDirectory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull BiFunctionEx<? super String, ? super String, ?> mapOutputFn
    ) {
        return StreamFilesP.metaSupplier(watchedDirectory, charset.name(), glob, sharedFileSystem, mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link Sources#jmsQueueBuilder}.
     *
     * @param maxGuarantee maximum processing guarantee for the source. You can
     *      use it to disable acknowledging in transactions to save transaction
     *      overhead
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamJmsQueueP(
            @Nullable String destination,
            @Nonnull ProcessingGuarantee maxGuarantee,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull FunctionEx<? super Message, ?> messageIdFn,
            @Nonnull FunctionEx<? super Message, ? extends T> projectionFn
    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(
                ConnectorPermission.jms(destination, ACTION_READ),
                new StreamJmsP.Supplier<>(destination, maxGuarantee, eventTimePolicy,
                        newConnectionFn, consumerFn, messageIdFn, projectionFn)
        );
    }

    /**
     * Returns a supplier of processors for {@link Sources#jmsTopicBuilder}.
     *
     * @param isSharedConsumer true, if {@code createSharedConsumer()} or
     *      {@code createSharedDurableConsumer()} was used to create the
     *      consumer in the {@code consumerFn}
     * @param maxGuarantee maximum processing guarantee for the source. You can
     *      use it to disable acknowledging in transactions to save transaction
     *      overhead
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamJmsTopicP(
            @Nullable String destination,
            boolean isSharedConsumer,
            @Nonnull ProcessingGuarantee maxGuarantee,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull FunctionEx<? super Session, ? extends MessageConsumer> consumerFn,
            @Nonnull FunctionEx<? super Message, ?> messageIdFn,
            @Nonnull FunctionEx<? super Message, ? extends T> projectionFn
    ) {
        ProcessorSupplier pSupplier = new StreamJmsP.Supplier<>(
                destination, maxGuarantee, eventTimePolicy, newConnectionFn, consumerFn, messageIdFn, projectionFn);
        ConnectorPermission permission = ConnectorPermission.jms(destination, ACTION_READ);
        return isSharedConsumer
                ? ProcessorMetaSupplier.preferLocalParallelismOne(permission, pSupplier)
                : ProcessorMetaSupplier.forceTotalParallelismOne(pSupplier, newUnsecureUuidString(), permission);
    }

    /**
     * Returns a supplier of processors for {@link Sources#jdbc(
     * SupplierEx, ToResultSetFunction, FunctionEx)}.
     */
    public static <T> ProcessorMetaSupplier readJdbcP(
            @Nonnull SupplierEx<? extends java.sql.Connection> newConnectionFn,
            @Nonnull ToResultSetFunction resultSetFn,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return ReadJdbcP.supplier(newConnectionFn, resultSetFn, mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#jdbc(String, String, FunctionEx)}.
     */
    public static <T> ProcessorMetaSupplier readJdbcP(
            @Nonnull String connectionURL,
            @Nonnull String query,
            @Nonnull FunctionEx<? super ResultSet, ? extends T> mapOutputFn
    ) {
        return ReadJdbcP.supplier(connectionURL, query, mapOutputFn);
    }

    /**
     * Returns a supplier of processors for a source that the user can create
     * using the {@link SourceBuilder}. This variant creates a source that
     * emits items without timestamps.
     *
     * @param createFn function that creates the source's context object
     * @param fillBufferFn function that fills Jet's buffer with items to emit
     * @param createSnapshotFn function that returns a snapshot of the context object's state
     * @param restoreSnapshotFn function that restores the context object's state from a snapshot
     * @param destroyFn function that cleans up the resources held by the context object
     * @param preferredLocalParallelism preferred local parallelism of the source vertex. Special values:
     *                                  {@value Vertex#LOCAL_PARALLELISM_USE_DEFAULT} -> use the cluster's
     *                                  default local parallelism;
     *                                  0 -> create a single processor for the entire cluster (total parallelism = 1)
     * @param isBatch true, if the fillBufferFn will call {@code buffer.close()}, that is whether
     *                the source reads a bounded or unbounded set of data
     *
     * @param <C> type of the source's context object
     * @param <T> type of items the source emits
     * @param <S> type of object saved to state snapshot
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static <C, T, S> ProcessorMetaSupplier convenientSourceP(
            @Nonnull FunctionEx<? super Context, ? extends C> createFn,
            @Nonnull BiConsumerEx<? super C, ? super SourceBuffer<T>> fillBufferFn,
            @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn,
            @Nonnull BiConsumerEx<? super C, ? super List<S>> restoreSnapshotFn,
            @Nonnull ConsumerEx<? super C> destroyFn,
            int preferredLocalParallelism,
            boolean isBatch,
            @Nullable Permission permission
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(fillBufferFn, "fillBufferFn");
        checkSerializable(destroyFn, "destroyFn");
        checkSerializable(createSnapshotFn, "createSnapshotFn");
        checkSerializable(restoreSnapshotFn, "restoreSnapshotFn");
        checkNotNegative(preferredLocalParallelism + 1, "preferredLocalParallelism must >= -1");
        ProcessorSupplier procSup = ProcessorSupplier.of(
                () -> new ConvenientSourceP<>(
                        createFn,
                        (BiConsumer<? super C, ? super SourceBufferConsumerSide<?>>) fillBufferFn,
                        createSnapshotFn,
                        restoreSnapshotFn,
                        destroyFn,
                        new SourceBufferImpl.Plain<>(isBatch),
                        null));
        return preferredLocalParallelism != 0
                ? ProcessorMetaSupplier.of(preferredLocalParallelism, permission, procSup)
                : ProcessorMetaSupplier.forceTotalParallelismOne(procSup, permission);
    }

    /**
     * Returns a supplier of processors for a source that the user can create
     * using the {@link SourceBuilder}. This variant creates a source that
     * emits timestamped events.
     *
     * @param createFn function that creates the source's context object
     * @param fillBufferFn function that fills Jet's buffer with items to emit
     * @param eventTimePolicy parameters for watermark generation
     * @param createSnapshotFn function that returns a snapshot of the context object's state
     * @param restoreSnapshotFn function that restores the context object's state from a snapshot
     * @param destroyFn function that cleans up the resources held by the context object
     * @param preferredLocalParallelism preferred local parallelism of the source vertex. Special values:
     *                                  {@value Vertex#LOCAL_PARALLELISM_USE_DEFAULT} ->
     *                                  use the cluster's default local parallelism;
     *                                  0 -> create a single processor for the entire cluster (total parallelism = 1)
     *
     * @param <C> type of the context object
     * @param <T> type of items the source emits
     * @param <S> type of the object saved to state snapshot
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public static <C, T, S> ProcessorMetaSupplier convenientTimestampedSourceP(
            @Nonnull FunctionEx<? super Context, ? extends C> createFn,
            @Nonnull BiConsumerEx<? super C, ? super TimestampedSourceBuffer<T>> fillBufferFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn,
            @Nonnull BiConsumerEx<? super C, ? super List<S>> restoreSnapshotFn,
            @Nonnull ConsumerEx<? super C> destroyFn,
            int preferredLocalParallelism
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(fillBufferFn, "fillBufferFn");
        checkSerializable(destroyFn, "destroyFn");
        checkSerializable(createSnapshotFn, "createSnapshotFn");
        checkSerializable(restoreSnapshotFn, "restoreSnapshotFn");
        checkNotNegative(preferredLocalParallelism + 1, "preferredLocalParallelism must >= -1");
        ProcessorSupplier procSup = ProcessorSupplier.of(
                () -> new ConvenientSourceP<>(
                        createFn,
                        (BiConsumer<? super C, ? super SourceBufferConsumerSide<?>>) fillBufferFn,
                        createSnapshotFn,
                        restoreSnapshotFn,
                        destroyFn,
                        new SourceBufferImpl.Timestamped<>(),
                        eventTimePolicy
                ));
        return preferredLocalParallelism > 0
                ? ProcessorMetaSupplier.of(preferredLocalParallelism, procSup)
                : ProcessorMetaSupplier.forceTotalParallelismOne(procSup);
    }
}
