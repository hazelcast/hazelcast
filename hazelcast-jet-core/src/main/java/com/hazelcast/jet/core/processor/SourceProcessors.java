/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamJmsP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.cacheEventToEntry;
import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * Static utility class with factories of source processors (the DAG
 * entry points). For other kinds for a vertices refer to the {@link
 * com.hazelcast.jet.core.processor package-level documentation}.
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
        return ReadWithPartitionIteratorP.readMapP(mapName);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.pipeline.Sources#map(String, Predicate, Projection)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMapP(mapName, predicate, projectionFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.pipeline.Sources#map(String, Predicate, DistributedFunction)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMapP(mapName, predicate, toProjection(projectionFn));
    }


    /**
     * Returns a supplier of processors for
     * {@link Sources#mapJournal(String, JournalInitialPosition)} )}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull JournalInitialPosition initialPos,
            WatermarkGenerationParams<Entry<K, V>> wmGenParams
    ) {
        return streamMapP(mapName, mapPutEvents(), mapEventToEntry(), initialPos, wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#mapJournal(String, DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            WatermarkGenerationParams<? super T> wmGenParams
    ) {
        return StreamEventJournalP.streamMapP(mapName, predicateFn, projectionFn, initialPos, wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteMapP(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readRemoteMapP(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig, Predicate, Projection)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projection
    ) {
        return ReadWithPartitionIteratorP.readRemoteMapP(mapName, clientConfig, projection, predicate);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteMap(String, ClientConfig, Predicate, DistributedFunction)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readRemoteMapP(
                mapName, clientConfig, toProjection(projectionFn), predicate
        );
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
            WatermarkGenerationParams<Entry<K, V>> wmGenParams
    ) {
        return streamRemoteMapP(mapName, clientConfig, mapPutEvents(), mapEventToEntry(), initialPos,
                wmGenParams);
    }

    /**
     * Returns a supplier of processors for {@link
     * com.hazelcast.jet.pipeline.Sources#remoteMapJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<T> wmGenParams
    ) {
        return StreamEventJournalP.streamRemoteMapP(
                mapName, clientConfig, predicateFn, projectionFn, initialPos, wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCacheP(cacheName);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#cacheJournal(String, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<Entry<K, V>> wmGenParams
    ) {
        return streamCacheP(cacheName, cachePutEvents(), cacheEventToEntry(), initialPos, wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.pipeline.Sources#cacheJournal(String,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<T> wmGenParams
    ) {
        return StreamEventJournalP.streamCacheP(cacheName, predicateFn, projectionFn, initialPos,
                wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return ReadWithPartitionIteratorP.readRemoteCacheP(cacheName, clientConfig);
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
            @Nonnull WatermarkGenerationParams<Entry<K, V>> wmGenParams
    ) {
        return streamRemoteCacheP(cacheName, clientConfig, cachePutEvents(), cacheEventToEntry(), initialPos, wmGenParams);
    }

    /**
     * Returns a supplier of processors for {@link
     * Sources#remoteCacheJournal(String, ClientConfig,
     * DistributedPredicate, DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos,
            @Nonnull WatermarkGenerationParams<T> wmGenParams
    ) {
        return StreamEventJournalP.streamRemoteCacheP(cacheName, clientConfig, predicateFn, projectionFn, initialPos,
                wmGenParams);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#list(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readListP(@Nonnull String listName) {
        return ReadIListP.metaSupplier(listName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#remoteList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.metaSupplier(listName, clientConfig);
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
     * Returns a supplier of processors for
     * {@link Sources#files(String, Charset, String, DistributedBiFunction, boolean)}.
     */
    @Nonnull
    public static <R> ProcessorMetaSupplier readFilesP(
            @Nonnull String directory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            @Nonnull DistributedBiFunction<String, String, R> mapOutputFn,
            boolean sharedFileSystem
    ) {
        String charsetName = charset.name();
        return ReadFilesP.metaSupplier(directory, glob,
                path -> uncheckCall(() -> Files.lines(path, Charset.forName(charsetName))),
                mapOutputFn, sharedFileSystem);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sources#fileWatcher(String, Charset, String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamFilesP(
            @Nonnull String watchedDirectory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            @Nonnull DistributedBiFunction<String, String, ?> mapOutputFn
    ) {
        return StreamFilesP.metaSupplier(watchedDirectory, charset.name(), glob, mapOutputFn);
    }

    /**
     * Returns a supplier of processors for {@link Sources#jmsQueueBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamJmsQueueP(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Session> sessionFn,
            @Nonnull DistributedFunction<Session, MessageConsumer> consumerFn,
            @Nonnull DistributedConsumer<Session> flushFn,
            @Nonnull DistributedFunction<Message, T> projectionFn
    ) {
        return ProcessorMetaSupplier.of(
                StreamJmsP.supplier(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn),
                StreamJmsP.PREFERRED_LOCAL_PARALLELISM);
    }

    /**
     * Returns a supplier of processors for {@link Sources#jmsTopicBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier streamJmsTopicP(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Session> sessionFn,
            @Nonnull DistributedFunction<Session, MessageConsumer> consumerFn,
            @Nonnull DistributedConsumer<Session> flushFn,
            @Nonnull DistributedFunction<Message, T> projectionFn
    ) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                StreamJmsP.supplier(connectionSupplier, sessionFn, consumerFn, flushFn, projectionFn));
    }

    private static <I, O> Projection<I, O> toProjection(DistributedFunction<I, O> projectionFn) {
        return new Projection<I, O>() {
            @Override public O transform(I input) {
                return projectionFn.apply(input);
            }
        };
    }
}
