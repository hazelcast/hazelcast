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

package com.hazelcast.jet.core.processor;

import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.JournalInitialPosition;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.cacheEventToEntry;
import static com.hazelcast.jet.Util.cachePutEvents;
import static com.hazelcast.jet.Util.mapEventToEntry;
import static com.hazelcast.jet.Util.mapPutEvents;

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
     * {@link com.hazelcast.jet.Sources#map(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMapP(@Nonnull String mapName) {
        return ReadWithPartitionIteratorP.readMapP(mapName);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#map(String, Predicate, Projection)}}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMapP(mapName, predicate, projectionFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#map(String, Predicate, DistributedFunction)}}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull DistributedFunction<Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMapP(mapName, predicate, toProjection(projectionFn));
    }


    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#mapJournal(String, JournalInitialPosition)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName, @Nonnull JournalInitialPosition initialPos
    ) {
        return streamMapP(mapName, mapPutEvents(), mapEventToEntry(), initialPos);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#mapJournal(String, DistributedPredicate,
     * DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return StreamEventJournalP.streamMapP(mapName, predicateFn, projectionFn, initialPos);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#remoteMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteMapP(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readRemoteMapP(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#remoteMap(String, ClientConfig, Predicate, Projection)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Predicate<K, V> predicate,
            @Nonnull Projection<Entry<K, V>, T> projection
    ) {
        return ReadWithPartitionIteratorP.readRemoteMapP(mapName, clientConfig, projection, predicate);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#remoteMap(String, ClientConfig, Predicate, DistributedFunction)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readRemoteMapP(
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
     * {@link com.hazelcast.jet.Sources#remoteMapJournal(String, ClientConfig, JournalInitialPosition)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return streamRemoteMapP(mapName, clientConfig, mapPutEvents(), mapEventToEntry(), initialPos);
    }

    /**
     * Returns a supplier of processors for {@link
     * com.hazelcast.jet.Sources#remoteMapJournal(
     * String, ClientConfig, DistributedPredicate, DistributedFunction, JournalInitialPosition
     * )}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalMapEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalMapEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return StreamEventJournalP.streamRemoteMapP(
                mapName, clientConfig, predicateFn, projectionFn, initialPos);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#cache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCacheP(cacheName);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#cacheJournal(String, JournalInitialPosition)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCacheP(@Nonnull String cacheName,
                                                     @Nonnull JournalInitialPosition initialPos) {
        return streamCacheP(cacheName, cachePutEvents(), cacheEventToEntry(), initialPos);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#cacheJournal(String, DistributedPredicate,
     * DistributedFunction, JournalInitialPosition)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return StreamEventJournalP.streamCacheP(cacheName, predicateFn, projectionFn, initialPos);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#remoteCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return ReadWithPartitionIteratorP.readRemoteCacheP(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#remoteCacheJournal(String, ClientConfig, JournalInitialPosition)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig, @Nonnull JournalInitialPosition initialPos
    ) {
        return streamRemoteCacheP(cacheName, clientConfig, cachePutEvents(), cacheEventToEntry(), initialPos);
    }

    /**
     * Returns a supplier of processors for {@link
     * com.hazelcast.jet.Sources#remoteCacheJournal(
     * String, ClientConfig, DistributedPredicate, DistributedFunction, JournalInitialPosition
     * )}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamRemoteCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedPredicate<EventJournalCacheEvent<K, V>> predicateFn,
            @Nonnull DistributedFunction<EventJournalCacheEvent<K, V>, T> projectionFn,
            @Nonnull JournalInitialPosition initialPos
    ) {
        return StreamEventJournalP.streamRemoteCacheP(
                cacheName, clientConfig, predicateFn, projectionFn, initialPos);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#list(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readListP(@Nonnull String listName) {
        return ReadIListP.metaSupplier(listName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#remoteList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readRemoteListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.metaSupplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#socket(String, int, Charset)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamSocketP(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamSocketP.supplier(host, port, charset.name());
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#files(String, Charset, String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readFilesP(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return ReadFilesP.metaSupplier(directory, charset.name(), glob);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#fileWatcher(String, Charset, String)}.
     */
    public static ProcessorMetaSupplier streamFilesP(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return StreamFilesP.metaSupplier(watchedDirectory, charset.name(), glob);
    }

    private static <I, O> Projection<I, O> toProjection(DistributedFunction<I, O> projectionFn) {
        return new Projection<I, O>() {
            @Override public O transform(I input) {
                return projectionFn.apply(input);
            }
        };
    }
}
