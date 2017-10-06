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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.connector.ReadFilesP;
import com.hazelcast.jet.impl.connector.ReadIListP;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import com.hazelcast.jet.impl.connector.StreamEventJournalP;
import com.hazelcast.jet.impl.connector.StreamFilesP;
import com.hazelcast.jet.impl.connector.StreamSocketP;
import com.hazelcast.map.journal.EventJournalMapEvent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Map;

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
     * {@link com.hazelcast.jet.Sources#readMap(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMapP(@Nonnull String mapName) {
        return ReadWithPartitionIteratorP.readMap(mapName);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readMap(String, ClientConfig)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
            @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn
    ) {
        return ReadWithPartitionIteratorP.readMap(mapName, predicate, projectionFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamMap(String, boolean)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMapP(@Nonnull String mapName, boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, null, null, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamMap(String, DistributedPredicate, DistributedFunction, boolean)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nullable DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            boolean startFromLatestSequence
    ) {
        return StreamEventJournalP.streamMap(mapName, predicate, projection, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readMapP(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readMap(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readMap(String, DistributedPredicate, DistributedFunction, ClientConfig)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier readMapP(
            @Nonnull String mapName,
            @Nonnull DistributedPredicate<Map.Entry<K, V>> predicate,
            @Nonnull DistributedFunction<Map.Entry<K, V>, T> projectionFn,
            @Nonnull ClientConfig clientConfig
    ) {
        return ReadWithPartitionIteratorP.readMap(mapName, predicate, projectionFn, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamMap(String, ClientConfig, boolean)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName, @Nonnull ClientConfig clientConfig, boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, clientConfig, null, null, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for {@link
     * com.hazelcast.jet.Sources#streamMap(String, ClientConfig, DistributedPredicate, DistributedFunction, boolean)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nullable DistributedPredicate<EventJournalMapEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalMapEvent<K, V>, T> projection,
            boolean startFromLatestSequence) {
        return StreamEventJournalP.streamMap(mapName, clientConfig, predicate, projection, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readCache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName) {
        return ReadWithPartitionIteratorP.readCache(cacheName);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamCache(String, boolean)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCacheP(@Nonnull String cacheName, boolean startFromLatestSequence) {
        return StreamEventJournalP.streamCache(cacheName, null, null, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamCache(String, DistributedPredicate, DistributedFunction, boolean)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nullable DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            boolean startFromLatestSequence
    ) {
        return StreamEventJournalP.streamCache(cacheName, predicate, projection, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readCacheP(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return ReadWithPartitionIteratorP.readCache(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamCache(String, ClientConfig, boolean)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig, boolean startFromLatestSequence
    ) {
        return StreamEventJournalP.streamCache(cacheName, clientConfig, null, null, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for {@link
     * com.hazelcast.jet.Sources#streamCache(String, ClientConfig, DistributedPredicate, DistributedFunction, boolean)}.
     */
    @Nonnull
    public static <K, V, T> ProcessorMetaSupplier streamCacheP(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig,
            @Nullable DistributedPredicate<EventJournalCacheEvent<K, V>> predicate,
            @Nullable DistributedFunction<EventJournalCacheEvent<K, V>, T> projection,
            boolean startFromLatestSequence
    ) {
        return StreamEventJournalP.streamCache(cacheName, clientConfig, predicate, projection, startFromLatestSequence);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readList(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readListP(@Nonnull String listName) {
        return ReadIListP.supplier(listName);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier readListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return ReadIListP.supplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamSocket(String, int, Charset)}.
     */
    @Nonnull
    public static ProcessorSupplier streamSocketP(
            @Nonnull String host, int port, @Nonnull Charset charset
    ) {
        return StreamSocketP.supplier(host, port, charset.name());
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#readFiles(String, Charset, String)}.
     */
    @Nonnull
    public static ProcessorSupplier readFilesP(
            @Nonnull String directory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return ReadFilesP.supplier(directory, charset.name(), glob);
    }

    /**
     * Returns a supplier of processors for
     * {@link com.hazelcast.jet.Sources#streamFiles(String, Charset, String)}.
     */
    public static ProcessorSupplier streamFilesP(
            @Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob
    ) {
        return StreamFilesP.supplier(watchedDirectory, charset.name(), glob);
    }
}
