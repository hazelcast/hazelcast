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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import com.hazelcast.jet.impl.connector.WriteFileP;
import com.hazelcast.jet.impl.connector.WriteJdbcP;
import com.hazelcast.jet.impl.connector.WriteJmsP;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.security.permission.ConnectorPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;
import javax.sql.CommonDataSource;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.util.Map;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;

/**
 * Static utility class with factories of sink processors (the terminators
 * of the DAG data flow). For other kinds of processors refer to the {@link
 * com.hazelcast.jet.core.processor package-level documentation}.
 *
 * @since Jet 3.0
 */
public final class SinkProcessors {
    private SinkProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#map(String)}.
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier writeMapP(@Nonnull String mapName) {
        return writeMapP(mapName, Map.Entry::getKey, Map.Entry<K, V>::getValue);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#map(String, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeMapP(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        return HazelcastWriters.writeMapSupplier(mapName, null, toKeyFn, toValueFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeRemoteMapP(
        @Nonnull String mapName, @Nonnull ClientConfig clientConfig
    ) {
        return writeRemoteMapP(mapName, clientConfig, identity(), identity());
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMap(String, ClientConfig, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        return HazelcastWriters.writeMapSupplier(mapName, clientConfig, toKeyFn, toValueFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithMerging(String, FunctionEx, FunctionEx,
     * BinaryOperatorEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier mergeMapP(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return HazelcastWriters.mergeMapSupplier(mapName, null, toKeyFn, toValueFn, mergeFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMapWithMerging(String, ClientConfig, FunctionEx,
     * FunctionEx, BinaryOperatorEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier mergeRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return HazelcastWriters.mergeMapSupplier(mapName, clientConfig, toKeyFn, toValueFn, mergeFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithEntryProcessor(String, FunctionEx, FunctionEx)} .
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateMapP(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        return HazelcastWriters.updateMapSupplier(mapName, null, toKeyFn, updateFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMapWithUpdating(String, ClientConfig, FunctionEx
     * , BiFunctionEx)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        return HazelcastWriters.updateMapSupplier(mapName, clientConfig, toKeyFn, updateFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithEntryProcessor(String, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapP(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn

    ) {
        return HazelcastWriters.updateMapSupplier(mapName, null, toKeyFn, toEntryProcessorFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithEntryProcessor(int, String, FunctionEx, FunctionEx)}.
     */
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapP(
            int maxParallelAsyncOps,
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn

    ) {
        return HazelcastWriters.updateMapSupplier(maxParallelAsyncOps, mapName, null, toKeyFn, toEntryProcessorFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMapWithEntryProcessor(String, ClientConfig, FunctionEx,
     * FunctionEx)}.
     */
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        return HazelcastWriters.updateMapSupplier(mapName, clientConfig, toKeyFn, toEntryProcessorFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#cache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeCacheP(@Nonnull String cacheName) {
        return HazelcastWriters.writeCacheSupplier(cacheName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return HazelcastWriters.writeCacheSupplier(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#list(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeListP(@Nonnull String listName) {
        return HazelcastWriters.writeListSupplier(listName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeRemoteListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeListSupplier(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#socket(String, int)}.
     */
    public static <T> ProcessorMetaSupplier writeSocketP(
            @Nonnull String host,
            int port,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull Charset charset
    ) {
        checkSerializable(toStringFn, "toStringFn");

        String charsetName = charset.name();
        return preferLocalParallelismOne(
                ConnectorPermission.socket(host, port, ACTION_WRITE),
                writeBufferedP(
                        SecuredFunctions.createBufferedWriterFn(host, port, charsetName),
                        (bufferedWriter, item) -> {
                            @SuppressWarnings("unchecked")
                            T t = (T) item;
                            bufferedWriter.write(toStringFn.apply(t));
                            bufferedWriter.write('\n');
                        },
                        BufferedWriter::flush,
                        BufferedWriter::close
                ));
    }

    /**
     * Returns a supplier of processors for {@link Sinks#filesBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFileP(
            @Nonnull String directoryName,
            @Nonnull Charset charset,
            @Nullable String datePattern,
            long maxFileSize,
            boolean exactlyOnce,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn
    ) {
        checkSerializable(toStringFn, "toStringFn");

        return WriteFileP.metaSupplier(directoryName, toStringFn, charset.name(),
                datePattern, maxFileSize, exactlyOnce);
    }

    /**
     * Shortcut for {@link #writeBufferedP(FunctionEx, BiConsumerEx,
     * ConsumerEx, ConsumerEx)} with a no-op {@code destroyFn}.
     */
    @Nonnull
    public static <W, T> SupplierEx<Processor> writeBufferedP(
            @Nonnull FunctionEx<? super Context, ? extends W> createFn,
            @Nonnull BiConsumerEx<? super W, ? super T> onReceiveFn,
            @Nonnull ConsumerEx<? super W> flushFn
    ) {
        return writeBufferedP(createFn, onReceiveFn, flushFn, ConsumerEx.noop());
    }

    /**
     * Returns a supplier of processors for a vertex that drains all the items
     * from the inbox to an internal writer object and then does a flush. As
     * each processor completes, it disposes of its writer by calling {@code
     * destroyFn}.
     * <p>
     * This is a useful building block to implement sinks with explicit control
     * over resource management, buffering and flushing.
     * <p>
     * The returned processor will have preferred local parallelism of 1. It
     * will not participate in state saving for fault tolerance.
     *
     * @param createFn     supplies the writer. The argument to this function
     *                     is the context for the given processor.
     * @param onReceiveFn function that Jet calls upon receiving each item for the sink
     * @param flushFn     function that flushes the writer
     * @param destroyFn   function that destroys the writer
     * @param <W>         type of the writer
     * @param <T>         type of the received item
     */
    @Nonnull
    public static <W, T> SupplierEx<Processor> writeBufferedP(
            @Nonnull FunctionEx<? super Context, ? extends W> createFn,
            @Nonnull BiConsumerEx<? super W, ? super T> onReceiveFn,
            @Nonnull ConsumerEx<? super W> flushFn,
            @Nonnull ConsumerEx<? super W> destroyFn
    ) {
        return WriteBufferedP.supplier(createFn, onReceiveFn, flushFn, destroyFn);
    }

    /**
     * Returns a supplier of processors for {@link Sinks#jmsQueueBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeJmsQueueP(
            @Nonnull String queueName,
            boolean exactlyOnce,
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn
    ) {
        return WriteJmsP.supplier(queueName, exactlyOnce, newConnectionFn, messageFn, false);
    }

    /**
     * Returns a supplier of processors for {@link Sinks#jmsTopicBuilder}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeJmsTopicP(
            @Nonnull String topicName,
            boolean exactlyOnce,
            @Nonnull SupplierEx<? extends Connection> newConnectionFn,
            @Nonnull BiFunctionEx<? super Session, ? super T, ? extends Message> messageFn
    ) {
        return WriteJmsP.supplier(topicName, exactlyOnce, newConnectionFn, messageFn, true);
    }

    /**
     * Returns a supplier of processors for {@link Sinks#jdbcBuilder()}.
     * <p>
     * <b>Notes</b>
     * <p>
     * Until Jet 4.4, the batch size limit for the supplied processors used to
     * be hard-coded to 50. Since Jet 4.5, this limit is now configurable and
     * must be explicitly set using the {@code batchLimit} parameter.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeJdbcP(
            @Nullable String jdbcUrl,
            @Nonnull String updateQuery,
            @Nonnull SupplierEx<? extends CommonDataSource> dataSourceSupplier,
            @Nonnull BiConsumerEx<? super PreparedStatement, ? super T> bindFn,
            boolean exactlyOnce,
            int batchLimit
    ) {
        checkNotNull(updateQuery, "updateQuery");
        checkNotNull(dataSourceSupplier, "dataSourceSupplier");
        checkNotNull(bindFn, "bindFn");
        checkPositive(batchLimit, "batchLimit");
        return WriteJdbcP.metaSupplier(jdbcUrl, updateQuery, dataSourceSupplier, bindFn, exactlyOnce, batchLimit);
    }

    /**
     * Returns a supplier of processors for {@link Sinks#observable}.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static ProcessorMetaSupplier writeObservableP(@Nonnull String name) {
        return HazelcastWriters.writeObservableSupplier(name);
    }
}
