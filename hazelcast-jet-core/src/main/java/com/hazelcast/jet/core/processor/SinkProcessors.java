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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import com.hazelcast.jet.impl.connector.WriteFileP;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.EntryProcessor;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Static utility class with factories of sink processors (the terminators
 * of the DAG data flow). For other kinds of processors refer to the {@link
 * com.hazelcast.jet.core.processor package-level documentation}.
 */
public final class SinkProcessors {
    private SinkProcessors() {
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#map(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeMapP(@Nonnull String mapName) {
        return HazelcastWriters.writeMapP(mapName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMap(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeRemoteMapP(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeMapP(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithMerging(String, DistributedFunction, DistributedFunction,
     * DistributedBinaryOperator)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier mergeMapP(
            @Nonnull String mapName,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedFunction<E, V> toValueFn,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        return HazelcastWriters.mergeMapP(mapName, null, toKeyFn, toValueFn, mergeFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMapWithMerging(String, ClientConfig, DistributedFunction,
     * DistributedFunction, DistributedBinaryOperator)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier mergeRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedFunction<E, V> toValueFn,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        return HazelcastWriters.mergeMapP(mapName, clientConfig, toKeyFn, toValueFn, mergeFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithEntryProcessor(String, DistributedFunction, DistributedFunction)} .
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier updateMapP(
            @Nonnull String mapName,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedBiFunction<V, E, V> updateFn
    ) {
        return HazelcastWriters.updateMapP(mapName, null, toKeyFn, updateFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMapWithUpdating(String, ClientConfig, DistributedFunction
     * , DistributedBiFunction)}.
     */
    @Nonnull
    public static <E, K, V> ProcessorMetaSupplier updateRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedBiFunction<V, E, V> updateFn
    ) {
        return HazelcastWriters.updateMapP(mapName, clientConfig, toKeyFn, updateFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#mapWithEntryProcessor(String, DistributedFunction, DistributedFunction)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateMapP(
            @Nonnull String mapName,
            @Nonnull DistributedFunction<T, K> toKeyFn,
            @Nonnull DistributedFunction<T, EntryProcessor<K, V>> toEntryProcessorFn

    ) {
        return HazelcastWriters.updateMapP(mapName, null, toKeyFn, toEntryProcessorFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteMapWithEntryProcessor(String, ClientConfig, DistributedFunction,
     * DistributedFunction)}.
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateRemoteMapP(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedFunction<T, K> toKeyFn,
            @Nonnull DistributedFunction<T, EntryProcessor<K, V>> toEntryProcessorFn
    ) {
        return HazelcastWriters.updateMapP(mapName, clientConfig, toKeyFn, toEntryProcessorFn);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#cache(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeCacheP(@Nonnull String cacheName) {
        return HazelcastWriters.writeCacheP(cacheName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteCache(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeRemoteCacheP(
            @Nonnull String cacheName, @Nonnull ClientConfig clientConfig
    ) {
        return HazelcastWriters.writeCacheP(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#list(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeListP(@Nonnull String listName) {
        return HazelcastWriters.writeListP(listName, null);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#remoteList(String, ClientConfig)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeRemoteListP(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeListP(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#socket(String, int)}.
     */
    public static <T> ProcessorMetaSupplier writeSocketP(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull Charset charset
    ) {
        String charsetName = charset.name();
        return dontParallelize(writeBufferedP(
                index -> uncheckCall(
                        () -> new BufferedWriter(new OutputStreamWriter(
                                new Socket(host, port).getOutputStream(), charsetName))),
                (bufferedWriter, item) -> {
                    try {
                        bufferedWriter.write(toStringFn.apply((T) item));
                        bufferedWriter.write('\n');
                    } catch (IOException e) {
                        throw sneakyThrow(e);
                    }
                },
                bufferedWriter -> uncheckRun(bufferedWriter::flush),
                bufferedWriter -> uncheckRun(bufferedWriter::close)
        ));
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#files(String, DistributedFunction, Charset, boolean)}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFileP(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull Charset charset,
            boolean append
    ) {
        return WriteFileP.metaSupplier(directoryName, toStringFn, charset.name(), append);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#files(String, DistributedFunction)}.
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeFileP(
            @Nonnull String directoryName, @Nonnull DistributedFunction<T, String> toStringFn
    ) {
        return writeFileP(directoryName, toStringFn, UTF_8, false);
    }

    /**
     * Returns a supplier of processors for
     * {@link Sinks#files(String)}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeFileP(@Nonnull String directoryName) {
        return writeFileP(directoryName, Object::toString, UTF_8, false);
    }

    /**
     * Returns a supplier of processors for a vertex that drains all the items
     * from its inbox to an intermediate buffer and then flushes the buffer.
     * This is a useful building block to implement sinks with explicit control
     * over buffering and flushing.
     *
     * @param <B>           type of buffer
     * @param <T>           type of received item
     * @param newBufferFn   supplies the buffer. The argument to this function
     *                      is the global processor index.
     * @param addToBufferFn adds an item to the buffer
     * @param flushBufferFn flushes the buffer
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBufferedP(
            @Nonnull DistributedIntFunction<B> newBufferFn,
            @Nonnull DistributedBiConsumer<B, T> addToBufferFn,
            @Nonnull DistributedConsumer<B> flushBufferFn
    ) {
        return writeBufferedP(newBufferFn, addToBufferFn, flushBufferFn, noopConsumer());
    }

    /**
     * Returns a supplier of processors for a vertex that drains all the items
     * from the inbox to an intermediate buffer and then flushes the buffer.
     * As each processor completes, it will dispose of the buffer by calling
     * {@code disposeBufferFn}.
     * <p>
     * This is a useful building block to implement sinks with explicit control
     * over buffering and flushing.
     *
     * @param <B>             type of buffer
     * @param <T>             type of received item
     * @param newBufferFn     supplies the buffer. The argument to this function
     *                        is the global processor index.
     * @param addToBufferFn   adds item to buffer
     * @param flushBufferFn   flushes the buffer
     * @param disposeBufferFn disposes of the buffer
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBufferedP(
            @Nonnull DistributedIntFunction<B> newBufferFn,
            @Nonnull DistributedBiConsumer<B, T> addToBufferFn,
            @Nonnull DistributedConsumer<B> flushBufferFn,
            @Nonnull DistributedConsumer<B> disposeBufferFn
    ) {
        return WriteBufferedP.supplier(newBufferFn, addToBufferFn, flushBufferFn, disposeBufferFn);
    }
}
