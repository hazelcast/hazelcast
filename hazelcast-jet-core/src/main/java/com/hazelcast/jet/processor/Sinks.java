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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.WriteBufferedP;
import com.hazelcast.jet.impl.connector.WriteFileP;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Static utility class with factories of sink processors (the terminators
 * of the DAG data flow). For other kinds of processors refer to the {@link
 * com.hazelcast.jet.processor package-level documentation}.
 */
public final class Sinks {
    private Sinks() {
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
     * @param newBufferF supplies the buffer. Supplier argument is the global processor index
     * @param addToBufferF adds item to buffer
     * @param flushBufferF flushes the buffer
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBuffered(
            @Nonnull DistributedIntFunction<B> newBufferF,
            @Nonnull DistributedBiConsumer<B, T> addToBufferF,
            @Nonnull DistributedConsumer<B> flushBufferF
    ) {
        return writeBuffered(newBufferF, addToBufferF, flushBufferF, noopConsumer());
    }

    /**
     * Returns a supplier of processor which drains all items from the inbox
     * to an intermediate buffer and then flushes the buffer. The buffer will
     * be disposed via {@code disposeBufferF} once the processor is completed.
     * <p>
     * This is a useful building block to implement sinks with explicit control
     * over buffering and flushing.
     *
     * @param <B> type of buffer
     * @param <T> type of received item
     * @param newBufferF supplies the buffer. Supplier argument is the global processor index.
     * @param addToBufferF adds item to buffer
     * @param flushBufferF flushes the buffer
     * @param disposeBufferF disposes of the buffer
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBuffered(
            @Nonnull DistributedIntFunction<B> newBufferF,
            @Nonnull DistributedBiConsumer<B, T> addToBufferF,
            @Nonnull DistributedConsumer<B> flushBufferF,
            @Nonnull DistributedConsumer<B> disposeBufferF
    ) {
        return WriteBufferedP.supplier(newBufferF, addToBufferF, flushBufferF, disposeBufferF);
    }

    /**
     * Convenience for {@link #writeSocket(String, int, DistributedFunction, Charset)}
     * with {@code Object::toString} formatter and UTF-8 charset.
     */
    public static ProcessorSupplier writeSocket(@Nonnull String host, int port) {
        return writeSocket(host, port, Object::toString, StandardCharsets.UTF_8);
    }

    /**
     * Convenience for {@link #writeSocket(String, int, DistributedFunction, Charset)}
     * with UTF-8 charset.
     */
    public static <T> ProcessorSupplier writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return writeSocket(host, port, toStringF, StandardCharsets.UTF_8);
    }

    /**
     * Returns a supplier of processor which connects to specified socket and
     * writes the items as text.
     * <p>
     * Note that no separator is added between items.
     */
    public static <T> ProcessorSupplier writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull Charset charset
    ) {
        String charsetName = charset.name();
        return writeBuffered(
                index -> uncheckCall(
                        () -> new BufferedWriter(new OutputStreamWriter(
                                new Socket(host, port).getOutputStream(), charsetName))),
                (bufferedWriter, item) -> {
                    try {
                        bufferedWriter.write(toStringF.apply((T) item));
                        bufferedWriter.write('\n');
                    } catch (IOException e) {
                        throw sneakyThrow(e);
                    }
                },
                bufferedWriter -> uncheckRun(bufferedWriter::flush),
                bufferedWriter -> uncheckRun(bufferedWriter::close)
        );
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all members.
     */
    @Nonnull
    public static ProcessorSupplier writeFile(@Nonnull String directoryName) {
        return writeFile(directoryName, Object::toString, StandardCharsets.UTF_8, false);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all members.
     * @param toStringF a function to convert items to String (a formatter).
     */
    @Nonnull
    public static <T> ProcessorSupplier writeFile(
            @Nonnull String directoryName, @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return writeFile(directoryName, toStringF, StandardCharsets.UTF_8, false);
    }

    /**
     * Returns a meta-supplier of processor that writes all items to a local
     * file on each member. The output of {@code toStringF} is written to the
     * file, followed by a platform-specific line separator. Files are named
     * with an integer number starting from 0, which is unique cluster-wide.
     * <p>
     * The same pathname must be available for writing on all members. Each
     * processor instance will write to its own file so the full data will be
     * distributed among several files and members.
     * <p>
     * Since the work of this processor is file IO-intensive, {@link
     * com.hazelcast.jet.Vertex#localParallelism(int) local parallelism} of the
     * vertex should be set according to the performance characteristics of the
     * underlying storage system. Modern high-end devices peak with 4-8 writing
     * threads, so if running a single Jet job with a single file-writing
     * vertex, the optimal value would be in the range of 4-8.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all members.
     * @param toStringF a function to convert items to String (a formatter)
     * @param charset charset used to encode the file output
     * @param append whether to append or overwrite the file
     */
    @Nonnull
    public static <T> ProcessorSupplier writeFile(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull Charset charset,
            boolean append
    ) {
        return WriteFileP.supplier(directoryName, toStringF, charset.name(), append);
    }
}
