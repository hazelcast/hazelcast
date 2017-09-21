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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.ProcessorSupplier;
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
     * Returns a supplier of processors for a vertex that puts {@code
     * Map.Entry}s it receives into a Hazelcast {@code IMap} with the
     * specified name.
     */
    @Nonnull
    public static ProcessorSupplier writeMap(@Nonnull String mapName) {
        return HazelcastWriters.writeMap(mapName);
    }

    /**
     * Returns a supplier of processors for a vertex that puts {@code
     * Map.Entry}s it receives into a Hazelcast {@code IMap} with the
     * specified name in a remote cluster identified by the supplied
     * {@code ClientConfig}.
     */
    @Nonnull
    public static ProcessorSupplier writeMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeMap(mapName, clientConfig);
    }

    /**
     * Returns a supplier of processors for a vertex that puts {@code
     * Map.Entry}s it receives into a Hazelcast {@code ICache} with the
     * specified name.
     */
    @Nonnull
    public static ProcessorSupplier writeCache(@Nonnull String cacheName) {
        return HazelcastWriters.writeCache(cacheName);
    }

    /**
     * Returns a supplier of processors for a vertex that puts {@code
     * Map.Entry}s it receives into a Hazelcast {@code ICache} with the
     * specified name in a remote cluster identified by the supplied
     * {@code ClientConfig}.
     */
    @Nonnull
    public static ProcessorSupplier writeCache(@Nonnull String cacheName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeCache(cacheName, clientConfig);
    }

    /**
     * Returns a supplier of processors for a vertex that adds the items it
     * receives to a Hazelcast {@code IList} with the specified name.
     */
    @Nonnull
    public static ProcessorSupplier writeList(@Nonnull String listName) {
        return HazelcastWriters.writeList(listName);
    }

    /**
     * Returns a supplier of processors for a vertex that adds the items it
     * receives to a Hazelcast {@code IList} with the specified name in a
     * remote cluster identified by the supplied {@code ClientConfig}.
     */
    @Nonnull
    public static ProcessorSupplier writeList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
        return HazelcastWriters.writeList(listName, clientConfig);
    }

    /**
     * Returns a supplier of processors for a vertex that drains all the items
     * from its inbox to an intermediate buffer and then flushes the buffer.
     * This is a useful building block to implement sinks with explicit control
     * over buffering and flushing.
     *
     * @param <B> type of buffer
     * @param <T> type of received item
     * @param newBufferFn supplies the buffer. The argument to this function
     *                   is the global processor index.
     * @param addToBufferFn adds an item to the buffer
     * @param flushBufferFn flushes the buffer
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBuffered(
            @Nonnull DistributedIntFunction<B> newBufferFn,
            @Nonnull DistributedBiConsumer<B, T> addToBufferFn,
            @Nonnull DistributedConsumer<B> flushBufferFn
    ) {
        return writeBuffered(newBufferFn, addToBufferFn, flushBufferFn, noopConsumer());
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
     * @param <B> type of buffer
     * @param <T> type of received item
     * @param newBufferFn supplies the buffer. The argument to this function
     *                   is the global processor index.
     * @param addToBufferFn adds item to buffer
     * @param flushBufferFn flushes the buffer
     * @param disposeBufferFn disposes of the buffer
     */
    @Nonnull
    public static <B, T> ProcessorSupplier writeBuffered(
            @Nonnull DistributedIntFunction<B> newBufferFn,
            @Nonnull DistributedBiConsumer<B, T> addToBufferFn,
            @Nonnull DistributedConsumer<B> flushBufferFn,
            @Nonnull DistributedConsumer<B> disposeBufferFn
    ) {
        return WriteBufferedP.supplier(newBufferFn, addToBufferFn, flushBufferFn, disposeBufferFn);
    }

    /**
     * Returns a supplier of processors for a vertex that connects to the
     * specified TCP socket and writes to it a string representation of the
     * items it receives. It converts an item to its string representation
     * using the supplied {@code toStringFn} function and encodes the string
     * using the supplied {@code Charset}. It follows each item with a newline
     * character.
     *
     * @param host the name of the host to connect to
     * @param port the port number to connect to
     * @param toStringFn a function that returns the string representation of an item
     * @param charset charset used to encode the string representation
     */
    public static <T> ProcessorSupplier writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull Charset charset
    ) {
        String charsetName = charset.name();
        return writeBuffered(
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
        );
    }

    /**
     * Returns a supplier of processors for a vertex that writes the items it
     * receives to files. Each processor will write to its own file whose name
     * is equal to the processor's global index (an integer unique to each
     * processor of the vertex), but a single pathname is used to resolve the
     * containing directory of all files, on all cluster members.
     * <p>
     * The vertex converts an item to its string representation using the
     * supplied {@code toStringFn} function and encodes the string using the
     * supplied {@code Charset}. It follows each item with a platform-specific
     * line separator.
     * <p>
     * Since the work of this vertex is file IO-intensive, {@link
     * com.hazelcast.jet.core.Vertex#localParallelism(int) local parallelism} of
     * the vertex should be set according to the performance characteristics of
     * the underlying storage system. Most typically, local parallelism of 1 will
     * already reach the maximum available performance.
     *
     * @param directoryName directory to create the files in. Will be created if it doesn't exist.
     * @param toStringFn a function that returns the string representation of an item
     * @param charset charset used to encode the string representation
     * @param append whether to append ({@code true}) or overwrite ({@code false})
     *               an existing file
     */
    @Nonnull
    public static <T> ProcessorSupplier writeFile(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull Charset charset,
            boolean append
    ) {
        return WriteFileP.supplier(directoryName, toStringFn, charset.name(), append);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all members.
     * @param toStringFn a function that returns the string representation of an item
     */
    @Nonnull
    public static <T> ProcessorSupplier writeFile(
            @Nonnull String directoryName, @Nonnull DistributedFunction<T, String> toStringFn
    ) {
        return writeFile(directoryName, toStringFn, UTF_8, false);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with {@code Object.toString()} as the conversion function,
     * UTF-8 as the charset and with overwriting of existing files.
     *
     * @param directoryName directory to create the files in. Will be created,
     *                      if it doesn't exist. Must be the same on all members.
     */
    @Nonnull
    public static ProcessorSupplier writeFile(@Nonnull String directoryName) {
        return writeFile(directoryName, Object::toString, UTF_8, false);
    }
}
