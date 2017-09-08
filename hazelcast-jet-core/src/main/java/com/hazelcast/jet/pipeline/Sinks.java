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

package com.hazelcast.jet.pipeline;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.impl.SinkImpl;
import com.hazelcast.jet.processor.SinkProcessors;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Sinks {

    private Sinks() {
    }

    /**
     * Returns a sink constructed directly from the given Core API processor
     * meta-supplier.
     *
     * @param sinkName user-friendly sink name
     * @param metaSupplier the processor meta-supplier
     */
    public static Sink fromProcessor(String sinkName, ProcessorMetaSupplier metaSupplier) {
        return new SinkImpl(sinkName, metaSupplier);
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code IMap} with the specified name.
     */
    public static Sink writeMap(String mapName) {
        return new SinkImpl("writeMap(" + mapName + ')', SinkProcessors.writeMap(mapName));
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code IMap} with the specified name in a remote cluster identified by
     * the supplied {@code ClientConfig}.
     */
    public static Sink writeMap(String mapName, ClientConfig clientConfig) {
        return new SinkImpl("writeMap(" + mapName + ')', SinkProcessors.writeMap(mapName, clientConfig));
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code ICache} with the specified name.
     */
    public static Sink writeCache(String cacheName) {
        return new SinkImpl("writeCache(" + cacheName + ')', SinkProcessors.writeCache(cacheName));
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code ICache} with the specified name in a remote cluster identified by
     * the supplied {@code ClientConfig}.
     */
    public static Sink writeCache(String cacheName, ClientConfig clientConfig) {
        return new SinkImpl("writeCache(" + cacheName + ')', SinkProcessors.writeCache(cacheName, clientConfig));
    }

    /**
     * Returns a sink that adds the items it receives to a Hazelcast {@code
     * IList} with the specified name.
     */
    public static Sink writeList(String listName) {
        return new SinkImpl("writeList(" + listName + ')', SinkProcessors.writeList(listName));
    }

    /**
     * Returns a sink that adds the items it receives to a Hazelcast {@code
     * IList} with the specified name in a remote cluster identified by the
     * supplied {@code ClientConfig}.
     */
    public static Sink writeList(String listName, ClientConfig clientConfig) {
        return new SinkImpl("writeList(" + listName + ')', SinkProcessors.writeList(listName, clientConfig));
    }

    /**
     * Returns a sink that connects to the specified TCP socket and writes to
     * it a string representation of the items it receives. It converts an
     * item to its string representation using the supplied {@code toStringF}
     * function and encodes the string using the supplied {@code Charset}. It
     * follows each item with a newline character.
     */
    public static <T> Sink writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull Charset charset
    ) {
        return new SinkImpl("writeSocket(" + host + ':' + port + ')',
                SinkProcessors.writeSocket(host, port, toStringF, charset));
    }

    /**
     * Convenience for {@link #writeSocket(String, int, DistributedFunction,
     * Charset)} with UTF-8 as the charset.
     */
    public static <T> Sink writeSocket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return new SinkImpl("writeSocket(" + host + ':' + port + ')',
                SinkProcessors.writeSocket(host, port, toStringF, UTF_8));
    }

    /**
     * Convenience for {@link #writeSocket(String, int, DistributedFunction,
     * Charset)} with {@code Object.toString} as the conversion function and
     * UTF-8 as the charset.
     */
    public static <T> Sink writeSocket(@Nonnull String host, int port) {
        return new SinkImpl("writeSocket(" + host + ':' + port + ')',
                SinkProcessors.writeSocket(host, port, Object::toString, UTF_8));
    }

    /**
     * Returns a sink that that writes the items it receives to files. Each
     * processor will write to its own file whose name is equal to the
     * processor's global index (an integer unique to each processor of the
     * vertex), but a single pathname is used to resolve the containing
     * directory of all files, on all cluster members.
     * <p>
     * The sink converts an item to its string representation using the
     * supplied {@code toStringF} function and encodes the string using the
     * supplied {@code Charset}. It follows each item with a platform-specific
     * line separator.
     *
     * @param directoryName directory to create the files in. Will be created
     *                      if it doesn't exist. Must be the same on all members.
     * @param toStringF a function to convert items to String (a formatter)
     * @param charset charset used to encode the file output
     * @param append whether to append ({@code true}) or overwrite ({@code false})
     *               an existing file
     */
    @Nonnull
    public static <T> Sink writeFile(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringF,
            @Nonnull Charset charset,
            boolean append
    ) {
        return new SinkImpl("writeFile(" + directoryName + ')',
                SinkProcessors.writeFile(directoryName, toStringF, charset, append));
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static <T> Sink writeFile(
            @Nonnull String directoryName, @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return writeFile(directoryName, toStringF, UTF_8, false);
    }

    /**
     * Convenience for {@link #writeFile(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static Sink writeFile(@Nonnull String directoryName) {
        return writeFile(directoryName, Object::toString, UTF_8, false);
    }
}
