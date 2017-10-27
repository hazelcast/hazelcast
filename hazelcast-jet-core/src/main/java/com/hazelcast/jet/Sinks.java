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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.SinkImpl;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.Map;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeSocketP;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sinks. Formally,
 * a sink is a transform that has no output. A pipeline stage with a sink
 * as its transform has the type {@link SinkStage} and accepts no
 * downstream stages.
 */
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
    public static <E> Sink<E> fromProcessor(String sinkName, ProcessorMetaSupplier metaSupplier) {
        return new SinkImpl<>(sinkName, metaSupplier);
    }

    /**
     * Returns a sink constructed directly from the given Core API processor
     * supplier.
     *
     * @param sinkName user-friendly sink name
     * @param supplier the processor supplier
     */
    public static <E> Sink<E> fromProcessor(String sinkName, ProcessorSupplier supplier) {
        return new SinkImpl<>(sinkName, supplier);
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code IMap} with the specified name.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     */
    public static <E extends Map.Entry> Sink<E> map(String mapName) {
        return fromProcessor("writeMap(" + mapName + ')', writeMapP(mapName));
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code IMap} with the specified name in a remote cluster identified by
     * the supplied {@code ClientConfig}.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     */
    public static <E extends Map.Entry> Sink<E> remoteMap(String mapName, ClientConfig clientConfig) {
        return fromProcessor("writeRemoteMap(" + mapName + ')', writeRemoteMapP(mapName, clientConfig));
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code ICache} with the specified name.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     */
    public static <E extends Map.Entry> Sink<E> cache(String cacheName) {
        return fromProcessor("writeCache(" + cacheName + ')', writeCacheP(cacheName));
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code ICache} with the specified name in a remote cluster identified by
     * the supplied {@code ClientConfig}.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     */
    public static <E extends Map.Entry> Sink<E> remoteCache(String cacheName, ClientConfig clientConfig) {
        return fromProcessor("writeRemoteCache(" + cacheName + ')', writeRemoteCacheP(cacheName, clientConfig));
    }

    /**
     * Returns a sink that adds the items it receives to a Hazelcast {@code
     * IList} with the specified name.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     */
    public static <E> Sink<E> list(String listName) {
        return fromProcessor("writeList(" + listName + ')', writeListP(listName));
    }

    /**
     * Returns a sink that adds the items it receives to a Hazelcast {@code
     * IList} with the specified name in a remote cluster identified by the
     * supplied {@code ClientConfig}.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     */
    public static <E> Sink<E> remoteList(String listName, ClientConfig clientConfig) {
        return fromProcessor("writeRemoteList(" + listName + ')', writeRemoteListP(listName, clientConfig));
    }

    /**
     * Returns a sink that connects to the specified TCP socket and writes to
     * it a string representation of the items it receives. It converts an
     * item to its string representation using the supplied {@code toStringFn}
     * function and encodes the string using the supplied {@code Charset}. It
     * follows each item with a newline character.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     */
    public static <E> Sink<E> socket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<E, String> toStringFn,
            @Nonnull Charset charset
    ) {
        return fromProcessor("writeSocket(" + host + ':' + port + ')', writeSocketP(host, port, toStringFn, charset));
    }

    /**
     * Convenience for {@link #socket(String, int, DistributedFunction,
     * Charset)} with UTF-8 as the charset.
     */
    public static <E> Sink<E> socket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<E, String> toStringFn
    ) {
        return fromProcessor("writeSocket(" + host + ':' + port + ')', writeSocketP(host, port, toStringFn, UTF_8));
    }

    /**
     * Convenience for {@link #socket(String, int, DistributedFunction,
     * Charset)} with {@code Object.toString} as the conversion function and
     * UTF-8 as the charset.
     */
    public static <E> Sink<E> socket(@Nonnull String host, int port) {
        return fromProcessor("writeSocket(" + host + ':' + port + ')',
                writeSocketP(host, port, Object::toString, UTF_8));
    }

    /**
     * Returns a sink that that writes the items it receives to files. Each
     * processor will write to its own file whose name is equal to the
     * processor's global index (an integer unique to each processor of the
     * vertex), but a single pathname is used to resolve the containing
     * directory of all files, on all cluster members.
     * <p>
     * The sink converts an item to its string representation using the
     * supplied {@code toStringFn} function and encodes the string using the
     * supplied {@code Charset}. It follows each item with a platform-specific
     * line separator.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     *
     * @param directoryName directory to create the files in. Will be created
     *                      if it doesn't exist. Must be the same on all members.
     * @param toStringFn a function to convert items to String (a formatter)
     * @param charset charset used to encode the file output
     * @param append whether to append ({@code true}) or overwrite ({@code false})
     *               an existing file
     */
    @Nonnull
    public static <E> Sink<E> file(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<E, String> toStringFn,
            @Nonnull Charset charset,
            boolean append
    ) {
        return fromProcessor("writeFile(" + directoryName + ')',
                writeFileP(directoryName, toStringFn, charset, append));
    }

    /**
     * Convenience for {@link #file(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static <E> Sink<E> file(
            @Nonnull String directoryName, @Nonnull DistributedFunction<E, String> toStringFn
    ) {
        return file(directoryName, toStringFn, UTF_8, false);
    }

    /**
     * Convenience for {@link #file(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static <E> Sink<E> file(@Nonnull String directoryName) {
        return file(directoryName, Object::toString, UTF_8, false);
    }

    /**
     * Returns a sink that logs all the data items it receives, at the INFO
     * level to the log category {@link
     * com.hazelcast.jet.impl.connector.WriteLoggerP}. It doesn't log {@link
     * com.hazelcast.jet.core.Watermark watermark} items.
     * <p>
     * The sink logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @param toStringFn a function that returns a string representation of a stream item
     * @param <E> stream item type
     */
    @Nonnull
    public static <E> Sink<E> writeLogger(DistributedFunction<E, String> toStringFn) {
        return fromProcessor("writeLogger", writeLoggerP(toStringFn));
    }

    /**
     * Convenience for {@link #writeLogger(DistributedFunction)} with {@code
     * Object.toString()} as the {@code toStringFn}.
     */
    @Nonnull
    public static <E> Sink<E> writeLogger() {
        return writeLogger(Object::toString);

    }
}
