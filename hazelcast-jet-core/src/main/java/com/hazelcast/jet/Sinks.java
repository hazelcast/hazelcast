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
import com.hazelcast.core.Offloadable;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.SinkImpl;
import com.hazelcast.map.EntryProcessor;
import java.nio.charset.Charset;
import java.util.Map;
import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.processor.SinkProcessors.mergeMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.mergeRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeSocketP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
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
     * Returns a sink that puts {@code Map.Entry}s it receives into a Hazelcast
     * {@code IMap} with the specified name.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     */
    public static <E extends Map.Entry> Sink<E> map(String mapName) {
        return fromProcessor("mapSink(" + mapName + ')', writeMapP(mapName));
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
        return fromProcessor("remoteMapSink(" + mapName + ')', writeRemoteMapP(mapName, clientConfig));
    }

    /**
     * Returns a sink that uses the supplied functions to extract the key
     * and value with which to update a Hazelcast {@code IMap}. If the map
     * already contains the key, it applies the given {@code mergeFn} to
     * resolve the existing and the proposed value into the value to use. If
     * the value comes out as {@code null}, it removes the key from the map.
     * Expressed as code, the sink performs the equivalent of the following for
     * each item:
     * <pre>
     * K key = toKeyFn.apply(item);
     * V oldValue = map.get(key);
     * V newValue = toValueFn.apply(item);
     * V resolved = (oldValue == null)
     *            ? newValue
                  : mergeFn.apply(oldValue, newValue);
     * if (value == null)
     *     map.remove(key);
     * else
     *     map.put(key, value);
     * </pre>
     *
     * @param mapName   name of the map
     * @param toKeyFn   function that extracts the key from the input item
     * @param toValueFn function that extracts the value from the input item
     * @param mergeFn   function that merges the existing value with the value acquired from the
     *                  received item
     * @param <E> input item type
     * @param <K> key type
     * @param <V> value type
     */
    public static <E, K, V> Sink<E> mapWithMerging(
            @Nonnull String mapName,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedFunction<E, V> toValueFn,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        return fromProcessor("mapWithMergingSink(" + mapName + ')',
                mergeMapP(mapName, toKeyFn, toValueFn,  mergeFn));
    }

    /**
     * Returns a sink equivalent to {@link #mapWithMerging}, but for a map
     * in a remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     */
    public static <E, K, V> Sink<E> remoteMapWithMerging(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedFunction<E, V> toValueFn,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        return fromProcessor("remoteMapWithMergingSink(" + mapName + ')',
                mergeRemoteMapP(mapName, clientConfig, toKeyFn, toValueFn, mergeFn));
    }

    /**
     * Convenience for {@link #mapWithMerging} with {@link Map.Entry} as
     * input item.
     */
    public static <E extends Map.Entry, V> Sink<E> mapWithMerging(
            @Nonnull String mapName,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        return fromProcessor("mapWithMergingSink(" + mapName + ')',
                mergeMapP(mapName, entryKey(), entryValue(), mergeFn));
    }

    /**
     * Convenience for {@link #remoteMapWithMerging} with {@link Map.Entry} as
     * input item.
     */
    public static <E extends Map.Entry, V> Sink<E> remoteMapWithMerging(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedBinaryOperator<V> mergeFn
    ) {
        return fromProcessor("remoteMapWithMergingSink(" + mapName + ')',
                mergeRemoteMapP(mapName, clientConfig, entryKey(), entryValue(), mergeFn));
    }

    /**
     * Returns a sink that uses the supplied key-extracting and value-updating
     * functions to update a Hazelcast {@code IMap}. For each item it receives, it
     * applies {@code toKeyFn} to get the key and then applies {@code updateFn} to
     * the existing value in the map and the received item to acquire the new
     * value to associate with the key. If the new value is {@code null}, it
     * removes the key from the map. Expressed as code, the sink performs the
     * equivalent of the following for each item:
     * <pre>
     * K key = toKeyFn.apply(item);
     * V oldValue = map.get(key);
     * V newValue = updateFn.apply(oldValue, item);
     * if (newValue == null)
     *     map.remove(key);
     * else
     *     map.put(key, newValue);
     * </pre>
     *
     * @param mapName   name of the map
     * @param toKeyFn   function that extracts the key from the input item
     * @param updateFn  function that receives the existing map value and the item
     *                  and returns the new map value
     * @param <E> input item type
     * @param <K> key type
     * @param <V> value type
     */
    public static <E, K, V> Sink<E> mapWithUpdating(
            @Nonnull String mapName,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedBiFunction<V, E, V> updateFn
    ) {
        return fromProcessor("mapWithUpdatingSink(" + mapName + ')', updateMapP(mapName, toKeyFn, updateFn));
    }

    /**
     * Returns a sink equivalent to {@link #mapWithUpdating}, but for a map
     * in a remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     */
    public static <E, K, V> Sink<E> remoteMapWithUpdating(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedBiFunction<V, E, V> updateFn

    ) {
        return fromProcessor("remoteMapWithUpdatingSink(" + mapName + ')',
                updateRemoteMapP(mapName, clientConfig, toKeyFn, updateFn));
    }

    /**
     * Convenience for {@link #mapWithUpdating} with {@link Map.Entry} as
     * input item.
     */
    public static <E extends Map.Entry, V> Sink<E> mapWithUpdating(
            @Nonnull String mapName,
            @Nonnull DistributedBiFunction<V, E, V> updateFn
    ) {
        return fromProcessor("mapWithUpdatingSink(" + mapName + ')', updateMapP(mapName, Map.Entry::getKey,
                updateFn));
    }

    /**
     * Convenience for {@link #remoteMapWithUpdating} with {@link Map.Entry} as
     * input item.
     */
    public static <E extends Map.Entry, V> Sink<E> remoteMapWithUpdating(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedBiFunction<V, E, V> updateFn

    ) {
        return fromProcessor("remoteMapWithUpdatingSink(" + mapName + ')',
                updateRemoteMapP(mapName, clientConfig, Map.Entry::getKey, updateFn));
    }

    /**
     * Returns a sink that uses the items it receives to create {@code
     * EntryProcessor}s it submits to a Hazelcast {@code IMap} with the
     * specified name. For each received item it applies {@code toKeyFn} to
     * get the key and {@code toEntryProcessorFn} to get the entry processor,
     * and then submits the key and the entry processor to the Hazelcast
     * cluster, which will internally apply the entry processor to the key.
     * <p>
     * As opposed to {@link #mapWithUpdating} and {@link #mapWithMerging},
     * this sink does not use batching and submits a separate entry processor
     * for each received item. For use cases that are efficiently solvable
     * using those sinks, this one will perform worse. Its main advantage is
     * that it can update large map values in a data-local manner, without
     * having to retrieve them first.
     * <p>
     * If your entry processors take a long time to update a value, consider
     * using entry processors that implement {@link Offloadable}. This will
     * avoid blocking the Hazelcast partition thread during large update
     * operations.
     *
     * @param mapName  name of the map
     * @param toKeyFn  function that extracts the key from the input item
     * @param toEntryProcessorFn function that returns the {@code EntryProcessor}
     *                           to apply to the key
     * @param <E> input item type
     * @param <K> key type
     * @param <V> value type
     */
    public static <E, K, V> Sink<E> mapWithEntryProcessor(
            @Nonnull String mapName,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedFunction<E, EntryProcessor<K, V>> toEntryProcessorFn

    ) {
        return fromProcessor("mapWithEntryProcessorSink(" + mapName + ')',
                updateMapP(mapName, toKeyFn, toEntryProcessorFn));
    }

    /**
     * Returns a sink equivalent to {@link #mapWithEntryProcessor}, but for a map
     * in a remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     */
    public static <E, K, V> Sink<E> remoteMapWithEntryProcessor(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull DistributedFunction<E, K> toKeyFn,
            @Nonnull DistributedFunction<E, EntryProcessor<K, V>> toEntryProcessorFn

    ) {
        return fromProcessor("remoteMapWithEntryProcessorSink(" + mapName + ')',
                updateRemoteMapP(mapName, clientConfig, toKeyFn, toEntryProcessorFn));
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
        return fromProcessor("cacheSink(" + cacheName + ')', writeCacheP(cacheName));
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
        return fromProcessor("remoteCacheSink(" + cacheName + ')', writeRemoteCacheP(cacheName, clientConfig));
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
        return fromProcessor("listSink(" + listName + ')', writeListP(listName));
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
        return fromProcessor("remoteListSink(" + listName + ')', writeRemoteListP(listName, clientConfig));
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
        return fromProcessor("socketSink(" + host + ':' + port + ')', writeSocketP(host, port, toStringFn, charset));
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
        return fromProcessor("socketSink(" + host + ':' + port + ')', writeSocketP(host, port, toStringFn, UTF_8));
    }

    /**
     * Convenience for {@link #socket(String, int, DistributedFunction,
     * Charset)} with {@code Object.toString} as the conversion function and
     * UTF-8 as the charset.
     */
    public static <E> Sink<E> socket(@Nonnull String host, int port) {
        return fromProcessor("socketSink(" + host + ':' + port + ')',
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
    public static <E> Sink<E> files(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<E, String> toStringFn,
            @Nonnull Charset charset,
            boolean append
    ) {
        return fromProcessor("filesSink(" + directoryName + ')',
                writeFileP(directoryName, toStringFn, charset, append));
    }

    /**
     * Convenience for {@link #files(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static <E> Sink<E> files(
            @Nonnull String directoryName, @Nonnull DistributedFunction<E, String> toStringFn
    ) {
        return files(directoryName, toStringFn, UTF_8, false);
    }

    /**
     * Convenience for {@link #files(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static <E> Sink<E> files(@Nonnull String directoryName) {
        return files(directoryName, Object::toString, UTF_8, false);
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
    public static <E> Sink<E> logger(DistributedFunction<E, String> toStringFn) {
        return fromProcessor("loggerSink", writeLoggerP(toStringFn));
    }

    /**
     * Convenience for {@link #logger(DistributedFunction)} with {@code
     * Object.toString()} as the {@code toStringFn}.
     */
    @Nonnull
    public static <E> Sink<E> logger() {
        return logger(Object::toString);
    }
}
