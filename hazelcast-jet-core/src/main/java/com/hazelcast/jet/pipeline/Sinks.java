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

package com.hazelcast.jet.pipeline;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Offloadable;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.map.EntryProcessor;

import javax.annotation.Nonnull;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.nio.charset.Charset;
import java.util.Map;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeJmsQueueP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeJmsTopicP;
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
 * a sink transform is one which has no output. A pipeline stage with a sink
 * transform has the type {@link SinkStage} and accepts no downstream stages.
 * <p>
 * The default local parallelism for the sinks in this class is typically 1,
 * check the documentation of individual methods.
 */
public final class Sinks {

    private Sinks() {
    }

    /**
     * Returns a sink constructed directly from the given Core API processor
     * meta-supplier.
     * <p>
     * The default local parallelism for this source is specified inside the
     * {@link ProcessorMetaSupplier#preferredLocalParallelism() metaSupplier}.
     *
     * @param sinkName user-friendly sink name
     * @param metaSupplier the processor meta-supplier
     */
    @Nonnull
    public static <T> Sink<T> fromProcessor(
            @Nonnull String sinkName,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
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
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T extends Map.Entry> Sink<T> map(@Nonnull String mapName) {
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
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T extends Map.Entry> Sink<T> remoteMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
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
     * This sink supports exactly-once processing only if the
     * supplied merge function performs <i>idempotent updates</i>, i.e.,
     * it satisfies the rule
     * {@code mergeFn.apply(oldValue, toValueFn.apply(e)).equals(oldValue)}
     * for any {@code e} that was already observed.
     * <p>
     * <b>Note:</b> This operation is <em>NOT</em> lock-aware, it will process the
     * entries no matter if they are locked or not. Use {@link #mapWithEntryProcessor}
     * if you need locking.
     * <p>
     * The default local parallelism for this sink is 1.
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
    @Nonnull
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
    @Nonnull
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
    @Nonnull
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
    @Nonnull
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
     * This sink supports exactly-once processing only if the
     * supplied update function performs <i>idempotent updates</i>, i.e., it
     * satisfies the rule {@code updateFn.apply(v, e).equals(v)} for any
     * {@code e} that was already observed.
     * <p>
     * <b>Note:</b> This operation is <em>NOT</em> lock-aware, it will process the entries
     * no matter if they are locked or not.
     * Use {@link #mapWithEntryProcessor} if you need locking.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param mapName  name of the map
     * @param toKeyFn  function that extracts the key from the input item
     * @param updateFn function that receives the existing map value and the item
     *                 and returns the new map value
     * @param <E>      input item type
     * @param <K>      key type
     * @param <V>      value type
     */
    @Nonnull
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
    @Nonnull
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
    @Nonnull
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
    @Nonnull
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
     * using those sinks, this one will perform worse. It should be used only
     * when they are not applicable.
     * <p>
     * If your entry processors take a long time to update a value, consider
     * using entry processors that implement {@link Offloadable}. This will
     * avoid blocking the Hazelcast partition thread during large update
     * operations.
     * <p>
     * This sink supports exactly-once processing only if the
     * supplied entry processor performs <i>idempotent updates</i>, i.e.,
     * the resulting value would be the same if an entry processor
     * was run on the same entry more than once.
     * <p>
     * <b>Note:</b> Unlike {@link #mapWithUpdating} and {@link #mapWithMerging},
     * this operation <em>is</em> lock-aware. If the key is locked,
     * the EntryProcessor will wait until it acquires the lock.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param mapName  name of the map
     * @param toKeyFn  function that extracts the key from the input item
     * @param toEntryProcessorFn function that returns the {@code EntryProcessor}
     *                           to apply to the key
     * @param <E> input item type
     * @param <K> key type
     * @param <V> value type
     */
    @Nonnull
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
    @Nonnull
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
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T extends Map.Entry> Sink<T> cache(@Nonnull String cacheName) {
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
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T extends Map.Entry> Sink<T> remoteCache(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        return fromProcessor("remoteCacheSink(" + cacheName + ')', writeRemoteCacheP(cacheName, clientConfig));
    }

    /**
     * Returns a sink that adds the items it receives to a Hazelcast {@code
     * IList} with the specified name.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T> Sink<T> list(@Nonnull String listName) {
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
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T> Sink<T> remoteList(@Nonnull String listName, @Nonnull ClientConfig clientConfig) {
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
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T> Sink<T> socket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringFn,
            @Nonnull Charset charset
    ) {
        return fromProcessor("socketSink(" + host + ':' + port + ')', writeSocketP(host, port, toStringFn, charset));
    }

    /**
     * Convenience for {@link #socket(String, int, DistributedFunction,
     * Charset)} with UTF-8 as the charset.
     */
    @Nonnull
    public static <T> Sink<T> socket(
            @Nonnull String host,
            int port,
            @Nonnull DistributedFunction<T, String> toStringFn
    ) {
        return socket(host, port, toStringFn, UTF_8);
    }

    /**
     * Convenience for {@link #socket(String, int, DistributedFunction,
     * Charset)} with {@code Object.toString} as the conversion function and
     * UTF-8 as the charset.
     */
    @Nonnull
    public static <T> Sink<T> socket(@Nonnull String host, int port) {
        return socket(host, port, Object::toString);
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
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param directoryName directory to create the files in. Will be created
     *                      if it doesn't exist. Must be the same on all members.
     * @param toStringFn a function to convert items to String (a formatter)
     * @param charset charset used to encode the file output
     * @param append whether to append ({@code true}) or overwrite ({@code false})
     *               an existing file
     */
    @Nonnull
    public static <T> Sink<T> files(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringFn,
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
    public static <T> Sink<T> files(
            @Nonnull String directoryName,
            @Nonnull DistributedFunction<T, String> toStringFn
    ) {
        return files(directoryName, toStringFn, UTF_8, false);
    }

    /**
     * Convenience for {@link #files(String, DistributedFunction, Charset,
     * boolean)} with the UTF-8 charset and with overwriting of existing files.
     */
    @Nonnull
    public static <T> Sink<T> files(@Nonnull String directoryName) {
        return files(directoryName, Object::toString, UTF_8, false);
    }

    /**
     * Returns a sink that logs all the data items it receives, at the INFO
     * level to the log category {@link
     * com.hazelcast.jet.impl.connector.WriteLoggerP}. It also logs {@link
     * com.hazelcast.jet.core.Watermark watermark} items, but at FINE level.
     * <p>
     * The sink logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development, when running Jet on
     * a local machine.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param toStringFn a function that returns a string representation of a stream item
     * @param <T> stream item type
     */
    @Nonnull
    public static <T> Sink<T> logger(@Nonnull DistributedFunction<T, String> toStringFn) {
        return fromProcessor("loggerSink", writeLoggerP(toStringFn));
    }

    /**
     * Convenience for {@link #logger(DistributedFunction)} with {@code
     * Object.toString()} as the {@code toStringFn}.
     */
    @Nonnull
    public static <T> Sink<T> logger() {
        return logger(Object::toString);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom {@link Sink} for the Pipeline API. It allows you to keep a
     * single-threaded, stateful writer object in each instance of a Jet worker
     * dedicated to driving the sink. Its primary intended purpose is to serve
     * as the holder of references to external resources and optional buffers.
     * Keep in mind that only the writer object may be stateful; the functions
     * you provide must hold no mutable state of their own.
     * <p>
     * These are the callback functions you can provide to implement the sink's
     * behavior:
     * <ol><li>
     *     {@code createFn} creates the writer. Gets the processor context as
     *     argument which can be used to obtain local Jet instance, global
     *     processor index etc. It will be called once for each worker thread.
     *     This component is required.
     * </li><li>
     *     {@code onReceiveFn} gets notified of each item the sink receives and
     *     (typically) passes it to the writer. This component is required.
     * </li><li>
     *     {@code flushFn} flushes the writer. This component is optional.
     * </li><li>
     *     {@code destroyFn} destroys the writer. This component is optional.
     * </li></ol>
     * The returned sink will be non-cooperative and will have preferred local
     * parallelism of 2. It also cannot participate in state snapshot saving
     * (fault-tolerance): it will behave as an at-least-once sink.
     *
     * @param <W> type of the writer object
     * @param <T> type of the items the sink will accept
     */
    @Nonnull
    public static <W, T> SinkBuilder<W, T> builder(
            @Nonnull DistributedFunction<Processor.Context, ? extends W> createFn
    ) {
        return new SinkBuilder<>(createFn);
    }

    /**
     * Returns a sink which discards all received items.
     */
    @Nonnull
    public static <T> Sink<T> noop() {
        return fromProcessor("noop", preferLocalParallelismOne(noopP()));
    }

    /**
     * Returns a sink which connects to a JMS provider and sends messages to the
     * specified JMS queue.
     * <p>
     * Sink creates a single connection for each member using the given {@code
     * connectionSupplier} and then creates a session and producer for each
     * {@link com.hazelcast.jet.core.Processor processor} using the given
     * {@code sessionFn} and {@code name}.
     * <p>
     * Sink converts an item to a {@link Message} using the given {@code
     * messageFn} and sends this message using the given {@code sendFn}. After
     * a batch of messages is sent, sink flushes the session using the given
     * {@code flushFn}.
     * <p>
     * Behavior on job restart: the processor is stateless. If the job is
     * restarted, duplicate events can occur. If you need exactly-once behavior,
     * you must ensure idempotence on the application level.
     * <p>
     * IO failures are generally handled by JMS provider and do not cause the
     * processor to fail. Most of the providers offer a configuration parameter
     * to enable auto-reconnection, refer to provider documentation for details.
     * <p>
     * Default local parallelism for this processor is 4 (or less if less CPUs
     * are available).
     *
     * @param connectionSupplier supplier to obtain connection to the JMS provider
     * @param sessionF           function to create session from the JMS connection
     * @param messageFn          function to create message from the item
     * @param sendFn             function to send the message to JMS queue
     * @param flushFn            function to commit the session for transacted sessions
     * @param name               the name of the queue
     */
    @Nonnull
    public static <T> Sink<T> jmsQueue(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Session> sessionF,
            @Nonnull DistributedBiFunction<Session, T, Message> messageFn,
            @Nonnull DistributedBiConsumer<MessageProducer, Message> sendFn,
            @Nonnull DistributedConsumer<Session> flushFn,
            @Nonnull String name
    ) {
        return fromProcessor("jmsQueueSink(" + name + ")",
                SinkProcessors.writeJmsQueueP(connectionSupplier, sessionF, messageFn, sendFn, flushFn, name));
    }

    /**
     * Convenience for {@link #jmsQueue(DistributedSupplier,
     * DistributedFunction, DistributedBiFunction, DistributedBiConsumer,
     * DistributedConsumer, String)}. Sink creates a connection without any
     * authentication parameters and uses non-transacted sessions with {@code
     * Session.AUTO_ACKNOWLEDGE} mode. Sink wraps {@code item.toString()} into
     * a {@link javax.jms.TextMessage}
     *
     * @param factorySupplier supplier to obtain JMS connection factory
     * @param name            the name of the queue
     */
    @Nonnull
    public static <T> Sink<T> jmsQueue(
            @Nonnull DistributedSupplier<ConnectionFactory> factorySupplier,
            @Nonnull String name
    ) {
        return fromProcessor("jmsQueueSink(" + name + ")", writeJmsQueueP(factorySupplier, name));
    }

    /**
     * Returns a sink which connects to a JMS provider and sends messages to the
     * specified JMS topic.
     * <p>
     * Sink creates a single connection for each member using the given {@code
     * connectionSupplier} and then creates a session and producer for each
     * {@link com.hazelcast.jet.core.Processor processor} using the given
     * {@code sessionFn} and {@code name}.
     * <p>
     * Sink converts an item to a {@link Message} using the given {@code
     * messageFn} and sends this message using the given {@code sendFn}. After
     * a batch of messages is sent, sink flushes the session using the given
     * {@code flushFn}.
     * <p>
     * Behavior on job restart: the processor is stateless. If the job is
     * restarted, duplicate events can occur. If you need exactly-once behavior,
     * you must ensure idempotence on the application level.
     * <p>
     * IO failures are generally handled by JMS provider and do not cause the
     * processor to fail. Most of the providers offer a configuration parameter
     * to enable auto-reconnection, refer to provider documentation for details.
     * <p>
     * Default local parallelism for this processor is 4 (or less if less CPUs
     * are available).
     *
     * @param connectionSupplier supplier to obtain connection to the JMS provider
     * @param sessionF           function to create session from the JMS connection
     * @param messageFn          function to create message from the item
     * @param sendFn             function to send the message to JMS queue
     * @param flushFn            function to commit the session for transacted sessions
     * @param name               the name of the topic
     */
    @Nonnull
    public static <T> Sink<T> jmsTopic(
            @Nonnull DistributedSupplier<Connection> connectionSupplier,
            @Nonnull DistributedFunction<Connection, Session> sessionF,
            @Nonnull DistributedBiFunction<Session, T, Message> messageFn,
            @Nonnull DistributedBiConsumer<MessageProducer, Message> sendFn,
            @Nonnull DistributedConsumer<Session> flushFn,
            @Nonnull String name
    ) {
        return fromProcessor("jmsTopicSink(" + name + ")",
                SinkProcessors.writeJmsTopicP(connectionSupplier, sessionF, messageFn, sendFn, flushFn, name));
    }

    /**
     * Convenience for {@link #jmsTopic(DistributedSupplier,
     * DistributedFunction, DistributedBiFunction, DistributedBiConsumer,
     * DistributedConsumer, String)}. Sink creates a connection without any
     * authentication parameters and uses non-transacted sessions with {@code
     * Session.AUTO_ACKNOWLEDGE} mode. Sink wraps {@code item.toString()} into
     * a {@link javax.jms.TextMessage}
     *
     * @param factorySupplier supplier to obtain JMS connection factory
     * @param name            the name of the queue
     */
    @Nonnull
    public static <T> Sink<T> jmsTopic(
            @Nonnull DistributedSupplier<ConnectionFactory> factorySupplier,
            @Nonnull String name
    ) {
        return fromProcessor("jmsTopicSink(" + name + ")", writeJmsTopicP(factorySupplier, name));
    }

}
