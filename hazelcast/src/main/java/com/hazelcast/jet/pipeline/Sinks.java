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

package com.hazelcast.jet.pipeline;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.security.permission.ReliableTopicPermission;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jms.ConnectionFactory;
import javax.sql.CommonDataSource;
import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Map.Entry;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.core.processor.SinkProcessors.mergeMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.mergeRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.updateRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteCacheP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteListP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeRemoteMapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeSocketP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUBLISH;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for various types of pipeline sinks. Formally,
 * a sink transform is one which has no output. A pipeline stage with a sink
 * transform has the type {@link SinkStage} and accepts no downstream stages.
 * <p>
 * The default local parallelism for the sinks in this class is typically 1,
 * check the documentation of individual methods.
 *
 * @since Jet 3.0
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
     * Returns a sink constructed directly from the given Core API processor
     * meta-supplier.
     * <p>
     * The default local parallelism for this source is specified inside the
     * {@link ProcessorMetaSupplier#preferredLocalParallelism() metaSupplier}.
     *
     * @param sinkName user-friendly sink name
     * @param metaSupplier the processor meta-supplier
     * @param partitionKeyFn key extractor function for partitioning edges to
     *     sink. It must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     */
    @Nonnull
    public static <T> Sink<T> fromProcessor(
            @Nonnull String sinkName,
            @Nonnull ProcessorMetaSupplier metaSupplier,
            @Nullable FunctionEx<? super T, ?> partitionKeyFn
    ) {
        return new SinkImpl<>(sinkName, metaSupplier, partitionKeyFn);
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
    public static <K, V> Sink<Entry<K, V>> map(@Nonnull String mapName) {
        return map(mapName, Entry::getKey, Entry::getValue);
    }

    /**
     * Returns a sink that puts {@code Map.Entry}s it receives into the given
     * Hazelcast {@code IMap}.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> map(@Nonnull IMap<? super K, ? super V> map) {
        return map(map.getName());
    }


    /**
     * Returns a sink that uses the supplied functions to extract the key
     * and value with which to put to a Hazelcast {@code IMap} with the
     * specified name.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <T, K, V> Sink<T> map(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn

    ) {
        return new SinkImpl<>("mapSink(" + mapName + ')',
                writeMapP(mapName, toKeyFn, toValueFn), toKeyFn);
    }

    /**
     * Returns a sink that uses the supplied functions to extract the key
     * and value with which to put to given Hazelcast {@code IMap}.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <T, K, V> Sink<T> map(
            @Nonnull IMap<? super K, ? super V> map,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn

    ) {
        return map(map.getName(), toKeyFn, toValueFn);
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
    public static <K, V> Sink<Entry<K, V>> remoteMap(@Nonnull String mapName, @Nonnull ClientConfig clientConfig) {
        return remoteMap(mapName, clientConfig, Entry::getKey, Entry::getValue);
    }

    /**
     * Returns a sink that uses the supplied functions to extract the key
     * and value with which to put to a Hazelcast {@code IMap} in a remote
     * cluster identified by the supplied {@code ClientConfig}.
     * <p>
     * This sink provides the exactly-once guarantee thanks to <i>idempotent
     * updates</i>. It means that the value with the same key is not appended,
     * but overwritten. After the job is restarted from snapshot, duplicate
     * items will not change the state in the target map.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <T, K, V> Sink<T> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        return fromProcessor("remoteMapSink(" + mapName + ')',
                writeRemoteMapP(mapName, clientConfig, toKeyFn, toValueFn),
                toKeyFn);
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
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param mapName   name of the map
     * @param toKeyFn   function that extracts the key from the input item
     * @param toValueFn function that extracts the value from the input item
     * @param mergeFn   function that merges the existing value with the value acquired from the
     *                  received item
     * @param <T> input item type
     * @param <K> key type
     * @param <V> value type
     */
    @Nonnull
    public static <T, K, V> Sink<T> mapWithMerging(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return new SinkImpl<>("mapWithMergingSink(" + mapName + ')',
                mergeMapP(mapName, toKeyFn, toValueFn,  mergeFn), toKeyFn);
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
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
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
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param map       the map to drain to
     * @param toKeyFn   function that extracts the key from the input item
     * @param toValueFn function that extracts the value from the input item
     * @param mergeFn   function that merges the existing value with the value acquired from the
     *                  received item
     * @param <T> input item type
     * @param <K> key type
     * @param <V> value type
     */
    @Nonnull
    public static <T, K, V> Sink<T> mapWithMerging(
            @Nonnull IMap<? super K, ? super V> map,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return mapWithMerging(map.getName(), toKeyFn, toValueFn, mergeFn);
    }

    /**
     * Returns a sink equivalent to {@link #mapWithMerging(String, BinaryOperatorEx)},
     * but for a map in a remote Hazelcast cluster identified by the supplied
     * {@code ClientConfig}.
     * <p>
     * Due to the used API, the remote cluster must be at least version 4.0.
     */
    @Nonnull
    public static <T, K, V> Sink<T> remoteMapWithMerging(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return fromProcessor("remoteMapWithMergingSink(" + mapName + ')',
                mergeRemoteMapP(mapName, clientConfig, toKeyFn, toValueFn, mergeFn),
                toKeyFn);
    }

    /**
     * Convenience for {@link #mapWithMerging(String, FunctionEx, FunctionEx,
     * BinaryOperatorEx)} with {@link Entry} as input item.
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> mapWithMerging(
            @Nonnull String mapName,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return Sinks.<Entry<K, V>, K, V>mapWithMerging(mapName, entryKey(), entryValue(), mergeFn);
    }

    /**
     * Convenience for {@link #mapWithMerging(IMap, FunctionEx, FunctionEx,
     * BinaryOperatorEx)} with {@link Entry} as input item.
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> mapWithMerging(
            @Nonnull IMap<? super K, V> map,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return mapWithMerging(map.getName(), mergeFn);
    }

    /**
     * Convenience for {@link #remoteMapWithMerging} with {@link Entry} as
     * input item.
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> remoteMapWithMerging(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull BinaryOperatorEx<V> mergeFn
    ) {
        return fromProcessor("remoteMapWithMergingSink(" + mapName + ')',
                mergeRemoteMapP(mapName, clientConfig, Entry::getKey, entryValue(), mergeFn));
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
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param mapName  name of the map
     * @param toKeyFn  function that extracts the key from the input item
     * @param updateFn function that receives the existing map value and the item
     *                 and returns the new map value
     * @param <T>      input item type
     * @param <K>      key type
     * @param <V>      value type
     */
    @Nonnull
    public static <T, K, V> Sink<T> mapWithUpdating(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        return new SinkImpl<>("mapWithUpdatingSink(" + mapName + ')',
                updateMapP(mapName, toKeyFn, updateFn), toKeyFn);
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
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
     * <p>
     * This sink supports exactly-once processing only if the supplied update
     * function performs <i>idempotent updates</i>, i.e., it satisfies the rule
     * {@code updateFn.apply(v, e).equals(v)} for any {@code e} that was
     * already observed.
     * <p>
     * <b>Note:</b> This operation is not lock-aware, it will process the entries
     * even if they are locked. Use {@link #mapWithEntryProcessor} if you need
     * locking.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param map      map to drain to
     * @param toKeyFn  function that extracts the key from the input item
     * @param updateFn function that receives the existing map value and the item
     *                 and returns the new map value
     * @param <T>      input item type
     * @param <K>      key type
     * @param <V>      value type
     */
    @Nonnull
    public static <T, K, V> Sink<T> mapWithUpdating(
            @Nonnull IMap<? super K, ? super V> map,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        return mapWithUpdating(map.getName(), toKeyFn, updateFn);
    }

    /**
     * Returns a sink equivalent to {@link #mapWithUpdating}, but for a map
     * in a remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     * <p>
     * Due to the used API, the remote cluster must be at least version 4.0.
     */
    @Nonnull
    public static <T, K, V> Sink<T> remoteMapWithUpdating(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        return fromProcessor("remoteMapWithUpdatingSink(" + mapName + ')',
                updateRemoteMapP(mapName, clientConfig, toKeyFn, updateFn),
                toKeyFn);
    }

    /**
     * Convenience for {@link #mapWithUpdating(String, FunctionEx,
     * BiFunctionEx)} with {@link Entry} as the input item.
     */
    @Nonnull
    public static <K, V, E extends Entry<K, V>> Sink<E> mapWithUpdating(
            @Nonnull String mapName,
            @Nonnull BiFunctionEx<? super V, ? super E, ? extends V> updateFn
    ) {
        return mapWithUpdating(mapName, entryKey(), updateFn);
    }

    /**
     * Convenience for {@link #mapWithUpdating(IMap, FunctionEx,
     * BiFunctionEx)} with {@link Entry} as the input item.
     */
    @Nonnull
    public static <K, V, E extends Entry<K, V>> Sink<E> mapWithUpdating(
            @Nonnull IMap<? super K, ? super V> map,
            @Nonnull BiFunctionEx<? super V, ? super E, ? extends V> updateFn
    ) {
        return mapWithUpdating(map.getName(), updateFn);
    }

    /**
     * Convenience for {@link #remoteMapWithUpdating} with {@link Entry} as
     * input item.
     */
    @Nonnull
    public static <K, V, E extends Entry<K, V>> Sink<E> remoteMapWithUpdating(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull BiFunctionEx<? super V, ? super E, ? extends V> updateFn
    ) {
        //noinspection Convert2MethodRef (provokes a javac 9 bug)
        return fromProcessor("remoteMapWithUpdatingSink(" + mapName + ')',
                updateRemoteMapP(mapName, clientConfig, (Entry<K, V> e) -> e.getKey(), updateFn));
    }

    /**
     * Convenience for {@link #mapWithEntryProcessor(int, String, FunctionEx, FunctionEx)}
     * when the maximum number of async operations is not specified.
     */
    @Nonnull
    public static <E, K, V, R> Sink<E> mapWithEntryProcessor(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super E, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        return fromProcessor("mapWithEntryProcessorSink(" + mapName + ')',
                updateMapP(mapName, toKeyFn, toEntryProcessorFn),
                toKeyFn);
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
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param maxParallelAsyncOps  maximum number of simultaneous entry
     *                             processors affecting the map
     * @param mapName  name of the map
     * @param toKeyFn  function that extracts the key from the input item
     * @param toEntryProcessorFn function that returns the {@code EntryProcessor}
     *                           to apply to the key
     * @param <E> input item type
     * @param <K> key type
     * @param <V> value type
     */
    @Nonnull
    public static <E, K, V, R> Sink<E> mapWithEntryProcessor(
            int maxParallelAsyncOps,
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super E, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        return fromProcessor("mapWithEntryProcessorSink(" + mapName + ')',
                updateMapP(maxParallelAsyncOps, mapName, toKeyFn, toEntryProcessorFn),
                toKeyFn);
    }

    /**
     * Returns a sink that uses the items it receives to create {@code
     * EntryProcessor}s it submits to a Hazelcast {@code IMap} with the
     * specified name. For each received item it applies {@code toKeyFn} to
     * get the key and {@code toEntryProcessorFn} to get the entry processor,
     * and then submits the key and the entry processor to the Hazelcast
     * cluster, which will internally apply the entry processor to the key.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the map you supply
     * and acquires a map with that name on the local cluster. If you supply a
     * map instance from another cluster, no error will be thrown to indicate
     * this.
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
     * This sink supports exactly-once processing only if the supplied entry
     * processor performs <i>idempotent updates</i>, i.e., the resulting value
     * would be the same if an entry processor was run on the same entry more
     * than once.
     * <p>
     * <b>Note:</b> Unlike {@link #mapWithUpdating} and {@link #mapWithMerging},
     * this operation <em>is</em> lock-aware. If the key is locked,
     * the EntryProcessor will wait until it acquires the lock.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param map                map to drain to
     * @param toKeyFn            function that extracts the key from the input item
     * @param toEntryProcessorFn function that returns the {@code EntryProcessor} to apply to the key
     * @param <T>                input item type
     * @param <K>                key type
     * @param <V>                value type
     */
    @Nonnull
    public static <T, K, V, R> Sink<T> mapWithEntryProcessor(
            @Nonnull IMap<? super K, ? super V> map,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        return mapWithEntryProcessor(map.getName(), toKeyFn, toEntryProcessorFn);
    }

    /**
     * Returns a sink equivalent to {@link #mapWithEntryProcessor}, but for a map
     * in a remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     */
    @Nonnull
    public static <E, K, V, R> Sink<E> remoteMapWithEntryProcessor(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super E, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        return fromProcessor("remoteMapWithEntryProcessorSink(" + mapName + ')',
                updateRemoteMapP(mapName, clientConfig, toKeyFn, toEntryProcessorFn),
                toKeyFn);
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
     * The default local parallelism for this sink is 2.
     */
    @Nonnull
    public static <T extends Entry> Sink<T> cache(@Nonnull String cacheName) {
        //noinspection Convert2MethodRef (provokes a javac 9 bug)
        return new SinkImpl<>("cacheSink(" + cacheName + ')', writeCacheP(cacheName), en -> en.getKey());
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
     * The default local parallelism for this sink is 2.
     */
    @Nonnull
    public static <T extends Entry> Sink<T> remoteCache(
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
     * Returns a sink that adds the items it receives to the specified
     * Hazelcast {@code IList}.
     * <p>
     * <strong>NOTE:</strong> Jet only remembers the name of the list you
     * supply and acquires a list with that name on the local cluster. If you
     * supply a list instance from another cluster, no error will be thrown to
     * indicate this.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     * <p>
     * The default local parallelism for this sink is 1.
     */
    @Nonnull
    public static <T> Sink<T> list(@Nonnull IList<? super T> list) {
        return list(list.getName());
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
     * Returns a sink which publishes the items it receives to a distributed
     * reliable topic with the specified name.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     * <p>
     * Local parallelism for this sink is 1.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <T> Sink<T> reliableTopic(@Nonnull String reliableTopicName) {
        return SinkBuilder.<ITopic<T>>sinkBuilder("reliableTopicSink(" + reliableTopicName + "))",
                SecuredFunctions.reliableTopicFn(reliableTopicName))
                .<T>receiveFn(ITopic::publish)
                .permission(new ReliableTopicPermission(reliableTopicName, ACTION_CREATE, ACTION_PUBLISH))
                .build();
    }

    /**
     * Returns a sink which publishes the items it receives to the provided
     * distributed reliable topic. More precisely, it takes the <em>name</em>
     * of the given {@code ITopic} and then independently retrieves the {@code
     * ITopic} with the same name from the cluster where the job is running. To
     * prevent surprising behavior, make sure you have obtained the {@code
     * ITopic} from the same cluster to which you will submit the pipeline.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     * <p>
     * Local parallelism for this sink is 1.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <T> Sink<T> reliableTopic(@Nonnull ITopic<Object> reliableTopic) {
        return reliableTopic(reliableTopic.getName());
    }

    /**
     * Returns a sink which publishes the items it receives to a distributed
     * reliable topic with the provided name in a remote cluster identified by
     * the supplied {@code ClientConfig}.
     * <p>
     * No state is saved to snapshot for this sink. After the job is restarted,
     * the items will likely be duplicated, providing an <i>at-least-once</i>
     * guarantee.
     * <p>
     * Local parallelism for this sink is 1.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <T> Sink<T> remoteReliableTopic(@Nonnull String reliableTopicName, @Nonnull ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig); //conversion needed for serializability
        return SinkBuilder
                .sinkBuilder("reliableTopicSink(" + reliableTopicName + "))",
                        ctx -> {
                            HazelcastInstance client = newHazelcastClient(asClientConfig(clientXml));
                            ITopic<T> topic = client.getReliableTopic(reliableTopicName);
                            return tuple2(client, topic);
                        })
                .<T>receiveFn((clientTopicTuple, message) -> clientTopicTuple.f1().publish(message))
                .destroyFn(clientTopicTuple -> clientTopicTuple.f0().shutdown())
                .build();
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
     *
     * @param host the host to connect to
     * @param port the target port
     * @param toStringFn a function to convert received items to string. It
     *     must be stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param charset charset used to convert the string to bytes
     */
    @Nonnull
    public static <T> Sink<T> socket(
            @Nonnull String host,
            int port,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull Charset charset
    ) {
        return fromProcessor("socketSink(" + host + ':' + port + ')', writeSocketP(host, port, toStringFn, charset));
    }

    /**
     * Convenience for {@link #socket(String, int, FunctionEx,
     * Charset)} with UTF-8 as the charset.
     */
    @Nonnull
    public static <T> Sink<T> socket(
            @Nonnull String host,
            int port,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn
    ) {
        return socket(host, port, toStringFn, UTF_8);
    }

    /**
     * Convenience for {@link #socket(String, int, FunctionEx,
     * Charset)} with {@code Object.toString} as the conversion function and
     * UTF-8 as the charset.
     */
    @Nonnull
    public static <T> Sink<T> socket(@Nonnull String host, int port) {
        return socket(host, port, Object::toString);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom file sink for the Pipeline API. See javadoc of methods in
     * {@link FileSinkBuilder} for more details.
     * <p>
     * The sink writes the items it receives to files. Each processor will
     * write to its own files whose names contain the processor's global index
     * (an integer unique to each processor of the vertex), but the same
     * directory is used for all files, on all cluster members. That directory
     * can be a shared in a network - each processor creates globally unique
     * file names.
     *
     * <h3>Fault tolerance</h3>
     *
     * If the job is running in <i>exactly-once</i> mode, Jet writes the items
     * to temporary files (ending with a {@value
     * FileSinkBuilder#TEMP_FILE_SUFFIX} suffix). When Jet commits a snapshot,
     * it atomically renames the file to remove this suffix. Thanks to the
     * two-phase commit of the snapshot the sink provides exactly-once
     * guarantee.
     * <p>
     * Because Jet starts a new file each time it snapshots the state, the sink
     * will produce many more small files, depending on the snapshot interval.
     * If you want to avoid the temporary files or the high number of files but
     * need to have exactly-once for other processors in the job, call {@link
     * FileSinkBuilder#exactlyOnce(boolean) exactlyOnce(false)} on the returned
     * builder. This will give you <i>at-least-once</i> guarantee for the
     * source and unchanged guarantee for other processors.
     * <p>
     * For the fault-tolerance to work, the target file system must be a
     * network file system. If you lose a member with its files, you'll
     * obviously lose data. Even if that member rejoins later with the lost
     * files, the job might have processed more transactions on the remaining
     * members and will not commit the temporary files on the resurrected
     * member.
     *
     * <h3>File name structure</h3>
     * <pre>{@code
     * [<date>-]<global processor index>[-<sequence>][".tmp"]
     * }</pre>
     *
     * Description (parts in {@code []} are optional):
     * <ul>
     *     <li>{@code <date>}: the current date and time, see {@link
     *          FileSinkBuilder#rollByDate(String)}. Not present if rolling by
     *          date is not used
     *
     *     <li>{@code <global processor index>}: a processor index ensuring
     *          that each parallel processor writes to its own file
     *
     *     <li>{@code <sequence>}: a sequence number starting from 0. Used if
     *          either:<ul>
     *              <li>running in <i>exactly-once</i> mode
     *              <li>{@linkplain FileSinkBuilder#rollByFileSize(long)
     *                    maximum file size} is set
     *          </ul>
     *          The sequence is reset to 0 when the {@code <date>} changes.
     *
     *     <li>{@code ".tmp"}: the {@link FileSinkBuilder#TEMP_FILE_SUFFIX},
     *          used if the file is not yet committed
     * </ul>
     *
     * <h3>Notes</h3>
     *
     * The target directory is not deleted before the job start. If file names
     * clash, they are appended to. This is needed to ensure at-least-once
     * behavior. In exactly-once mode the file names never clash thanks to the
     * sequence number in file name: a number higher than the highest sequence
     * number found in the directory is always chosen.
     * <p>
     * For performance, the processor doesn't delete old files from the
     * directory. If you have frequent snapshots, you should delete the old
     * files from time to time to avoid having huge number of files in the
     * directory. Jet lists the files in the directory after a restart to find
     * out the sequence number to use.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param <T> type of the items the sink accepts
     */
    @Nonnull
    public static <T> FileSinkBuilder<T> filesBuilder(@Nonnull String directoryName) {
        return new FileSinkBuilder<>(directoryName);
    }

    /**
     * Convenience for {@link #filesBuilder} with the UTF-8 charset and with
     * overwriting of existing files.
     */
    @Nonnull
    public static <T> Sink<T> files(@Nonnull String directoryName) {
        return Sinks.<T>filesBuilder(directoryName).build();
    }

    /**
     * Convenience for {@link #filesBuilder} with the UTF-8 charset and with
     * overwriting of existing files. The sink converts each item to a JSON
     * string adds it to the file as a line.
     */
    @Nonnull
    public static <T> Sink<T> json(@Nonnull String directoryName) {
        return Sinks.<T>filesBuilder(directoryName).toStringFn(JsonUtil::toJson).build();
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
     * @param toStringFn a function that returns a string representation of a
     *     stream item. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <T> stream item type
     */
    @Nonnull
    public static <T> Sink<T> logger(@Nonnull FunctionEx<? super T, String> toStringFn) {
        return fromProcessor("loggerSink", writeLoggerP(toStringFn));
    }

    /**
     * Convenience for {@link #logger(FunctionEx)} with {@code
     * Object.toString()} as the {@code toStringFn}.
     */
    @Nonnull
    public static <T> Sink<T> logger() {
        return logger(Object::toString);
    }

    /**
     * Returns a sink which discards all received items.
     */
    @Nonnull
    public static <T> Sink<T> noop() {
        return fromProcessor("noop", preferLocalParallelismOne(noopP()));
    }

    /**
     * Convenience for {@link #jmsQueueBuilder(SupplierEx)}. Creates a
     * connection without any authentication parameters. If a received item is
     * not an instance of {@code javax.jms.Message}, the sink wraps {@code
     * item.toString()} into a {@link javax.jms.TextMessage}.
     *
     * @param queueName the name of the queue
     * @param factorySupplier supplier to obtain JMS connection factory. It
     *     must be stateless.
     */
    @Nonnull
    public static <T> Sink<T> jmsQueue(
            @Nonnull String queueName,
            @Nonnull SupplierEx<ConnectionFactory> factorySupplier
    ) {
        return Sinks.<T>jmsQueueBuilder(factorySupplier)
                .destinationName(queueName)
                .build();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom JMS queue sink for the Pipeline API. See javadoc for {@link
     * JmsSinkBuilder} methods for more details.
     * <p>
     * In <b>exactly-once mode</b> the processor uses two-phase XA transactions
     * to guarantee exactly-once delivery. The supplier is expected to return
     * an {@link javax.jms.XAConnectionFactory}. The transaction is committed
     * after all processors finished processing the items and stored all data
     * to the snapshot. Processor is also able to finish the commit after a
     * restart, should the job fail mid-way of the commit process. This mode
     * significantly increases latency because produced messages are visible
     * only after they are committed; if you want to avoid it, you can reduce
     * the guarantee just for this sink. To do so call {@link
     * JmsSinkBuilder#exactlyOnce(boolean) exactlyOnce(false)} on the returned
     * builder. If your messages have a unique identifier, some JMS brokers
     * have deduplication functionality - you can use this to avoid the latency
     * penalty.
     * <p>
     * In <b>at-least-once mode</b> or when the <b>guarantee is off</b>, the
     * produced records are acknowledged immediately. We use transactions to
     * produce messages in batches, but those transactions have very short
     * duration.
     * <p>
     * IO failures should be handled by the JMS provider. If any JMS operation
     * throws an exception, the job will fail. Most of the providers offer a
     * configuration parameter to enable auto-reconnection, refer to provider
     * documentation for details.
     * <p>
     * <b>Test the XA support of your broker</b>
     * <p>
     * The JMS is an API, some brokers don't implement the XA transactions
     * correctly. We run our stress tests with RabbitMQ and ActiveMQ. The most
     * common flaw is that a prepared transaction is rolled back if the client
     * disconnects. To check your broker, you can run the code in <a
     * href="https://github.com/hazelcast/hazelcast-jet-contrib/xa-test">
     * https://github.com/hazelcast/hazelcast-jet-contrib/xa-test</a>
     * <p>
     * <b>Notes</b>
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * @param factorySupplier supplier to obtain JMS connection factory. For
     *      exactly-once the factory must implement {@link
     *      javax.jms.XAConnectionFactory}. It must be stateless.
     * @param <T> type of the items the sink accepts
     */
    @Nonnull
    public static <T> JmsSinkBuilder<T> jmsQueueBuilder(@Nonnull SupplierEx<ConnectionFactory> factorySupplier) {
        return new JmsSinkBuilder<>(factorySupplier, false);
    }

    /**
     * Shortcut for:
     * <pre>{@code
     *      jmsTopicBuilder(factorySupplier)
     *                 .destinationName(topicName)
     *                 .build();
     * }</pre>
     *
     * See {@link #jmsTopicBuilder(SupplierEx)} for more details.
     *
     * @param topicName the name of the queue
     * @param factorySupplier supplier to obtain JMS connection factory. For
     *      exactly-once the factory must implement {@link
     *      javax.jms.XAConnectionFactory}. It must be stateless.
     */
    @Nonnull
    public static <T> Sink<T> jmsTopic(
            @Nonnull String topicName,
            @Nonnull SupplierEx<ConnectionFactory> factorySupplier
    ) {
        return Sinks.<T>jmsTopicBuilder(factorySupplier)
                .destinationName(topicName)
                .build();
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom JMS topic sink for the Pipeline API. See javadoc on {@link
     * JmsSinkBuilder} methods for more details.
     * <p>
     * In <b>exactly-once mode</b> the processor uses two-phase XA transactions
     * to guarantee exactly-once delivery. The supplier is expected to return
     * an {@link javax.jms.XAConnectionFactory}. The transaction is committed
     * after all processors finished processing the items and stored all data
     * to the snapshot. Processor is also able to finish the commit after a
     * restart, should the job fail mid-way of the commit process. This mode
     * significantly increases latency because produced messages are visible
     * only after they are committed; if you want to avoid it, you can reduce
     * the guarantee just for this sink. To do so call {@link
     * JmsSinkBuilder#exactlyOnce(boolean) exactlyOnce(false)} on the returned
     * builder.
     * <p>
     * In at-least-once mode or when the guarantee is off, the produced records
     * are acknowledged immediately. We use transactions to produce messages in
     * batches, but those transactions have very short duration.
     * <p>
     * IO failures should be handled by the JMS provider. If any JMS operation
     * throws an exception, the job will fail. Most of the providers offer a
     * configuration parameter to enable auto-reconnection, refer to provider
     * documentation for details.
     * <p>
     * <b>Test the XA support of your broker</b>
     * <p>
     * The JMS is an API, some brokers don't implement the XA transactions
     * correctly. We run our stress tests with RabbitMQ and ActiveMQ. The most
     * common flaw is that a prepared transaction is rolled back if the client
     * disconnects. To check your broker, you can run the code in <a
     * href="https://github.com/hazelcast/hazelcast-jet-contrib/xa-test">
     * https://github.com/hazelcast/hazelcast-jet-contrib/xa-test</a>
     * <p>
     * <b>Notes</b>
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * @param factorySupplier supplier to obtain JMS connection factory. It
     *     must be stateless.
     * @param <T> type of the items the sink accepts
     */
    @Nonnull
    public static <T> JmsSinkBuilder<T> jmsTopicBuilder(@Nonnull SupplierEx<ConnectionFactory> factorySupplier) {
        return new JmsSinkBuilder<>(factorySupplier, true);
    }

    /**
     * A shortcut for:
     * <pre>{@code
     *     Sinks.<T>jdbcBuilder()
     *             .updateQuery(updateQuery)
     *             .dataSourceSupplier(dataSourceSupplier)
     *             .bindFn(bindFn)
     *             .build();
     * }</pre>
     *
     * See {@link #jdbcBuilder()} for more information.
     */
    @Nonnull
    public static <T> Sink<T> jdbc(
            @Nonnull String updateQuery,
            @Nonnull SupplierEx<? extends CommonDataSource> dataSourceSupplier,
            @Nonnull BiConsumerEx<PreparedStatement, T> bindFn
    ) {
        return Sinks.<T>jdbcBuilder()
                .updateQuery(updateQuery)
                .dataSourceSupplier(dataSourceSupplier)
                .bindFn(bindFn)
                .build();
    }

    /**
     * A shortcut for:
     * <pre>{@code
     *     jdbcBuilder(updateQuery, bindFn)
     *              .jdbcUrl(jdbcUrl)
     *              .build()
     * }</pre>
     *
     * See {@link #jdbcBuilder()} for more information.
     */
    @Nonnull
    public static <T> Sink<T> jdbc(
            @Nonnull String updateQuery,
            @Nonnull String jdbcUrl,
            @Nonnull BiConsumerEx<PreparedStatement, T> bindFn
    ) {
        return Sinks.<T>jdbcBuilder()
                .updateQuery(updateQuery)
                .jdbcUrl(jdbcUrl)
                .bindFn(bindFn)
                .build();
    }

    /**
     * Returns a builder to build a sink that connects to a JDBC database,
     * prepares an SQL statement and executes it for each item. On the returned
     * builder you must specify a connection (either using a {@linkplain
     * JdbcSinkBuilder#jdbcUrl(String) JDBC URL} or using a {@linkplain
     * JdbcSinkBuilder#dataSourceSupplier(SupplierEx) datasource}), the
     * {@linkplain JdbcSinkBuilder#updateQuery(String) SQL statement} and a
     * {@linkplain JdbcSinkBuilder#bindFn(BiConsumerEx) bind function}.
     * <p>
     * <b>Example</b>
     *
     * <pre>{@code
     * stage.writeTo(Sinks.<Entry<Integer, String>>jdbcBuilder()
     *     .updateQuery("INSERT INTO table (key, value) VALUES(?, ?)")
     *     .bindFn((stmt, item) -> {
     *         stmt.setInt(1, item.getKey());
     *         stmt.setString(2, item.getValue());
     *     })
     *     .jdbcUrl("jdbc:...")
     *     .build());
     * }</pre>
     * <p>
     * <b>Commit behavior</b>
     * <p>
     * The commit behavior depends on the job guarantee:<ul>
     *     <li><b>Exactly-once:</b> XA transactions will be used to commit the
     *     work in phase two of the snapshot, that is after all other vertices
     *     in the job have performed the snapshot. Very small state will be
     *     saved to snapshot.
     *
     *     <li><b>At-least-once or no guarantee:</b> Records will be committed
     *     in batches. A batch is created from records that are readily available
     *     at the sink.
     * </ul>
     * <p>
     * If the job is in exactly-once mode, the overhead in the database and the
     * output latency are higher. This is caused by the fact that Jet will not
     * commit the transaction until the next snapshot occurs and the number of
     * uncommitted records in the transactions can be very high. Latency is
     * high because the changes are visible only after the transactions are
     * committed. Configure the snapshot interval accordingly.
     * <p>
     * If your driver doesn't support XA transactions or if you want to avoid
     * the performance or latency penalty, you can decrease the guarantee just
     * for this sink by calling {@link JdbcSinkBuilder#exactlyOnce(boolean)
     * exactlyOnce(false)} on the returned builder.
     * <p>
     * <b>Notes</b>
     * <p>
     * In non-XA mode, in case of an {@link SQLException} the processor will
     * transparently reconnect and the job won't fail, except for an {@link
     * SQLNonTransientException} subclass. In XA mode the job will fail
     * immediately.
     * <p>
     * <b>Test the XA support of your database</b>
     * <p>
     * The JDBC is an API, some brokers don't implement the XA transactions
     * correctly. We run our stress tests with PostgreSQL. The most common flaw
     * is that a prepared transaction is rolled back if the client disconnects.
     * To check your database, you can run the code in <a
     * href="https://github.com/hazelcast/hazelcast-jet-contrib/xa-test">
     * https://github.com/hazelcast/hazelcast-jet-contrib/xa-test</a>
     * <p>
     * <b>Notes</b>
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @param <T> type of the items the sink accepts
     * @since Jet 4.1
     */
    @Nonnull
    public static <T> JdbcSinkBuilder<T> jdbcBuilder() {
        return new JdbcSinkBuilder<>();
    }

    /**
     * Returns a sink that publishes to the {@link Observable} with the
     * provided name. The records that are sent to the observable can be
     * read through first getting a handle to it through
     * {@link JetService#getObservable(String)} and then subscribing to
     * the events using the methods on {@link Observable}.
     * <p>
     * The {@code Observable} should be destroyed after using it. For the full
     * description see {@link Observable the javadoc for Observable}.
     * Example:<pre>{@code
     *   Observable<Integer> observable = jet.newObservable();
     *   CompletableFuture<List<Integer>> list = observable.toFuture(o -> o.collect(toList()));
     *
     *   pipeline.readFrom(TestSources.items(1, 2, 3, 4))
     *           .writeTo(Sinks.observable(observable));
     *
     *   Job job = jet.newJob(pipeline);
     *
     *   System.out.println(list.get());
     *   observable.destroy();
     * }</pre>
     * This sink is cooperative and uses a local parallelism of 1.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <T> Sink<T> observable(String name) {
        return Sinks.fromProcessor(String.format("observableSink(%s)", name),
                SinkProcessors.writeObservableP(name));
    }

    /**
     * Returns a sink that publishes to the provided {@link Observable}. More
     * precisely, it takes the <em>name</em> of the given {@code Observable}
     * and then independently retrieves an {@code Observable} with the same
     * name from the cluster where the job is running. To prevent surprising
     * behavior, make sure you have obtained the {@code Observable} from the
     * same cluster to which you will submit the pipeline.
     * <p>
     * For more details refer to {@link #observable(String) observable(name)}.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public static <T> Sink<T> observable(Observable<? super T> observable) {
        return observable(observable.name());
    }

}
