/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.MapSinkConfiguration;
import com.hazelcast.jet.impl.util.ImdgUtil;

import javax.annotation.Nonnull;
import java.util.Locale;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static java.util.Objects.requireNonNull;

/**
 * Builder for a map that is used as sink.
 * <p>
 * Builds a local Map sink unless one of {@link #dataConnectionRef} or
 * {@link #clientConfig} is provided, in which case a remote sink is built.
 * The required parameters are: <ul>
 * <li>mapName</li>
 * <li>toKeyFn - key-extracting function</li>
 * <li>one of toValue/updateFn - key ex</li>
 * </ul>
 * <p>
 * <p>
 * This sink provides the exactly-once guarantee thanks to <i>idempotent
 * updates</i>. It means that the value with the same key is not appended,
 * but overwritten. After the job is restarted from snapshot, duplicate
 * items will not change the state in the target map.
 *
 * @param <T> specifies type of input that will be written to sink
 * @param <K> specifies key type of map sink
 * @param <V> specifies value type of map sink
 * @since 5.4
 */
public class MapSinkBuilder<T, K, V> {

    private final String mapName;
    private DataConnectionRef dataConnectionRef;
    private ClientConfig clientConfig;

    private FunctionEx<? super T, ? extends K> toKeyFn;

    private FunctionEx<? super T, ? extends V> toValueFn;
    private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
    private BinaryOperatorEx<V> mergeFn;

    /**
     * Creates {@link MapSinkBuilder} to build a local or remote Map sink
     *
     * @param mapName name of the map to sink into
     */
    public MapSinkBuilder(@Nonnull String mapName) {
        this.mapName = requireNonNull(mapName, "mapName must not be null");
    }

    /**
     * Sets the {@link DataConnectionRef} reference to a HazelcastDataConnection
     * to use for remote Map sink.
     * <p>
     * Only one of {@link #dataConnectionRef} and {@link #clientConfig} can be set.
     *
     * @param dataConnectionRef reference to a {@link com.hazelcast.dataconnection.HazelcastDataConnection}
     */
    public MapSinkBuilder<T, K, V> dataConnectionRef(DataConnectionRef dataConnectionRef) {
        if (clientConfig != null) {
            throw new IllegalStateException("You cannot set dataConnectionRef, clientConfig is already set");
        }
        this.dataConnectionRef = requireNonNull(dataConnectionRef, "dataConnectionRef can not be null");
        return this;
    }

    /**
     * Sets the {@link ClientConfig} with configuration for a Hazelcast client
     * to use for remote Map sink.
     * <p>
     * Only one of {@link #dataConnectionRef} and {@link #clientConfig} can be set.
     *
     * @param clientConfig remote Hazelcast client configuration
     */
    public MapSinkBuilder<T, K, V> clientConfig(ClientConfig clientConfig) {
        if (dataConnectionRef != null) {
            throw new IllegalStateException("You cannot set clientConfig, dataConnectionRef is already set");
        }
        this.clientConfig = requireNonNull(clientConfig, "clientConfig can not be null");
        return this;
    }

    /**
     * Set the key-extracting function.
     * The resulting value will be used as the key in the sink IMap.
     * <p>
     * The function must be {@link java.io.Serializable}.
     *
     * @param toKeyFn function to extract key from incoming items
     */
    public MapSinkBuilder<T, K, V> toKeyFn(FunctionEx<? super T, ? extends K> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        this.toKeyFn = toKeyFn;
        return this;
    }

    /**
     * Set the function to extract a value from the incoming items.
     * The value will be put into the IMap under key extracted using
     * {@link #toKeyFn}.
     * <p>
     * Only one of {@link #toValueFn} or {@link #updateFn} can be set.
     * Optionally {@link #mergeFn} can be set together with `toValueFn`.
     * <p>
     * The function must be {@link java.io.Serializable}.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param toValueFn function to extract value from incoming items
     */
    public MapSinkBuilder<T, K, V> toValueFn(FunctionEx<? super T, ? extends V> toValueFn) {
        checkSerializable(toValueFn, "toValueFn");
        this.toValueFn = toValueFn;
        return this;
    }

    /**
     * Set the function to update the value in Hazelcast IMap.
     * <p>
     * For each item it receives, it
     * applies {@link #toKeyFn} to get the key and then applies {@link #updateFn} to
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
     * <p>
     * <b>Note:</b> This operation is <em>NOT</em> lock-aware, it will process the entries
     * no matter if they are locked or not.
     * Use {@link MapSinkEntryProcessorBuilder} if you need locking.
     * <p>
     * The function must be {@link java.io.Serializable}.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param updateFn function that receives the existing map value and the item
     *                 and returns the new map value
     */
    public MapSinkBuilder<T, K, V> updateFn(BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        checkSerializable(updateFn, "updateFn");
        this.updateFn = updateFn;
        return this;
    }

    /**
     * Set the function to merge the existing value with new value.
     * <p>
     * If the map
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
     *            : mergeFn.apply(oldValue, newValue);
     * if (value == null)
     *     map.remove(key);
     * else
     *     map.put(key, value);
     * </pre>
     * <b>Note:</b> This operation is <em>NOT</em> lock-aware, it will process the entries
     * no matter if they are locked or not.
     * Use {@link MapSinkEntryProcessorBuilder} if you need locking.
     * <p>
     * The function must be {@link java.io.Serializable}.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param mergeFn function that merges the existing value with the value acquired from the
     *                received item
     */
    public MapSinkBuilder<T, K, V> mergeFn(BinaryOperatorEx<V> mergeFn) {
        checkSerializable(mergeFn, "mergeFn");
        this.mergeFn = mergeFn;
        return this;
    }

    /**
     * Build the sink.
     * <p>
     * The default local parallelism for this sink is 1.
     *
     * @return the sink
     */
    public Sink<T> build() {
        validateOperation();

        MapSinkConfiguration<T, K, V> configuration = new MapSinkConfiguration<>(mapName);
        configuration.setDataConnectionRef(dataConnectionRef);
        configuration.setClientXml(ImdgUtil.asXmlString(clientConfig));
        configuration.setToKeyFn(toKeyFn);
        configuration.setToValueFn(toValueFn);
        configuration.setUpdateFn(updateFn);
        configuration.setMergeFn(mergeFn);

        ProcessorMetaSupplier processorMetaSupplier = buildProcessorMetaSupplier(configuration);

        return fromProcessor(
                getSinkName(),
                processorMetaSupplier,
                toKeyFn
        );
    }

    private void validateOperation() {
        boolean hasToValueFn = toValueFn != null;
        boolean hasUpdateFn = updateFn != null;
        boolean hasMergeFn = mergeFn != null;

        if ((hasToValueFn && hasUpdateFn) ||
                (hasUpdateFn && hasMergeFn)) {
            throw new IllegalArgumentException("You must set exactly one combination of " +
                    "toValueFn, updateFn or updateFn and mergeFn parameters");
        }
    }

    private ProcessorMetaSupplier buildProcessorMetaSupplier(MapSinkConfiguration<T, K, V> configuration) {
        if (updateFn != null) {
            return HazelcastWriters.updateMapSupplier(configuration);
        } else if (mergeFn != null) {
            return HazelcastWriters.mergeMapSupplier(configuration);
        } else { // toValueFn != null
            return HazelcastWriters.writeMapSupplier(configuration);
        }
    }

    private String getSinkName() {
        StringBuilder sb = new StringBuilder();
        if (isRemote()) {
            sb.append("remote");
        }
        if (updateFn != null) {
            sb.append("MapWithUpdatingSink");
        } else if (mergeFn != null) {
            sb.append("MapWithMergingSink");
        } else { // toValueFn != null
            sb.append("MapSink");
        }
        sb.append('(')
                .append(mapName)
                .append(')');
        sb.replace(0, 1, sb.substring(0, 1).toLowerCase(Locale.ROOT));
        return sb.toString();
    }


    private boolean isRemote() {
        return dataConnectionRef != null || clientConfig != null;
    }
}

