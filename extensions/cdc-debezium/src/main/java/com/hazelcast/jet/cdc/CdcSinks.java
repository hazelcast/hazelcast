/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.cdc.Operation.DELETE;

/**
 * Contains factory methods for change data capture specific pipeline
 * sinks. As a consequence these sinks take {@link ChangeRecord} items
 * as their input.
 * <p>
 * The local parallelism for these sinks in this class is typically 1,
 * check the documentation of individual methods.
 *
 * @since 4.2
 */
public final class CdcSinks {

    private static final int PREFERRED_LOCAL_PARALLELISM = 1;

    private CdcSinks() {
    }

    /**
     * Returns a sink which maintains an up-to-date image of a change
     * data capture stream in the form of an {@code IMap}. By image we
     * mean that the map should always escribe the end result of merging
     * all the change events seen so far.
     * <p>
     * For each item the sink receives it uses the {@code keyFn} to
     * determine which map key the change event applies to. Then, based
     * on the {@code ChangeRecord}'s {@code Operation} it determines
     * eiher to:
     * <ul>
     *   <li>delete the key from the map
     *          ({@link Operation#DELETE})</li>
     *   <li>insert a new value for the key
     *          ({@link Operation#SYNC} & {@link Operation#INSERT})</li>
     *   <li>update the current value for the key
     *          ({@link Operation#UPDATE})</li>
     * </ul>
     * For insert and update operations the new value to use is
     * determined from the input record by using the provided
     * {@code valueFn}. <strong>IMPORTANT</strong> to note that if the
     * {@code valueFn} returns {@code null}, then the key will be
     * deleted from the map no matter the operation (ie. even for update
     * and insert records).
     * <p>
     * For the functionality of this sink it is vital that the order of
     * the input items is preserved so the sink is non-distributed and
     * its local parallelism is forced to 1. This way only a single
     * instance will be created for each pipeline.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull String map,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        return Sinks.fromProcessor("localMapCdcSink(" + map + ')',
                metaSupplier(map, null, keyFn, valueFn));
    }

    /**
     * Convenience for {@link #map(String, FunctionEx, FunctionEx)} with
     * actual {@code IMap} instance being passed in, instead of just
     * name.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull IMap<? super K, V> map,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        return map(map.getName(), keyFn, valueFn);
    }

    /**
     * Returns a sink equivalent to {@link #map}, but for a map in a
     * remote Hazelcast cluster identified by the supplied {@code
     * ClientConfig}.
     * <p>
     * Due to the used API, the remote cluster must be at least 3.11.
     *
     * @since 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> remoteMap(
            @Nonnull String map,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn
    ) {
        return Sinks.fromProcessor("remoteMapCdcSink(" + map + ')',
                metaSupplier(map, clientConfig, keyFn, valueFn));
    }

    @Nonnull
    private static <V> BiFunctionEx<V, ChangeRecord, V> updateFn(@Nonnull FunctionEx<ChangeRecord, V> valueFn) {
        return (BiFunctionEx<V, ChangeRecord, V>) (oldValue, record) -> {
            if (DELETE.equals(record.operation())) {
                return null;
            }
            return valueFn.apply(record);
        };
    }

    @Nonnull
    private static <K, V> ProcessorMetaSupplier metaSupplier(
            @Nonnull String map,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<ChangeRecord, K> keyFn,
            @Nonnull FunctionEx<ChangeRecord, V> valueFn) {
        return HazelcastWriters.updateMapSupplier(
                map, clientConfig, keyFn, updateFn(valueFn), PREFERRED_LOCAL_PARALLELISM);
    }

}
