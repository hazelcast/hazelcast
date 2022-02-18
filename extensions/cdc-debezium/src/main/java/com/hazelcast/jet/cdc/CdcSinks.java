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

package com.hazelcast.jet.cdc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.cdc.impl.WriteCdcP;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.AbstractHazelcastConnectorSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.security.Permission;
import java.util.List;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.DISTRIBUTED_PARTITIONED;
import static com.hazelcast.jet.impl.util.ImdgUtil.asXmlString;
import static com.hazelcast.security.PermissionsUtil.mapUpdatePermission;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains factory methods for change data capture specific pipeline
 * sinks. As a consequence these sinks take {@link ChangeRecord} items
 * as their input.
 * <p>
 * These sinks can detect any <em>reordering</em> that might happen in
 * the {@code ChangeRecord} stream (Jet pipelines use parallel
 * execution, so item reordering can and does happen). Reordering
 * detection is based on implementation-specific sequence numbers
 * provided by CDC event sources. The sink reacts to reordering by
 * dropping obsolete input items. The exact behavior is as follows. For
 * each input item, the sink:
 * <ol><li>
 *     applies the {@code keyFn} to the input item to extract its key
 * </li><li>
 *     extracts the item's sequence number
 * </li><li>
 *     compares the sequence number with the previously seen sequence number
 *     for the same key, if any
 * </li><li>
 *     if the previous sequence number is more recent than the one observed in
 *     the input item, it drops (ignores) the input item
 * </li></ol>
 * <p>
 * About the implementation-specific sequence numbers provided by the
 * CDC sources. They consist of two parts:
 * <ul><li>
 *     <em>numeric sequence</em> for which a monotonically increasing
 *     value is emitted by the source and which allows ordering of the
 *     event
 * </li><li>
 *     <em>source descriptor</em> which allows us to identify situations
 *     when the numeric sequence gets reset or any other events when
 *     comparing new numeric values with previous ones no longer makes
 *     sense
 * </li></ul>
 * <p>
 * The sequence source is made up of information like ID of the database
 * instance the connector is connected to, name of the binlog file being
 * monitored and so on. So whenever the source reconnects to a new server
 * or switches to a new binlog file or other such event, the source field
 * of sequence numbers will change.
 * <p>
 * The logic of determining which event are more recent takes the sequence
 * source into consideration. Whenever the source field changes, the event
 * carrying it will be considered more recent than ones with the old
 * source value. Numeric sequence numbers are compared to establish order
 * only when their sources match.
 * <p>
 * Restarting the CDC Jet source will not change sequence number sources,
 * only significant changes on the database side will.
 *
 * @since Jet 4.2
 */
public final class CdcSinks {

    /**
     * Number of seconds for which the sink will remember the last seen
     * sequence number for an input key (used to detect reordering). After
     * this time the last-seen sequence number values will eventually be
     * evicted, in order to save space.
     * <p>
     * The default value is 10 seconds.
     *
     * @since Jet 4.2
     */
    public static final HazelcastProperty SEQUENCE_CACHE_EXPIRATION_SECONDS
            = new HazelcastProperty("jet.cdc.sink.sequence.cache.expiration.seconds", 10, SECONDS);

    private CdcSinks() {
    }

    /**
     * Returns a sink that applies the changes described by a Change Data
     * Capture (CDC) stream to an {@code IMap}. The main usage is to have
     * the {@code IMap} mirror the contents of the data table that is the
     * source of the CDC stream, but since it accepts arbitrary key and
     * value functions, other behaviors are possible as well.
     * <p>
     * <strong>NOTE</strong>: in order for the sink behavior to be predictable,
     * the map should be non-existent or empty at the time the sink starts
     * using it.
     * <p>
     * For each item the sink receives, it uses the {@code keyFn} to
     * determine which map key the change event applies to. Then, based
     * on the {@code ChangeRecord}'s {@code Operation} it decides to
     * either:
     * <ul><li>
     *     delete the key from the map ({@link Operation#DELETE})
     * </li><li>
     *     insert a new value for the key
     *          ({@link Operation#SYNC} & {@link Operation#INSERT})
     * </li><li>
     *     update the current value for the key ({@link Operation#UPDATE})
     * </li></ul>
     * For insert and update operations, the sink determines the new value
     * by applying the provided {@code valueFn} to the change record.
     * <p>
     * <strong>NOTE:</strong> if {@code valueFn} returns {@code null},
     * then the key will be deleted no matter the operation (ie. even for
     * update and insert records).
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        String name = "mapCdcSink(" + mapName + ')';
        return sink(name, mapName, null, keyFn, valueFn);
    }

    /**
     * Returns a sink that applies the changes described by a Change Data
     * Capture (CDC) stream to an {@code IMap}. The main usage is to have
     * the {@code IMap} mirror the contents of the data table that is the
     * source of the CDC stream, but since it accepts arbitrary key and
     * value functions, other behaviors are possible as well.
     * <p>
     * <strong>NOTE</strong>: in order for the sink behavior to be predictable,
     * the map should be non-existent or empty at the time the sink starts
     * using it.
     * <p>
     * For each item the sink receives it uses the {@code keyFn} to
     * determine which map key the change event applies to. Then, based
     * on the {@code ChangeRecord}'s {@code Operation} it decides to
     * either:
     * <ul><li>
     *     delete the key from the map ({@link Operation#DELETE})
     * </li><li>
     *     insert a new value for the key
     *          ({@link Operation#SYNC} & {@link Operation#INSERT})
     * </li><li>
     *     update the current value for the key ({@link Operation#UPDATE})
     * </li></ul>
     * For insert and update operations, the sink determines the new value
     * by applying the provided {@code valueFn} to the change record.
     * <p>
     * <strong>NOTE:</strong> if {@code valueFn} returns {@code null},
     * then the key will be deleted no matter the operation (ie. even for
     * update and insert records).
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> map(
            @Nonnull IMap<? super K, ? super V> map,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        return map(map.getName(), keyFn, valueFn);
    }

    /**
     * Returns a sink equivalent to {@link #map}, but for a map in a remote
     * Hazelcast cluster identified by the supplied {@code ClientConfig}.
     * <p>
     * <strong>NOTE 1</strong>: in order for the sink behavior to be
     * predictable, the map should be non-existent or empty at the time the
     * sink starts
     * using it.
     * <p>
     * <strong>NOTE 2:</strong> if {@code valueFn} returns {@code null},
     * then the key will be deleted no matter the operation (ie. even for
     * update and insert records).
     * <p>
     * Due to the used API, the remote cluster must be at least version 4.0.
     *
     * @since Jet 4.2
     */
    @Nonnull
    public static <K, V> Sink<ChangeRecord> remoteMap(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        String name = "remoteMapCdcSink(" + mapName + ')';
        return sink(name, mapName, clientConfig, keyFn, valueFn);
    }

    @Nonnull
    private static <K, V> Sink<ChangeRecord> sink(
            @Nonnull String name,
            @Nonnull String map,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        FunctionEx<? super ChangeRecord, ? extends V> toValueFn =
                record -> DELETE.equals(record.operation()) ? null : valueFn.apply(record);
        String clientXml = asXmlString(clientConfig);
        ProcessorSupplier supplier = AbstractHazelcastConnectorSupplier.ofMap(clientXml,
                procFn(name, map, clientXml, keyFn, toValueFn));
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.of(mapUpdatePermission(clientXml, name), supplier);
        return new SinkImpl<>(name, metaSupplier, DISTRIBUTED_PARTITIONED, keyFn);
    }

    private static <K, V> FunctionEx<HazelcastInstance, Processor> procFn(
            String name, String map, String clientXml,
            FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        return new FunctionEx<HazelcastInstance, Processor>() {
            @Override
            public Processor applyEx(HazelcastInstance instance) {
                return new WriteCdcP<>(instance, map, keyFn, valueFn);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(mapUpdatePermission(clientXml, name));
            }
        };
    }
}
