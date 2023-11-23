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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.connector.HazelcastReaders;
import com.hazelcast.jet.impl.connector.RemoteMapSourceConfiguration;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.of;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static java.util.Objects.requireNonNull;

/**
 * Builder providing a fluent API to build a remote map source.
 * Use {@link Sources#remoteMapBuilder(String)} to obtain the builder instance.
 * <p>
 * By supplying a {@code predicate} and
 * {@code projection} here instead of in separate {@code map/filter}
 * transforms you allow the source to apply these functions early, before
 * generating any output, with the potential of significantly reducing
 * data traffic. If your data is stored in the IMDG using the <a href=
 *     "http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#implementing-portable-serialization">
 * portable serialization format</a>, there are additional optimizations
 * available when using {@link Projections#singleAttribute} and {@link
 * Projections#multiAttribute}) to create your projection instance and
 * using the {@link Predicates} factory or
 * {@link PredicateBuilder PredicateBuilder} to create
 * the predicate. In this case Jet can test the predicate and apply the
 * projection without deserializing the whole object.
 * <p>
 * Due to the current limitations in the way Jet reads the map it can't use
 * any indexes on the map. It will always scan the map in full.
 * <p>
 * The source does not save any state to snapshot. If the job is restarted,
 * it will re-emit all entries.
 * <p>
 * If the {@code IMap} is modified while being read, or if there is a
 * cluster topology change (triggering data migration), the source may miss
 * and/or duplicate some entries. If we detect a topology change, the job
 * will fail, but the detection is only on a best-effort basis - we might
 * still give incorrect results without reporting a failure. Concurrent
 * mutation is not detected at all.
 * <p>
 * The default local parallelism for this processor is 1.
 * <p>
 * <h4>Predicate/projection class requirements</h4>
 *
 * The classes implementing {@code predicate} and {@code projection} need
 * to be available on the remote cluster's classpath or loaded using
 * <em>Hazelcast User Code Deployment</em>. It's not enough to add them to
 * the job classpath in {@link JobConfig}. The same is true for the class
 * of the objects stored in the map itself. If you cannot meet these
 * conditions, do not add predicate and projection and add a
 * subsequent {@link GeneralStage#map map} or {@link GeneralStage#filter
 * filter} stage.
 *<p>
 * @param <K> the type of the key in the map
 * @param <V> the type of the value in the map
 * @param <T> the type of the emitted items
 * @since 5.4
 */
public class RemoteMapSourceBuilder<K, V, T> {

    private final String mapName;
    private String dataConnectionName;
    private ClientConfig clientConfig;
    private Predicate<K, V> predicate;
    private Projection<? super Entry<K, V>, ? extends T> projection;

    RemoteMapSourceBuilder(String mapName) {
        requireNonNull(mapName, "mapName can not be null");
        this.mapName = mapName;
    }

    /**
     * Set the data connection name to use to connect to the remote cluster.
     * The data connection must be of
     * {@link com.hazelcast.dataconnection.HazelcastDataConnection} type.
     * <p>
     * One of {@link #dataConnectionName} or {@link #clientConfig} is required
     * to build the source. If both are provided the data connection takes
     * precedence.
     *
     * @param dataConnectionName name of the data connection
     * @return this builder
     */
    public RemoteMapSourceBuilder<K, V, T> dataConnectionName(@Nonnull String dataConnectionName) {
        this.dataConnectionName = requireNonNull(dataConnectionName, "dataConnectionName can not be null");
        return this;
    }

    /**
     * Set the client configuration to use to connect to the remote cluster.
     * <p>
     * One of {@link #dataConnectionName} or {@link #clientConfig} is required
     * to build the source. If both are provided the data connection takes
     * precedence.
     *
     * @param clientConfig client configuration
     * @return this builder
     */
    public RemoteMapSourceBuilder<K, V, T> clientConfig(@Nonnull ClientConfig clientConfig) {
        this.clientConfig = requireNonNull(clientConfig);
        return this;
    }

    /**
     * Set the predicate to apply at the source.
     * See the {@link RemoteMapSourceBuilder} for more details.
     * @param predicate the predicate
     * @return this builder
     */
    public RemoteMapSourceBuilder<K, V, T> predicate(@Nonnull Predicate<K, V> predicate) {
        requireNonNull(predicate, "predicate can not be null");
        checkSerializable(predicate, "predicate");
        this.predicate = predicate;
        return this;
    }

    /**
     * Set the projection to apply at the source.
     * See the {@link RemoteMapSourceBuilder} for more details.
     * @param projection the projection
     * @return this builder
     * @param <T_NEW> type of the emitted items
     */
    public <T_NEW> RemoteMapSourceBuilder<K, V, T_NEW> projection(
            @Nonnull Projection<? super Entry<K, V>, ? extends T_NEW> projection
    ) {
        requireNonNull(projection, "projection can not be null");
        checkSerializable(requireNonNull(projection), "projection");
        @SuppressWarnings("unchecked")
        RemoteMapSourceBuilder<K, V, T_NEW> newThis = (RemoteMapSourceBuilder<K, V, T_NEW>) this;
        newThis.projection = projection;
        return newThis;
    }

    /**
     * Build the source using the parameters set in this builder.
     *
     * @return a batch source emitting items from the remote IMap
     */
    public <N> BatchSource<N> build() {
        if ((dataConnectionName == null) && (clientConfig == null)) {
            throw new HazelcastException("Either dataConnectionName or clientConfig must be non-null");
        }
        RemoteMapSourceConfiguration<K, V, T> configuration = new RemoteMapSourceConfiguration<>(
                mapName, dataConnectionName, clientConfig, predicate, projection
        );

        return batchFromProcessor("remoteMapSource(" + mapName + ')', of(HazelcastReaders.readRemoteMapSupplier(configuration)));
    }
}
