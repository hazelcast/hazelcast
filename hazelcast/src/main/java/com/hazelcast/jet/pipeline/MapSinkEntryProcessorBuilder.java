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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.MapSinkEntryProcessorConfiguration;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.EntryProcessor;

import static com.hazelcast.jet.impl.connector.AsyncHazelcastWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;

/**
 * Parameters for using a map as a sink with an EntryProcessor:
 *
 * TODO review if this can be merged with MapSinkEntryProcessorBuilder, add full javadoc if not
 */
public class MapSinkEntryProcessorBuilder<E, K, V, R> {

    private final String mapName;
    private DataConnectionRef dataConnectionRef;
    private ClientConfig clientConfig;

    private FunctionEx<? super E, ? extends K> toKeyFn;
    private FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn;
    private int maxParallelAsyncOps = MAX_PARALLEL_ASYNC_OPS_DEFAULT;

    public MapSinkEntryProcessorBuilder(String mapName) {
        this.mapName = mapName;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> dataConnectionName(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> clientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        return this;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> toKeyFn(FunctionEx<? super E, ? extends K> toKeyFn) {
        this.toKeyFn = toKeyFn;
        return this;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> toEntryProcessorFn(
            FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn) {
        this.toEntryProcessorFn = toEntryProcessorFn;
        return this;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> maxParallelAsyncOps(int maxParallelAsyncOps) {
        this.maxParallelAsyncOps = maxParallelAsyncOps;
        return this;
    }

    public Sink<E> build() {
        MapSinkEntryProcessorConfiguration<E, K, V, R> configuration =
                new MapSinkEntryProcessorConfiguration<>(mapName);
        configuration.setDataConnectionRef(dataConnectionRef);
        configuration.setClientXml(ImdgUtil.asXmlString(clientConfig));
        configuration.setToKeyFn(toKeyFn);
        configuration.setToEntryProcessorFn(toEntryProcessorFn);
        configuration.setMaxParallelAsyncOps(maxParallelAsyncOps);

        ProcessorMetaSupplier processorMetaSupplier = HazelcastWriters.updateMapSupplier(configuration);
        return fromProcessor(
                geSinkName(),
                processorMetaSupplier,
                toKeyFn
        );
    }

    private String geSinkName() {
        if (isRemote()) {
            return "remoteMapWithEntryProcessorSink(" + mapName + ')';
        } else {
            return "mapWithEntryProcessorSink(" + mapName + ')';
        }
    }

    private boolean isRemote() {
        return dataConnectionRef != null || clientConfig != null;
    }
}
