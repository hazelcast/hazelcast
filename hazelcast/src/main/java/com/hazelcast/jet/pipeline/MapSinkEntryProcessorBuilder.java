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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.MapSinkEntryProcessorConfiguration;
import com.hazelcast.map.EntryProcessor;

import static com.hazelcast.jet.impl.connector.AsyncHazelcastWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static java.util.Objects.requireNonNull;

/**
 * Parameters for using a map as a sink with an EntryProcessor:
 */
public class MapSinkEntryProcessorBuilder<E, K, V, R> {

    private enum EnumMapOperation {
        MAP_ENTRY_PROCESSOR("mapWithEntryProcessorSink", false),
        REMOTE_MAP_ENTRY_PROCESSOR("remoteMapWithEntryProcessorSink", true);

        private final String mapSinkPrefix;

        private final boolean isRemoteOperation;

        EnumMapOperation(String mapSinkPrefix, boolean isRemoteOperation) {
            this.mapSinkPrefix = mapSinkPrefix;
            this.isRemoteOperation = isRemoteOperation;
        }

        public String getMapSinkPrefix() {
            return mapSinkPrefix;
        }

        public boolean isRemoteOperation() {
            return isRemoteOperation;
        }
    }

    private final String mapName;

    private EnumMapOperation enumMapOperation;

    private String dataConnectionName;

    private ClientConfig clientConfig;

    private FunctionEx<? super E, ? extends K> toKeyFn;

    private FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn;

    private int maxParallelAsyncOps = MAX_PARALLEL_ASYNC_OPS_DEFAULT;

    public MapSinkEntryProcessorBuilder(String mapName) {
        this.mapName = mapName;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> mapOperation() {
        this.enumMapOperation = EnumMapOperation.MAP_ENTRY_PROCESSOR;
        return this;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> remoteMapOperation() {
        this.enumMapOperation = EnumMapOperation.REMOTE_MAP_ENTRY_PROCESSOR;
        return this;
    }

    public MapSinkEntryProcessorBuilder<E, K, V, R> dataConnectionName(String dataConnectionName) {
        this.dataConnectionName = dataConnectionName;
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
        requireNonNull(enumMapOperation, "MapOperation enumeration can not be null");

        validateIfRemoteOperation();

        MapSinkEntryProcessorConfiguration<E, K, V, R> configuration = new MapSinkEntryProcessorConfiguration<>(mapName);
        configuration.setDataConnectionName(dataConnectionName);
        configuration.setClientConfig(clientConfig);
        configuration.setToKeyFn(toKeyFn);
        configuration.setToEntryProcessorFn(toEntryProcessorFn);
        configuration.setMaxParallelAsyncOps(maxParallelAsyncOps);

        ProcessorMetaSupplier processorMetaSupplier = HazelcastWriters.updateMapSupplier(configuration);
        return fromProcessor(geSinkName(),
                processorMetaSupplier,
                toKeyFn);

    }

    private void validateIfRemoteOperation() {
        if (enumMapOperation.isRemoteOperation() && ((dataConnectionName == null) && (clientConfig == null))) {
            throw new HazelcastException("Either dataConnectionName or clientConfig must be non-null");
        }
    }

    private String geSinkName() {
        return enumMapOperation.getMapSinkPrefix() + "(" + mapName + ')';
    }
}
