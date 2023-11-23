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
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.MapSinkConfiguration;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static java.util.Objects.requireNonNull;

/**
 * Builder for a map that is used as sink.
 *
 * @param <T> specifies type of input that will be written to sink
 * @param <K> specifies key type of map sink
 * @param <V> specifies value type of map sink
 */
public class MapSinkBuilder<T, K, V> {

    private enum EnumMapOperation {
        MAP_WRITE("mapSink", false),
        MAP_UPDATE("mapWithUpdatingSink", false),
        MAP_MERGE("mapWithMergingSink", false),
        REMOTE_MAP_WRITE("remoteMapSink", true),
        REMOTE_MAP_UPDATE("remoteMapWithUpdatingSink", true),
        REMOTE_MAP_MERGE("remoteMapWithMergingSink", true);

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

    private FunctionEx<? super T, ? extends K> toKeyFn;

    private FunctionEx<? super T, ? extends V> toValueFn;

    private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;

    private BinaryOperatorEx<V> mergeFn;

    public MapSinkBuilder(String mapName) {
        this.mapName = mapName;
    }

    public MapSinkBuilder<T, K, V> mapWriteOperation() {
        this.enumMapOperation = EnumMapOperation.MAP_WRITE;
        return this;
    }

    public MapSinkBuilder<T, K, V> remoteMapWriteOperation() {
        this.enumMapOperation = EnumMapOperation.REMOTE_MAP_WRITE;
        return this;
    }

    public MapSinkBuilder<T, K, V> mapUpdateOperation() {
        this.enumMapOperation = EnumMapOperation.MAP_UPDATE;
        return this;
    }

    public MapSinkBuilder<T, K, V> remoteMapUpdateOperation() {
        this.enumMapOperation = EnumMapOperation.REMOTE_MAP_UPDATE;
        return this;
    }

    public MapSinkBuilder<T, K, V> mapMergeOperation() {
        this.enumMapOperation = EnumMapOperation.MAP_MERGE;
        return this;
    }

    public MapSinkBuilder<T, K, V> remoteMapMergeOperation() {
        this.enumMapOperation = EnumMapOperation.REMOTE_MAP_MERGE;
        return this;
    }

    public MapSinkBuilder<T, K, V> dataConnectionName(String dataConnectionName) {
        this.dataConnectionName = requireNonNull(dataConnectionName, "dataConnectionName can not be null");
        return this;
    }

    public MapSinkBuilder<T, K, V> clientConfig(ClientConfig clientConfig) {
        this.clientConfig = requireNonNull(clientConfig, "clientConfig can not be null");
        return this;
    }

    public MapSinkBuilder<T, K, V> toKeyFn(FunctionEx<? super T, ? extends K> toKeyFn) {
        checkSerializable(toKeyFn, "toKeyFn");
        this.toKeyFn = toKeyFn;
        return this;
    }


    public MapSinkBuilder<T, K, V> toValueFn(FunctionEx<? super T, ? extends V> toValueFn) {
        checkSerializable(toValueFn, "toValueFn");
        this.toValueFn = toValueFn;
        return this;
    }

    public MapSinkBuilder<T, K, V> updateFn(BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        checkSerializable(updateFn, "updateFn");
        this.updateFn = updateFn;
        return this;
    }

    public MapSinkBuilder<T, K, V> mergeFn(BinaryOperatorEx<V> mergeFn) {
        checkSerializable(mergeFn, "mergeFn");
        this.mergeFn = mergeFn;
        return this;
    }

    public Sink<T> build() {
        requireNonNull(enumMapOperation, "MapOperation enumeration can not be null");

        if (enumMapOperation == EnumMapOperation.REMOTE_MAP_WRITE || enumMapOperation == EnumMapOperation.MAP_WRITE) {
            return buildWrite();
        } else if (enumMapOperation == EnumMapOperation.REMOTE_MAP_UPDATE || enumMapOperation == EnumMapOperation.MAP_UPDATE) {
            return buildUpdate();
        } else if (enumMapOperation == EnumMapOperation.REMOTE_MAP_MERGE || enumMapOperation == EnumMapOperation.MAP_MERGE) {
            return buildMerge();
        }
        throw new IllegalStateException("Unknown MapOperation enumeration : " + enumMapOperation);
    }

    private Sink<T> buildWrite() {
        validateIfRemoteOperation();

        MapSinkConfiguration<T, K, V> configuration = new MapSinkConfiguration<>(mapName);
        configuration.setDataConnectionName(dataConnectionName);
        configuration.setClientConfig(clientConfig);
        configuration.setToKeyFn(toKeyFn);
        configuration.setToValueFn(toValueFn);

        ProcessorMetaSupplier processorMetaSupplier = HazelcastWriters.writeMapSupplier(configuration);
        return fromProcessor(geSinkName(),
                processorMetaSupplier,
                toKeyFn);
    }

    private Sink<T> buildUpdate() {
        validateIfRemoteOperation();

        MapSinkConfiguration<T, K, V> configuration = new MapSinkConfiguration<>(mapName);
        configuration.setDataConnectionName(dataConnectionName);
        configuration.setClientConfig(clientConfig);
        configuration.setToKeyFn(toKeyFn);
        configuration.setUpdateFn(updateFn);

        ProcessorMetaSupplier processorMetaSupplier = HazelcastWriters.updateMapSupplier(configuration);
        return fromProcessor(geSinkName(),
                processorMetaSupplier,
                toKeyFn);
    }

    private Sink<T> buildMerge() {
        validateIfRemoteOperation();

        MapSinkConfiguration<T, K, V> configuration = new MapSinkConfiguration<>(mapName);
        configuration.setDataConnectionName(dataConnectionName);
        configuration.setClientConfig(clientConfig);
        configuration.setToKeyFn(toKeyFn);
        configuration.setToValueFn(toValueFn);
        configuration.setMergeFn(mergeFn);

        ProcessorMetaSupplier processorMetaSupplier = HazelcastWriters.mergeMapSupplier(configuration);
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

