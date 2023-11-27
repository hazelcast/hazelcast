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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.HazelcastWriters;
import com.hazelcast.jet.impl.connector.MapSinkConfiguration;
import com.hazelcast.jet.impl.util.ImdgUtil;

import java.util.Locale;

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

    private final String mapName;
    private DataConnectionRef dataConnectionRef;
    private ClientConfig clientConfig;

    private FunctionEx<? super T, ? extends K> toKeyFn;

    private FunctionEx<? super T, ? extends V> toValueFn;
    private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
    private BinaryOperatorEx<V> mergeFn;

    public MapSinkBuilder(String mapName) {
        this.mapName = mapName;
    }

    public MapSinkBuilder<T, K, V> dataConnectionRef(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = requireNonNull(dataConnectionRef, "dataConnectionRef can not be null");
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

