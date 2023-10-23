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

package com.hazelcast.jet.core.processor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.util.Map;
import java.util.Objects;

/**
 * Parameters to use a remote map as source
 *
 * @param <T> specifies emitted type
 * @param <K> specifies key type
 * @param <V> specifies value type
 */
public class RemoteMapSourceParams<T, K, V> {

    private final String mapName;

    private final String dataConnectionName;

    private final ClientConfig clientConfig;

    private final Predicate<K, V> predicate;

    private final Projection<? super Map.Entry<K, V>, ? extends T> projection;

    private RemoteMapSourceParams(Builder<T, K, V> builder) {
        this.mapName = builder.mapName;
        this.dataConnectionName = builder.dataConnectionName;
        this.clientConfig = builder.clientConfig;
        this.predicate = builder.predicate;
        this.projection = builder.projection;
    }

    public boolean hasDataSourceConnection() {
        return dataConnectionName != null;
    }

    public boolean hasPredicate() {
        return predicate != null;
    }

    public String getMapName() {
        return mapName;
    }

    public String getDataConnectionName() {
        return dataConnectionName;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public Predicate<K, V> getPredicate() {
        return predicate;
    }

    public Projection<? super Map.Entry<K, V>, ? extends T> getProjection() {
        return projection;
    }

    public static <T, K, V> Builder<T, K, V> builder(String mapName) {
        return new Builder<>(mapName);
    }

    public static class Builder<T, K, V> {

        private final String mapName;

        private String dataConnectionName;

        private ClientConfig clientConfig;

        private Predicate<K, V> predicate;

        private Projection<? super Map.Entry<K, V>, ? extends T> projection;

        public Builder(String mapName) {
            Objects.requireNonNull(mapName, "mapName can not be null");
            this.mapName = mapName;
        }

        public Builder<T, K, V> withDataConnectionName(String dataConnectionName) {
            this.dataConnectionName = dataConnectionName;
            return this;
        }

        public Builder<T, K, V> withClientConfig(ClientConfig clientConfig) {
            this.clientConfig = clientConfig;
            return this;
        }

        public Builder<T, K, V> withPredicate(Predicate<K, V> predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder<T, K, V> withProjection(Projection<? super Map.Entry<K, V>, ? extends T> projection) {
            this.projection = projection;
            return this;
        }

        public RemoteMapSourceParams<T, K, V> build() {
            return new RemoteMapSourceParams<>(this);
        }
    }
}
