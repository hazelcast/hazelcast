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

/**
 * Parameters to use a remote map as source
 *
 * @param <T> specifies emitted type
 * @param <K> specifies key type
 * @param <V> specifies value type
 */
public class RemoteMapSourceParams<T, K, V> {

    private final String mapName;

    private String dataConnectionName;

    private ClientConfig clientConfig;

    private Predicate<K, V> predicate;

    private Projection<? super Map.Entry<K, V>, ? extends T> projection;

    public RemoteMapSourceParams(String mapName) {
        this.mapName = mapName;
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

    public void setDataConnectionName(String dataConnectionName) {
        this.dataConnectionName = dataConnectionName;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public Predicate<K, V> getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate<K, V> predicate) {
        this.predicate = predicate;
    }

    public Projection<? super Map.Entry<K, V>, ? extends T> getProjection() {
        return projection;
    }

    public void setProjection(Projection<? super Map.Entry<K, V>, ? extends T> projection) {
        this.projection = projection;
    }
}
