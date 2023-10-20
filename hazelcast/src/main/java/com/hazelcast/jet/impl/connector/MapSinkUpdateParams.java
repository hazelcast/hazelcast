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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

/**
 * Parameters for using a map as a sink with a update function
 */
public class MapSinkUpdateParams<T, K, V> {

    private final String mapName;

    private String dataConnectionName;

    private ClientConfig clientConfig;

    private FunctionEx<? super T, ? extends K> toKeyFn;

    private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;

    public MapSinkUpdateParams(String mapName) {
        this.mapName = mapName;
    }

    public boolean hasDataSourceConnection() {
        return dataConnectionName != null;
    }

    public boolean hasClientConfig() {
        return clientConfig != null;
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

    public FunctionEx<? super T, ? extends K> getToKeyFn() {
        return toKeyFn;
    }

    public void setToKeyFn(FunctionEx<? super T, ? extends K> toKeyFn) {
        this.toKeyFn = toKeyFn;
    }

    public BiFunctionEx<? super V, ? super T, ? extends V> getUpdateFn() {
        return updateFn;
    }

    public void setUpdateFn(BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        this.updateFn = updateFn;
    }
}
