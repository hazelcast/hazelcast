/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.DataConnectionRef;

/**
 * Parameters for using a map as a sink
 */
public class MapSinkConfiguration<T, K, V> {

    private final String mapName;

    private DataConnectionRef dataConnectionRef;
    private String clientXml;

    private FunctionEx<? super T, ? extends K> toKeyFn;
    private FunctionEx<? super T, ? extends V> toValueFn;
    private BiFunctionEx<? super V, ? super T, ? extends V> updateFn;
    private BinaryOperatorEx<V> mergeFn;

    public MapSinkConfiguration(String mapName) {
        this.mapName = mapName;
    }

    public String getMapName() {
        return mapName;
    }

    public DataConnectionRef getDataConnectionRef() {
        return dataConnectionRef;
    }

    public String getDataConnectionName() {
        return dataConnectionRef == null ? null : dataConnectionRef.getName();
    }

    public void setDataConnectionRef(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
    }

    public String getClientXml() {
        return clientXml;
    }

    public void setClientXml(String clientXml) {
        this.clientXml = clientXml;
    }

    public boolean isRemote() {
        return dataConnectionRef != null || clientXml != null;
    }

    public FunctionEx<? super T, ? extends K> getToKeyFn() {
        return toKeyFn;
    }

    public void setToKeyFn(FunctionEx<? super T, ? extends K> toKeyFn) {
        this.toKeyFn = toKeyFn;
    }

    public FunctionEx<? super T, ? extends V> getToValueFn() {
        return toValueFn;
    }

    public void setToValueFn(FunctionEx<? super T, ? extends V> toValueFn) {
        this.toValueFn = toValueFn;
    }

    public BiFunctionEx<? super V, ? super T, ? extends V> getUpdateFn() {
        return updateFn;
    }

    public void setUpdateFn(BiFunctionEx<? super V, ? super T, ? extends V> updateFn) {
        this.updateFn = updateFn;
    }

    public BinaryOperatorEx<V> getMergeFn() {
        return mergeFn;
    }

    public void setMergeFn(BinaryOperatorEx<V> mergeFn) {
        this.mergeFn = mergeFn;
    }

}
