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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.map.EntryProcessor;

import static com.hazelcast.jet.impl.connector.AsyncHazelcastWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;

/**
 * Parameters for using a map as a sink with an EntryProcessor:
 */
public class MapSinkEntryProcessorConfiguration<E, K, V, R> {

    private final String mapName;
    private DataConnectionRef dataConnectionRef;
    private String clientXml;

    private FunctionEx<? super E, ? extends K> toKeyFn;

    private FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn;

    private int maxParallelAsyncOps = MAX_PARALLEL_ASYNC_OPS_DEFAULT;

    public MapSinkEntryProcessorConfiguration(String mapName) {
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

    public FunctionEx<? super E, ? extends K> getToKeyFn() {
        return toKeyFn;
    }

    public void setToKeyFn(FunctionEx<? super E, ? extends K> toKeyFn) {
        this.toKeyFn = toKeyFn;
    }

    public FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> getToEntryProcessorFn() {
        return toEntryProcessorFn;
    }

    public void setToEntryProcessorFn(FunctionEx<? super E, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn) {
        this.toEntryProcessorFn = toEntryProcessorFn;
    }

    public int getMaxParallelAsyncOps() {
        return maxParallelAsyncOps;
    }

    public void setMaxParallelAsyncOps(int maxParallelAsyncOps) {
        this.maxParallelAsyncOps = maxParallelAsyncOps;
    }

    public boolean isRemote() {
        return dataConnectionRef != null || clientXml != null;
    }
}
