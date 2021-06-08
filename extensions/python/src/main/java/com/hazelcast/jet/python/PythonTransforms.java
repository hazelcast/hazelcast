/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.python;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;


/**
 * Transforms which allow the user to call Python user-defined functions
 * from inside a Jet pipeline.
 * <p>
 * Support for Python is currently in the beta phase. The API might change
 * in future versions.
 *
 * @since Jet 4.0
 */
@EvolvingApi
public final class PythonTransforms {

    private PythonTransforms() {
    }

    /**
     * A stage-transforming method that adds a "map using Python" pipeline stage.
     * Use it with {@code stage.apply(PythonService.mapUsingPython(pyConfig))}.
     * See {@link com.hazelcast.jet.python.PythonServiceConfig} for more details.
     */
    @Nonnull
    public static FunctionEx<StreamStage<String>, StreamStage<String>> mapUsingPython(
            @Nonnull PythonServiceConfig cfg
    ) {
        return s -> s
                .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
                .setName("mapUsingPython");
    }

    /**
     * A stage-transforming method that adds a partitioned "map using Python"
     * pipeline stage. It applies partitioning using the supplied {@code keyFn}.
     * You need partitioning if your input stream comes from a non-distributed
     * data source (all data coming in on a single cluster member), in order to
     * distribute the Python work across the whole cluster.
     * <p>
     * Use it like this: {@code stage.apply(PythonService.mapUsingPython(keyFn,
     * pyConfig))}. See {@link com.hazelcast.jet.python.PythonServiceConfig}
     * for more details.
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Deprecated
    @Nonnull
    public static <K> FunctionEx<StreamStage<String>, StreamStage<String>> mapUsingPython(
            @Nonnull FunctionEx<? super String, ? extends K> keyFn,
            @Nonnull PythonServiceConfig cfg
    ) {
        return s -> s
                .groupingKey(keyFn)
                .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
                .setName("mapUsingPython");
    }

    /**
     * A stage-transforming method that adds a "map using Python" pipeline stage.
     * Use it with {@code stage.apply(PythonService.mapUsingPythonBatch(pyConfig))}.
     * See {@link com.hazelcast.jet.python.PythonServiceConfig} for more details.
     */
    @Nonnull
    public static FunctionEx<BatchStage<String>, BatchStage<String>> mapUsingPythonBatch(
            @Nonnull PythonServiceConfig cfg
    ) {
        return s -> s
                .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
                .setName("mapUsingPythonBatch");
    }

    /**
     * A stage-transforming method that adds a partitioned "map using Python"
     * pipeline stage. It applies partitioning using the supplied {@code keyFn}.
     * You need partitioning if your input stream comes from a non-distributed
     * data source (all data coming in on a single cluster member), in order to
     * distribute the Python work across the whole cluster.
     * <p>
     * Use it like this: {@code stage.apply(PythonService.mapUsingPythonBatch(keyFn,
     * pyConfig))}. See {@link com.hazelcast.jet.python.PythonServiceConfig}
     * for more details.
     */
    @Nonnull
    public static <K> FunctionEx<BatchStage<String>, BatchStage<String>> mapUsingPythonBatch(
            @Nonnull FunctionEx<? super String, ? extends K> keyFn,
            @Nonnull PythonServiceConfig cfg
    ) {
        return s -> s
                .groupingKey(keyFn)
                .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
                .setName("mapUsingPythonBatch");
    }
}
