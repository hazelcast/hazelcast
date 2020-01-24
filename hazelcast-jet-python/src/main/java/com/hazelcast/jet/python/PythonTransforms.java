/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.python;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;


/**
 * Transforms which allow the user to call Python user-defined functions
 * from inside a Jet pipeline.
 *
 * @since 4.0
 */
@Beta
public final class PythonTransforms {

    private PythonTransforms() {
    }

    /**
     * A stage-transforming method that adds a "map using Python" pipeline stage.
     * Use it with {@code stage.apply(PythonService.mapUsingPython(pyConfig))}.
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
     * A stage-transforming method that adds a "map using Python" pipeline stage.
     * Use it with {@code stage.apply(PythonService.mapUsingPythonBatch(pyConfig))}.
     */
    @Nonnull
    public static FunctionEx<BatchStage<String>, BatchStage<String>> mapUsingPythonBatch(
            @Nonnull PythonServiceConfig cfg
    ) {
        return s -> s
                .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
                .setName("mapUsingPython");
    }
}
