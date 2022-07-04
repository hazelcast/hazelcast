/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.Stage;

import javax.annotation.Nonnull;

public abstract class AbstractStage implements Stage {

    final PipelineImpl pipelineImpl;
    final Transform transform;

    AbstractStage(Transform transform, PipelineImpl pipelineImpl) {
        this.transform = transform;
        this.pipelineImpl = pipelineImpl;
    }

    public static Transform transformOf(GeneralStage<?> stage) {
        return ((AbstractStage) stage).transform;
    }

    public static Transform transformOf(SinkStage stage) {
        return ((AbstractStage) stage).transform;
    }

    @Override
    public Pipeline getPipeline() {
        return pipelineImpl;
    }

    @Nonnull @Override
    public Stage setLocalParallelism(int localParallelism) {
        transform.localParallelism(localParallelism);
        return this;
    }

    @Nonnull @Override
    public Stage setName(@Nonnull String name) {
        transform.setName(name);
        return this;
    }

    @Nonnull @Override
    public String name() {
        return transform.name();
    }

    @Override
    public String toString() {
        return String.valueOf(transform);
    }
}
