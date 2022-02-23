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

import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.pipeline.SinkStage;

import javax.annotation.Nonnull;

class SinkStageImpl extends AbstractStage implements SinkStage {

    SinkStageImpl(SinkTransform transform, PipelineImpl pipeline) {
        super(transform, pipeline);
    }

    @Nonnull @Override
    public SinkStage setLocalParallelism(int localParallelism) {
        super.setLocalParallelism(localParallelism);
        return this;
    }

    @Nonnull @Override
    public SinkStage setName(@Nonnull String name) {
        super.setName(name);
        return this;
    }
}
