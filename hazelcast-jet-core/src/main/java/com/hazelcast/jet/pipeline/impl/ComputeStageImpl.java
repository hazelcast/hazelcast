/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.impl;

import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.EndStage;
import com.hazelcast.jet.pipeline.MultiTransform;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.Stage;
import com.hazelcast.jet.pipeline.Transform;
import com.hazelcast.jet.pipeline.UnaryTransform;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
public class ComputeStageImpl<E> extends AbstractStage implements ComputeStage<E> {

    ComputeStageImpl(Source<E> source, PipelineImpl pipeline) {
        super(emptyList(), source, pipeline);
    }

    ComputeStageImpl(List<Stage> upstream, Transform transform, PipelineImpl pipeline) {
        super(upstream, transform, pipeline);
    }

    ComputeStageImpl(Stage upstream, Transform transform, PipelineImpl pipeline) {
        super(singletonList(upstream), transform, pipeline);
    }

    @Override
    public <R> ComputeStage<R> attach(UnaryTransform<? super E, R> unaryTransform) {
        return pipelineImpl.attach(this, unaryTransform);
    }

    @Override
    public <R> ComputeStage<R> attach(MultiTransform<R> multiTransform, List<ComputeStage> otherInputs) {
        return pipelineImpl.attach(
                Stream.concat(Stream.of(this), otherInputs.stream()).collect(toList()),
                multiTransform);
    }

    @Override
    public EndStage drainTo(Sink sink) {
        return pipelineImpl.drainTo(this, sink);
    }
}
