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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.pipeline.ComputeStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.Source;
import com.hazelcast.jet.pipeline.Stage;
import com.hazelcast.jet.pipeline.impl.transform.MultiTransform;
import com.hazelcast.jet.pipeline.impl.transform.UnaryTransform;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineImpl implements Pipeline {

    final Map<Stage, List<Stage>> adjacencyMap = new HashMap<>();

    @Override
    public <E> ComputeStage<E> drawFrom(Source<E> source) {
        return new ComputeStageImpl<>(source, this);
    }

    @Nonnull @Override
    public DAG toDag() {
        return new Planner(this).createDag();
    }

    public ComputeStage attach(List<ComputeStage> upstream, MultiTransform transform) {
        ComputeStageImpl attached = new ComputeStageImpl(upstream, transform, this);
        upstream.forEach(u -> connect(u, attached));
        return attached;
    }

    <IN, OUT> ComputeStage<OUT> attach(
            ComputeStage<IN> upstream, UnaryTransform<? super IN, OUT> unaryTransform
    ) {
        ComputeStageImpl<OUT> output = new ComputeStageImpl<>(upstream, unaryTransform, this);
        connect(upstream, output);
        return output;
    }

    <E> SinkStage drainTo(ComputeStage<E> upstream, Sink sink) {
        SinkStageImpl output = new SinkStageImpl(upstream, sink, this);
        connect(upstream, output);
        return output;
    }

    private void connect(ComputeStage upstream, Stage downstream) {
        adjacencyMap.get(upstream).add(downstream);
    }
}
