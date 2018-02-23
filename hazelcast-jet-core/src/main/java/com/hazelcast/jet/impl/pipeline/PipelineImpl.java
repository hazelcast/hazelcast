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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DONT_ADAPT;
import static java.util.stream.Collectors.toList;

public class PipelineImpl implements Pipeline {

    private final Map<Transform, List<Transform>> adjacencyMap = new HashMap<>();

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BatchStage<T> drawFrom(@Nonnull BatchSource<? extends T> batchSource) {
        return new BatchStageImpl<>((BatchSourceTransform<? extends T>) batchSource, this);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> StreamStage<T> drawFrom(@Nonnull StreamSource<? extends T> source) {
        StreamSourceTransform<? extends T> xform = (StreamSourceTransform<? extends T>) source;
        return new StreamStageImpl<>(xform, xform.emitsJetEvents() ? ADAPT_TO_JET_EVENT : DONT_ADAPT, this);
    }

    @Override
    public <T> SinkStage drainTo(@Nonnull Sink<T> sink, GeneralStage<?>... stages) {
        List<Transform> upstream = Arrays.stream(stages)
                                        .map(s -> (AbstractStage) s)
                                        .map(s -> s.transform)
                                        .collect(toList());
        int[] ordinalsToAdapt = IntStream
                .range(0, stages.length)
                .filter(i -> ((ComputeStageImplBase) stages[i]).fnAdapter == ADAPT_TO_JET_EVENT)
                .toArray();
        SinkImpl sinkImpl = (SinkImpl) sink;
        SinkTransform sinkTransform = new SinkTransform(sinkImpl, upstream, ordinalsToAdapt);
        SinkStageImpl sinkStage = new SinkStageImpl(sinkTransform, this);
        sinkImpl.onAssignToStage();
        connect(upstream, sinkTransform);
        return sinkStage;
    }

    @Nonnull @Override
    public DAG toDag() {
        return new Planner(this).createDag();
    }

    public void connect(Transform upstream, Transform downstream) {
        adjacencyMap.get(upstream).add(downstream);
    }

    public void connect(List<Transform> upstream, Transform downstream) {
        upstream.forEach(u -> connect(u, downstream));
    }

    @Override
    public String toString() {
        return "Pipeline " + adjacencyMap;
    }

    Map<Transform, List<Transform>> adjacencyMap() {
        Map<Transform, List<Transform>> safeCopy = new HashMap<>();
        adjacencyMap.forEach((k, v) -> safeCopy.put(k, new ArrayList<>(v)));
        return safeCopy;
    }

    void register(Transform stage, List<Transform> downstream) {
        List<Transform> prev = adjacencyMap.put(stage, downstream);
        assert prev == null : "Double registering of a Stage with this Pipeline: " + stage;
    }
}
