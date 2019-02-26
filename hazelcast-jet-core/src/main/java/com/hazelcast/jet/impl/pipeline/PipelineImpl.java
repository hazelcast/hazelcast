/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.pipeline.StreamSourceStage;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.util.Util.addOrIncrementIndexInName;
import static com.hazelcast.jet.impl.util.Util.escapeGraphviz;
import static java.util.stream.Collectors.toList;

public class PipelineImpl implements Pipeline {

    private static final GeneralStage[] NO_STAGES = {};
    private final Map<Transform, List<Transform>> adjacencyMap = new LinkedHashMap<>();

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BatchStage<T> drawFrom(@Nonnull BatchSource<? extends T> source) {
        BatchSourceTransform<? extends T> xform = (BatchSourceTransform<? extends T>) source;
        xform.onAssignToStage();
        return new BatchStageImpl<>(xform, this);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> StreamSourceStage<T> drawFrom(@Nonnull StreamSource<? extends T> source) {
        StreamSourceTransform<T> xform = (StreamSourceTransform<T>) source;
        xform.onAssignToStage();
        return new StreamSourceStageImpl<>(xform, this);
    }

    @Nonnull
    @Override
    public <T> SinkStage drainTo(
            @Nonnull Sink<? super T> sink,
            @Nonnull GeneralStage<? extends T> stage0,
            @Nonnull GeneralStage<? extends T> stage1,
            @Nonnull GeneralStage<? extends T>... moreStages
    ) {
        GeneralStage[] stages = new GeneralStage[2 + moreStages.length];
        stages[0] = stage0;
        stages[1] = stage1;
        System.arraycopy(moreStages, 0, stages, 2, moreStages.length);
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

    @Nonnull @Override
    public String toDotString() {
        makeNamesUnique();
        Map<Transform, List<Transform>> adjMap = this.adjacencyMap();
        Map<Transform, String> transformNames = new HashMap<>();
        final StringBuilder builder = new StringBuilder(256);
        builder.append("digraph Pipeline {\n");
        for (Entry<Transform, List<Transform>> entry : adjMap.entrySet()) {
            Transform src = entry.getKey();
            String srcName = transformNames.computeIfAbsent(src, Transform::name);
            for (Transform dest : entry.getValue()) {
                String destName = transformNames.computeIfAbsent(dest, Transform::name);
                builder.append("\t")
                       .append("\"").append(escapeGraphviz(srcName)).append("\"")
                       .append(" -> ")
                       .append("\"").append(escapeGraphviz(destName)).append("\"")
                       .append(";\n");
            }
        }
        builder.append("}");
        return builder.toString();
    }

    Map<Transform, List<Transform>> adjacencyMap() {
        Map<Transform, List<Transform>> safeCopy = new LinkedHashMap<>();
        adjacencyMap.forEach((k, v) -> safeCopy.put(k, new ArrayList<>(v)));
        return safeCopy;
    }

    void register(Transform stage, List<Transform> downstream) {
        List<Transform> prev = adjacencyMap.put(stage, downstream);
        assert prev == null : "Double registration of a Stage with this Pipeline: " + stage;
    }

    void makeNamesUnique() {
        Set<String> usedNames = new HashSet<>();
        for (Transform transform : adjacencyMap.keySet()) {
            // replace the name with a unique one
            while (!usedNames.add(transform.name())) {
                transform.setName(addOrIncrementIndexInName(transform.name()));
            }
        }
    }
}
