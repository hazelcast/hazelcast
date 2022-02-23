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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
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
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class PipelineImpl implements Pipeline {

    private static final long serialVersionUID = 1L;

    private final Map<Transform, List<Transform>> adjacencyMap = new LinkedHashMap<>();
    private final Map<String, File> attachedFiles = new HashMap<>();
    private boolean preserveOrder;

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <T> BatchStage<T> readFrom(@Nonnull BatchSource<? extends T> source) {
        BatchSourceTransform<? extends T> xform = (BatchSourceTransform<? extends T>) source;
        xform.onAssignToStage();
        register(xform);
        return new BatchStageImpl<>(xform, this);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> StreamSourceStage<T> readFrom(@Nonnull StreamSource<? extends T> source) {
        StreamSourceTransform<T> xform = (StreamSourceTransform<T>) source;
        xform.onAssignToStage();
        register(xform);
        return new StreamSourceStageImpl<>(xform, this);
    }

    @Override
    public boolean isPreserveOrder() {
        return preserveOrder;
    }

    @Nonnull @Override
    public PipelineImpl setPreserveOrder(boolean value) {
        preserveOrder = value;
        return this;
    }

    @Nonnull @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> SinkStage writeTo(
            @Nonnull Sink<? super T> sink,
            @Nonnull GeneralStage<? extends T> stage0,
            @Nonnull GeneralStage<? extends T> stage1,
            @Nonnull GeneralStage<? extends T>... moreStages
    ) {
        List<GeneralStage> stages = new ArrayList<>(asList(moreStages));
        stages.add(0, stage0);
        stages.add(1, stage1);
        List<Transform> upstream = stages
                .stream()
                .map(s -> (AbstractStage) s)
                .map(s -> s.transform)
                .collect(toList());
        int[] ordinalsToAdapt = IntStream
                .range(0, stages.size())
                .filter(i -> ((ComputeStageImplBase) stages.get(i)).fnAdapter == ADAPT_TO_JET_EVENT)
                .toArray();
        SinkImpl sinkImpl = (SinkImpl) sink;
        SinkTransform sinkTransform = new SinkTransform(sinkImpl, upstream, ordinalsToAdapt);
        SinkStageImpl sinkStage = new SinkStageImpl(sinkTransform, this);
        sinkImpl.onAssignToStage();
        connectGeneralStages(stages, sinkTransform);
        return sinkStage;
    }

    @Nonnull
    public DAG toDag(Context context) {
        return new Planner(this).createDag(context);
    }

    @Nonnull @Override
    public DAG toDag() {
        final int localParallelismUseDefault = -1;
        return toDag(new Context() {
            @Override public int defaultLocalParallelism() {
                return localParallelismUseDefault;
            }
        });
    }



    @SuppressWarnings("rawtypes")
    public void connect(
            @Nonnull List<ComputeStageImplBase> stages,
            @Nonnull AbstractTransform transform
    ) {
        @SuppressWarnings("unchecked")
        List<AbstractTransform> upstreamTransforms = (List<AbstractTransform>) (List) transform.upstream();
        for (int i = 0; i < upstreamTransforms.size(); i++) {
            ComputeStageImplBase us = stages.get(i);
            transform.setRebalanceInput(i, us.isRebalanceOutput);
            transform.setPartitionKeyFnForInput(i, us.rebalanceKeyFn);
        }
        upstreamTransforms.forEach(u -> adjacencyMap.get(u).add(transform));
        register(transform);
    }

    @SuppressWarnings("rawtypes")
    public void connect(
            @Nonnull ComputeStageImplBase stage,
            @Nonnull AbstractTransform toTransform
    ) {
        connect(singletonList(stage), toTransform);
    }

    @SuppressWarnings("rawtypes")
    public void connect(
            @Nonnull ComputeStageImplBase stage0,
            @Nonnull List<? extends GeneralStage> moreStages,
            @Nonnull AbstractTransform toTransform
    ) {
        List<ComputeStageImplBase> allStages =
                moreStages.stream().map(ComputeStageImplBase.class::cast).collect(toList());
        allStages.add(0, stage0);
        connect(allStages, toTransform);
    }

    @SuppressWarnings("rawtypes")
    public void connectGeneralStages(
            @Nonnull List<? extends GeneralStage> stages,
            @Nonnull AbstractTransform toTransform
    ) {
        List<ComputeStageImplBase> implStages = stages.stream().map(ComputeStageImplBase.class::cast).collect(toList());
        connect(implStages, toTransform);
    }

    public void attachFiles(@Nonnull Map<String, File> filesToAttach) {
        this.attachedFiles.putAll(filesToAttach);
    }

    @Nonnull
    public Map<String, File> attachedFiles() {
        return Collections.unmodifiableMap(attachedFiles);
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

    @Override
    public boolean isEmpty() {
        return adjacencyMap.isEmpty();
    }

    Map<Transform, List<Transform>> adjacencyMap() {
        Map<Transform, List<Transform>> safeCopy = new LinkedHashMap<>();
        adjacencyMap.forEach((k, v) -> safeCopy.put(k, new ArrayList<>(v)));
        return safeCopy;
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

    private void register(Transform stage) {
        List<Transform> prev = adjacencyMap.putIfAbsent(stage, new ArrayList<>());
        assert prev == null : "Double registration of a Stage with this Pipeline: " + stage;
    }

    /**
     * Context passed to {@link #toDag(Context)}.
     */
    public interface Context {
        int defaultLocalParallelism();
    }
}
