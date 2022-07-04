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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.pipeline.transform.WindowGroupTransform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.GroupAggregateBuilder1;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.WindowGroupAggregateBuilder1;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.toList;

/**
 * Support class for {@link GroupAggregateBuilder1}
 * and {@link WindowGroupAggregateBuilder1}. The
 * motivation is to have the ability to specify different output
 * types ({@code Entry<K, R>} vs. {@code TimestampedEntry<K, R>}).
 *
 * @param <K> type of the grouping key
 */
@SuppressWarnings("rawtypes")
public class GrAggBuilder<K> {
    private final PipelineImpl pipelineImpl;
    private final WindowDefinition wDef;
    private final List<ComputeStageImplBase> upstreamStages = new ArrayList<>();
    private final List<FunctionEx<?, ? extends K>> keyFns = new ArrayList<>();

    public GrAggBuilder(BatchStageWithKey<?, K> stage0) {
        ComputeStageImplBase computeStage = ((StageWithGroupingBase) stage0).computeStage;
        pipelineImpl = (PipelineImpl) computeStage.getPipeline();
        wDef = null;
        upstreamStages.add(computeStage);
        keyFns.add(stage0.keyFn());
    }

    public GrAggBuilder(StageWithKeyAndWindow<?, K> stage) {
        ComputeStageImplBase computeStage = ((StageWithGroupingBase) stage).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        pipelineImpl = (PipelineImpl) computeStage.getPipeline();
        wDef = stage.windowDefinition();
        upstreamStages.add(computeStage);
        keyFns.add(stage.keyFn());
    }

    @SuppressWarnings("unchecked")
    public <T> Tag<T> add(StreamStageWithKey<T, K> stage) {
        ComputeStageImplBase computeStage = ((StageWithGroupingBase) stage).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        upstreamStages.add(computeStage);
        keyFns.add(stage.keyFn());
        return (Tag<T>) tag(upstreamStages.size() - 1);
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(BatchStageWithKey<E, K> stage) {
        upstreamStages.add(((StageWithGroupingBase) stage).computeStage);
        keyFns.add(stage.keyFn());
        return (Tag<E>) tag(upstreamStages.size() - 1);
    }

    public <A, R, OUT> BatchStage<OUT> buildBatch(
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull BiFunctionEx<? super K, ? super R, OUT> mapToOutputFn
    ) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        List<Transform> upstreamTransforms = toList(upstreamStages, s -> s.transform);
        AbstractTransform transform = new GroupTransform<>(upstreamTransforms, keyFns, aggrOp, mapToOutputFn);
        pipelineImpl.connect(upstreamStages, transform);
        return new BatchStageImpl<>(transform, pipelineImpl);
    }

    public <A, R> StreamStage<KeyedWindowResult<K, R>> buildStream(@Nonnull AggregateOperation<A, ? extends R> aggrOp) {
        List<Transform> upstreamTransforms = toList(upstreamStages, s -> s.transform);
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        // Casts in this expression are a workaround for JDK 8 compiler bug:
        @SuppressWarnings("unchecked")
        List<FunctionEx<?, ? extends K>> adaptedKeyFns = toList(keyFns, fn -> fnAdapter.adaptKeyFn((FunctionEx) fn));
        AbstractTransform transform = new WindowGroupTransform<K, R>(
                upstreamTransforms, wDef, adaptedKeyFns, fnAdapter.adaptAggregateOperation(aggrOp));
        pipelineImpl.connect(upstreamStages, transform);
        return new StreamStageImpl<>(transform, fnAdapter, pipelineImpl);
    }
}
