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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.pipeline.transform.WindowAggregateTransform;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.util.Util.toList;

public class AggBuilder {
    @Nullable
    private final WindowDefinition wDef;

    @Nonnull
    private final PipelineImpl pipelineImpl;

    @Nonnull
    @SuppressWarnings("rawtypes")
    private final List<GeneralStage> upstreamStages = new ArrayList<>();

    public <T0> AggBuilder(
            @Nonnull GeneralStage<T0> stage0,
            @Nullable WindowDefinition wDef
    ) {
        this.wDef = wDef;
        this.pipelineImpl = ((AbstractStage) stage0).pipelineImpl;
        add(stage0);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E> Tag<E> add(@Nonnull GeneralStage<E> stage) {
        if (wDef != null) {
            ensureJetEvents((ComputeStageImplBase) stage, "This pipeline stage");
        }
        upstreamStages.add(stage);
        return (Tag<E>) tag(upstreamStages.size() - 1);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <A, R, OUT, OUT_STAGE extends GeneralStage<OUT>> OUT_STAGE build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull CreateOutStageFn<OUT, OUT_STAGE> createOutStageFn
    ) {
        AggregateOperation adaptedAggrOp = wDef != null ? ADAPT_TO_JET_EVENT.adaptAggregateOperation(aggrOp) : aggrOp;
        List<Transform> upstreamTransforms = toList(upstreamStages, s -> ((AbstractStage) s).transform);
        final AbstractTransform transform;
        if (wDef != null) {
            transform = new WindowAggregateTransform<>(upstreamTransforms, wDef, adaptedAggrOp);
        } else {
            transform = new AggregateTransform<>(upstreamTransforms, adaptedAggrOp);
        }
        OUT_STAGE attached = createOutStageFn.get(transform, ADAPT_TO_JET_EVENT, pipelineImpl);
        pipelineImpl.connectGeneralStages(upstreamStages, transform);
        return attached;
    }

    @FunctionalInterface
    public interface CreateOutStageFn<OUT, OUT_STAGE extends GeneralStage<OUT>> {
        OUT_STAGE get(Transform transform, FunctionAdapter fnAdapter, PipelineImpl pipeline);
    }
}
