/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.function.WindowResultFunction;
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
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DO_NOT_ADAPT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AggBuilder {
    @Nullable
    private final WindowDefinition wDef;
    @Nonnull
    private final PipelineImpl pipelineImpl;
    @Nonnull
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
    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(@Nonnull GeneralStage<E> stage) {
        if (wDef != null) {
            ensureJetEvents((ComputeStageImplBase) stage, "This pipeline stage");
        }
        upstreamStages.add(stage);
        return (Tag<E>) tag(upstreamStages.size() - 1);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public <A, R, OUT, OUT_STAGE extends GeneralStage<OUT>> OUT_STAGE build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull CreateOutStageFn<OUT, OUT_STAGE> createOutStageFn,
            @Nullable WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        AggregateOperation adaptedAggrOp = wDef != null ? adaptAggregateOperation(aggrOp) : aggrOp;
        List<Transform> upstreamTransforms = upstreamStages
                .stream()
                .map(s -> ((AbstractStage) s).transform)
                .collect(toList());
        final Transform transform;
        if (wDef != null) {
            requireNonNull(mapToOutputFn, "wDef != null but mapToOutputFn == null");
            transform = new WindowAggregateTransform<>(upstreamTransforms, wDef, adaptedAggrOp, mapToOutputFn);
        } else {
            transform = new AggregateTransform<>(upstreamTransforms, adaptedAggrOp);
        }
        OUT_STAGE attached = createOutStageFn.get(transform, DO_NOT_ADAPT, pipelineImpl);
        pipelineImpl.connect(upstreamTransforms, transform);
        return attached;
    }

    @FunctionalInterface
    public interface CreateOutStageFn<OUT, OUT_STAGE extends GeneralStage<OUT>> {
        OUT_STAGE get(Transform transform, FunctionAdapter fnAdapter, PipelineImpl pipeline);
    }
}
