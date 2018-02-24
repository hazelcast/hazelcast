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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.pipeline.transform.WindowAggregateTransform;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation1;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Javadoc pending.
 */
public class StageWithWindowImpl<T> implements StageWithWindow<T> {

    @Nonnull
    private final StreamStageImpl<T> streamStage;
    @Nonnull
    private final WindowDefinition wDef;

    StageWithWindowImpl(@Nonnull StreamStageImpl<T> streamStage, @Nonnull WindowDefinition wDef) {
        this.streamStage = streamStage;
        this.wDef = wDef;
    }

    @Nonnull @Override
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Nonnull @Override
    public StreamStage<T> streamStage() {
        return streamStage;
    }

    @Nonnull @Override
    public <K> StageWithGroupingAndWindow<T, K> groupingKey(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        return new StageWithGroupingAndWindowImpl<>(streamStage, keyFn, wDef);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        ensureJetEvents(streamStage, "This pipeline stage");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        AggregateOperation1<JetEvent<T>, A, R> adaptedAggrOp =
                adaptAggregateOperation1(aggrOp);
        return streamStage.attach(new WindowAggregateTransform<A, R, JetEvent<OUT>>(
                singletonList(streamStage.transform), wDef, adaptedAggrOp,
                fnAdapter.adaptWindowResultFn(mapToOutputFn)
        ), fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = (ComputeStageImplBase) stage1;
        ensureJetEvents(streamStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return streamStage.attach(new WindowAggregateTransform<A, R, JetEvent<OUT>>(
                asList(streamStage.transform, stageImpl1.transform),
                wDef,
                adaptAggregateOperation(aggrOp),
                fnAdapter.adaptWindowResultFn(mapToOutputFn)
        ), fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = (ComputeStageImplBase) stage1;
        ComputeStageImplBase stageImpl2 = (ComputeStageImplBase) stage2;
        ensureJetEvents(streamStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return streamStage.attach(new WindowAggregateTransform<A, R, JetEvent<OUT>>(
                asList(streamStage.transform, stageImpl1.transform, stageImpl2.transform),
                wDef,
                adaptAggregateOperation(aggrOp),
                fnAdapter.adaptWindowResultFn(mapToOutputFn)
        ), fnAdapter);
    }
}
