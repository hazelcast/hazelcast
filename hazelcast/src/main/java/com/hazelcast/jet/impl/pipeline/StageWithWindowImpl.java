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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.pipeline.transform.WindowAggregateTransform;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation2;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation3;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StageWithWindowImpl<T> implements StageWithWindow<T> {

    private final StreamStageImpl<T> streamStage;
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
    public <K> StageWithKeyAndWindow<T, K> groupingKey(
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        return new StageWithKeyAndWindowImpl<>(streamStage, keyFn, wDef);
    }

    @Nonnull @Override
    public <R> StreamStage<WindowResult<R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        ensureJetEvents(streamStage, "This pipeline stage");
        return attachAggregate(aggrOp);
    }

    // This method was extracted in order to capture the wildcard parameter A.
    private <A, R> StreamStage<WindowResult<R>> attachAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return streamStage.attach(new WindowAggregateTransform<A, R>(
                        singletonList(streamStage.transform),
                        wDef,
                        fnAdapter.adaptAggregateOperation1(aggrOp)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("rawtypes")
    public <T1, R> StreamStage<WindowResult<R>> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    ) {
        ensureJetEvents(streamStage, "This pipeline stage");
        ensureJetEvents((ComputeStageImplBase) stage1, "stage1");
        return attachAggregate2(stage1, aggrOp);
    }

    // This method was extracted in order to capture the wildcard parameter A.
    @SuppressWarnings("rawtypes")
    private <T1, A, R> StreamStage<WindowResult<R>> attachAggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return streamStage.attach(new WindowAggregateTransform<A, R>(
                        asList(streamStage.transform, ((StreamStageImpl) stage1).transform),
                        wDef,
                        adaptAggregateOperation2(aggrOp)
                ),
                singletonList(stage1),
                ADAPT_TO_JET_EVENT);
    }

    @Nonnull @Override
    @SuppressWarnings("rawtypes")
    public <T1, T2, R> StreamStage<WindowResult<R>> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        ComputeStageImplBase stageImpl1 = (ComputeStageImplBase) stage1;
        ComputeStageImplBase stageImpl2 = (ComputeStageImplBase) stage2;
        ensureJetEvents(streamStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        return attachAggregate3(stage1, stage2, aggrOp);
    }

    // This method was extracted in order to capture the wildcard parameter A.
    @SuppressWarnings("rawtypes")
    private <T1, T2, K, A, R> StreamStage<WindowResult<R>> attachAggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return streamStage.attach(new WindowAggregateTransform<A, R>(
                        asList(streamStage.transform,
                                ((StreamStageImpl) stage1).transform,
                                ((StreamStageImpl) stage2).transform),
                        wDef,
                        adaptAggregateOperation3(aggrOp)
                ),
                asList(stage1, stage2),
                ADAPT_TO_JET_EVENT);
    }
}
