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
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.pipeline.transform.WindowGroupTransform;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithGrouping;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.aggregate.AggregateOperations.pickAny;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation1;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation2;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation3;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptKeyFn;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StageWithGroupingAndWindowImpl<T, K>
        extends StageWithGroupingBase<T, K>
        implements StageWithGroupingAndWindow<T, K> {

    @Nonnull
    private final WindowDefinition wDef;

    StageWithGroupingAndWindowImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull WindowDefinition wDef
    ) {
        super(computeStage, keyFn);
        this.wDef = wDef;
    }

    @Nonnull @Override
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Nonnull
    public <R> StreamStage<R> distinct(
            @Nonnull WindowResultFunction<? super T, ? extends R> mapToOutputFn
    ) {
        return aggregate(pickAny(), (start, end, key, item) -> mapToOutputFn.apply(start, end, item));
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, A, R, JetEvent<OUT>>(
                        singletonList(computeStage.transform),
                        wDef,
                        singletonList(adaptKeyFn(keyFn())),
                        adaptAggregateOperation1(aggrOp),
                        fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, A, R, JetEvent<OUT>>(
                asList(computeStage.transform, stageImpl1.transform),
                wDef,
                asList(adaptKeyFn(keyFn()),
                       adaptKeyFn(stage1.keyFn())),
                adaptAggregateOperation2(aggrOp),
                fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
        ), fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase stageImpl2 = ((StageWithGroupingBase) stage2).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(
                new WindowGroupTransform<K, A, R, JetEvent<OUT>>(
                        asList(computeStage.transform, stageImpl1.transform, stageImpl2.transform),
                        wDef,
                        asList(adaptKeyFn(keyFn()),
                               adaptKeyFn(stage1.keyFn()),
                               adaptKeyFn(stage2.keyFn())),
                        adaptAggregateOperation3(aggrOp),
                        fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
                ), fnAdapter);
    }
}
