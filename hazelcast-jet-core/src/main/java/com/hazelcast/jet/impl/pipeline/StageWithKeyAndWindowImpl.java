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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.pipeline.transform.WindowGroupTransform;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation2;
import static com.hazelcast.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation3;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StageWithKeyAndWindowImpl<T, K>
        extends StageWithGroupingBase<T, K>
        implements StageWithKeyAndWindow<T, K> {

    @Nonnull
    private final WindowDefinition wDef;

    StageWithKeyAndWindowImpl(
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

    public <R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attachAggregate(aggrOp, mapToOutputFn);
    }

    // This method was extracted in order to capture the wildcard parameter A.
    private <A, R, OUT> StreamStage<OUT> attachAggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R, JetEvent<OUT>>(
                        singletonList(computeStage.transform),
                        wDef,
                        singletonList(fnAdapter.adaptKeyFn(keyFn())),
                        fnAdapter.adaptAggregateOperation1(aggrOp),
                        fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    public <T1, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(((StageWithGroupingBase) stage1).computeStage, "stage1");
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attachAggregate2(stage1, aggrOp, mapToOutputFn);
    }

    // This method was extracted in order to capture the wildcard parameter A.
    private <T1, A, R, OUT> StreamStage<OUT> attachAggregate2(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        Transform upstream1 = ((StageWithGroupingBase) stage1).computeStage.transform;
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R, JetEvent<OUT>>(
                        asList(computeStage.transform, upstream1),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()),
                                fnAdapter.adaptKeyFn(stage1.keyFn())),
                        adaptAggregateOperation2(aggrOp),
                        fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    public <T1, T2, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        ComputeStageImplBase stageImpl1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase stageImpl2 = ((StageWithGroupingBase) stage2).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(stageImpl1, "stage1");
        ensureJetEvents(stageImpl2, "stage2");
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attachAggregate3(stage1, stage2, aggrOp, mapToOutputFn);
    }

    // This method was extracted in order to capture the wildcard parameter A.
    private <T1, T2, A, R, OUT> StreamStage<OUT> attachAggregate3(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        Transform transform1 = ((StageWithGroupingBase) stage1).computeStage.transform;
        Transform transform2 = ((StageWithGroupingBase) stage2).computeStage.transform;
        JetEventFunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R, JetEvent<OUT>>(
                        asList(computeStage.transform, transform1, transform2),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()),
                                fnAdapter.adaptKeyFn(stage1.keyFn()),
                                fnAdapter.adaptKeyFn(stage2.keyFn())),
                        adaptAggregateOperation3(aggrOp),
                        fnAdapter.adaptKeyedWindowResultFn(mapToOutputFn)
                ),
                fnAdapter);
    }
}
