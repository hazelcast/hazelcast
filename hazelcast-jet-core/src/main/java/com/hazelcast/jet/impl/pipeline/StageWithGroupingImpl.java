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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StageWithGrouping;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DONT_ADAPT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StageWithGroupingImpl<T, K> extends StageWithGroupingBase<T, K> implements StageWithGrouping<T, K> {

    StageWithGroupingImpl(
            @Nonnull ComputeStageImplBase<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    @Nonnull
    public <A, R, OUT> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return computeStage.attach(new GroupTransform<>(
                        singletonList(computeStage.transform),
                        singletonList(keyFn()),
                        aggrOp,
                        mapToOutputFn),
                DONT_ADAPT);
    }

    @Nonnull
    public <T1, A, R, OUT> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull StageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, transformOf(stage1)),
                        asList(keyFn(), stage1.keyFn()),
                        aggrOp,
                        mapToOutputFn
                ), DONT_ADAPT);
    }

    @Nonnull
    public <T1, T2, A, R, OUT> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull StageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, transformOf(stage1), transformOf(stage2)),
                        asList(keyFn(), stage1.keyFn(), stage2.keyFn()),
                        aggrOp,
                        mapToOutputFn),
                DONT_ADAPT);
    }
}
