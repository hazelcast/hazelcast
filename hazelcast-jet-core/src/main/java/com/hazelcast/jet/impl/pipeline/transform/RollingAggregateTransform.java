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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.rollingAggregateP;

public class RollingAggregateTransform<T, K, R, OUT> extends AbstractTransform {
    private final DistributedFunction<? super T, ? extends K> keyFn;
    @Nonnull private final AggregateOperation1<? super T, ?, ? extends R> aggrOp;
    @Nonnull private final DistributedBiFunction<? super K, ? super R, ? extends OUT> mapToOutputFn;

    public RollingAggregateTransform(
            @Nonnull Transform upstream,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        super("rolling-aggregate", upstream);
        this.keyFn = keyFn;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                rollingAggregateP(keyFn, aggrOp, mapToOutputFn));
        p.addEdges(this, pv.v, edge -> edge.partitioned(keyFn).distributed());
    }
}
