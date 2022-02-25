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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.pipeline.SinkImpl;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.FunctionAdapter.adaptingMetaSupplier;
import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;

public class SinkTransform<T> extends AbstractTransform {

    private static final long serialVersionUID = 1L;

    private static final int[] EMPTY_ORDINALS = new int[0];

    private final SinkImpl<T> sink;
    private final int[] ordinalsToAdapt;

    public SinkTransform(@Nonnull SinkImpl<T> sink, @Nonnull List<Transform> upstream, @Nonnull int[] ordinalsToAdapt) {
        super(sink.name(), upstream);
        this.sink = sink;
        this.ordinalsToAdapt = ordinalsToAdapt;
    }

    public SinkTransform(@Nonnull SinkImpl<T> sink, @Nonnull Transform upstream, boolean adaptToJetEvents) {
        super(sink.name(), upstream);
        this.sink = sink;
        this.ordinalsToAdapt = adaptToJetEvents ? new int[] {0} : EMPTY_ORDINALS;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(sink.metaSupplier().preferredLocalParallelism(), context, false);
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                adaptingMetaSupplier(sink.metaSupplier(), ordinalsToAdapt));
        p.addEdges(this, pv.v, (e, ord) -> {
            // note: have to use an all-to-one edge for the assertion sink.
            // all the items will be routed to the member with the partition key
            if (sink.getType() == TOTAL_PARALLELISM_ONE) {
                e.allToOne(sink.name()).distributed();
            } else {
                if (sink.getType().isPartitioned()) {
                    FunctionEx keyFn = sink.partitionKeyFunction();
                    if (arrayIndexOf(ord, ordinalsToAdapt) >= 0) {
                        keyFn = ADAPT_TO_JET_EVENT.adaptKeyFn(keyFn);
                    }
                    e.partitioned(keyFn, Partitioner.defaultPartitioner());
                }
                if (sink.getType().isDistributed()) {
                    e.distributed();
                }
            }
        });
    }
}
