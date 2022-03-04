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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.accumulateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.aggregateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;
import static com.hazelcast.jet.impl.pipeline.transform.AggregateTransform.FIRST_STAGE_VERTEX_NAME_SUFFIX;

public class GroupTransform<K, A, R, OUT> extends AbstractTransform {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private final List<FunctionEx<?, ? extends K>> groupKeyFns;
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final BiFunctionEx<? super K, ? super R, OUT> mapToOutputFn;

    public GroupTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull List<FunctionEx<?, ? extends K>> groupKeyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull BiFunctionEx<? super K, ? super R, OUT> mapToOutputFn
    ) {
        super(createName(upstream), upstream);
        this.groupKeyFns = groupKeyFns;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    private static String createName(@Nonnull List<Transform> upstream) {
        return upstream.size() == 1
                ? "group-and-aggregate"
                : upstream.size() + "-way cogroup-and-aggregate";
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, false);
        if (shouldRebalanceAnyInput() || aggrOp.combineFn() == null) {
            addToDagSingleStage(p);
        } else {
            addToDagTwoStage(p);
        }
    }

    //                   ---------        ---------
    //                  | source0 |  ... | sourceN |
    //                   ---------        ---------
    //                       |                  |
    //                  distributed        distributed
    //                  partitioned        partitioned
    //                       |                  |
    //                       \                  /
    //                        ----\      /------
    //                            v      v
    //                         -----------------
    //                        | aggregateByKeyP |
    //                         -----------------
    private void addToDagSingleStage(Planner p) {
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                aggregateByKeyP(groupKeyFns, aggrOp, mapToOutputFn));
        p.addEdges(this, pv.v, (e, ord) -> e.distributed().partitioned(groupKeyFns.get(ord)));
    }

    //                   ---------        ---------
    //                  | source0 |  ... | sourceN |
    //                   ---------        ---------
    //                       |                |
    //                     local            local
    //                  partitioned      partitioned
    //                       v                v
    //                      --------------------
    //                     |  accumulateByKeyP  |
    //                      --------------------
    //                                |
    //                           distributed
    //                           partitioned
    //                                v
    //                         ---------------
    //                        | combineByKeyP |
    //                         ---------------
    private void addToDagTwoStage(Planner p) {
        List<FunctionEx<?, ? extends K>> groupKeyFns = this.groupKeyFns;
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, accumulateByKeyP(groupKeyFns, aggrOp))
                .localParallelism(determinedLocalParallelism());
        PlannerVertex pv2 = p.addVertex(this, name(), determinedLocalParallelism(),
                combineByKeyP(aggrOp, mapToOutputFn));
        p.addEdges(this, v1, (e, ord) -> e.partitioned(groupKeyFns.get(ord), HASH_CODE));
        p.dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }
}
