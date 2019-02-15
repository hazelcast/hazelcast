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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.accumulateP;
import static com.hazelcast.jet.core.processor.Processors.aggregateP;
import static com.hazelcast.jet.core.processor.Processors.combineP;

public class AggregateTransform<A, R> extends AbstractTransform {
    public static final String FIRST_STAGE_VERTEX_NAME_SUFFIX = "-prepare";

    @Nonnull
    private final AggregateOperation<A, ? extends R> aggrOp;

    public AggregateTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp
    ) {
        super(createName(upstream), upstream);
        this.aggrOp = aggrOp;
    }

    private static String createName(@Nonnull List<Transform> upstream) {
        return upstream.size() == 1
                ? "aggregate"
                : upstream.size() + "-way co-aggregate";
    }

    @Override
    public void addToDag(Planner p) {
        // We don't use single-stage even when optimizing for memory because the
        // single-stage setup doesn't save memory with just one global key.
        if (aggrOp.combineFn() == null) {
            addToDagSingleStage(p);
        } else {
            addToDagTwoStage(p);
        }
    }

    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //              distributed    distributed
    //              all-to-one      all-to-one
    //                   \              /
    //                    ---\    /-----
    //                        v  v
    //                   ----------------
    //                  |   aggregateP   | local parallelism = 1
    //                   ----------------
    private void addToDagSingleStage(Planner p) {
        PlannerVertex pv = p.addVertex(this, name(), 1, aggregateP(aggrOp));
        p.addEdges(this, pv.v, edge -> edge.distributed().allToOne(name().hashCode()));
    }

    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //                 local          local
    //                unicast        unicast
    //                   v              v
    //                  -------------------
    //                 |    accumulateP    |
    //                  -------------------
    //                           |
    //                      distributed
    //                       all-to-one
    //                           v
    //                   ----------------
    //                  |    combineP    | local parallelism = 1
    //                   ----------------
    private void addToDagTwoStage(Planner p) {
        String vertexName = name();
        Vertex v1 = p.dag.newVertex(vertexName + FIRST_STAGE_VERTEX_NAME_SUFFIX, accumulateP(aggrOp))
                         .localParallelism(localParallelism());
        PlannerVertex pv2 = p.addVertex(this, vertexName, 1, combineP(aggrOp));
        p.addEdges(this, v1);
        p.dag.edge(between(v1, pv2.v).distributed().allToOne(name().hashCode()));
    }
}
