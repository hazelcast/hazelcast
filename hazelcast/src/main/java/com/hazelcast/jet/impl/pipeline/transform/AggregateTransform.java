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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.accumulateP;
import static com.hazelcast.jet.core.processor.Processors.aggregateP;
import static com.hazelcast.jet.core.processor.Processors.combineP;

public class AggregateTransform<A, R> extends AbstractTransform {
    public static final String FIRST_STAGE_VERTEX_NAME_SUFFIX = "-prepare";

    private static final long serialVersionUID = 1L;

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
    public void addToDag(Planner p, Context context) {
        if (aggrOp.combineFn() == null) {
            addToDagSingleStage(p);
        } else {
            addToDagTwoStage(p, context);
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
        determinedLocalParallelism(1);
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(), aggregateP(aggrOp));
        p.addEdges(this, pv.v, edge -> edge.distributed().allToOne(name().hashCode()));
    }

    //  WHEN PRESERVE ORDER IS NOT ACTIVE
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
    //  WHEN PRESERVE ORDER IS ACTIVE
    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //                isolated       isolated
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

    private void addToDagTwoStage(Planner p, Context context) {
        String vertexName = name();
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, p.isPreserveOrder());
        Vertex v1 = p.dag.newVertex(vertexName + FIRST_STAGE_VERTEX_NAME_SUFFIX, accumulateP(aggrOp))
                         .localParallelism(determinedLocalParallelism());
        if (p.isPreserveOrder()) {
            p.addEdges(this, v1, Edge::isolated);
        } else {
            p.addEdges(this, v1);
        }

        determinedLocalParallelism(1);
        PlannerVertex pv2 = p.addVertex(this, vertexName, determinedLocalParallelism(),
                ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of(combineP(aggrOp)), vertexName));

        p.dag.edge(between(v1, pv2.v)
                .distributed()
                .allToOne(vertexName));
    }
}
