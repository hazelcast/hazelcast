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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.pipeline.JetEvent;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.SessionWindowDef;
import com.hazelcast.jet.pipeline.SlidingWindowDef;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.pipeline.transform.AbstractTransform.Optimization.MEMORY;
import static com.hazelcast.jet.pipeline.WindowDefinition.WindowKind.SESSION;
import static java.util.Collections.nCopies;

public class WindowGroupTransform<K, A, R, OUT> extends AbstractTransform {
    @Nonnull
    private final WindowDefinition wDef;
    @Nonnull
    private final List<DistributedFunction<?, ? extends K>> keyFns;
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn;

    public WindowGroupTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull WindowDefinition wDef,
            @Nonnull List<DistributedFunction<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        super(createName(wDef), upstream);
        this.wDef = wDef;
        this.keyFns = keyFns;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    private static String createName(WindowDefinition wDef) {
        return wDef.kind().name().toLowerCase() + "-window";
    }

    @Override
    public void addToDag(Planner p) {
        if (wDef.kind() == SESSION) {
            addSessionWindow(p, wDef.downcast());
        } else if (aggrOp.combineFn() == null || getOptimization() == MEMORY) {
            addSlidingWindowSingleStage(p, wDef.downcast());
        } else {
            addSlidingWindowTwoStage(p, wDef.downcast());
        }
    }

    //               ---------       ---------
    //              | source0 | ... | sourceN |
    //               ---------       ---------
    //                   |              |
    //              distributed    distributed
    //              partitioned    partitioned
    //                   \              /
    //                    ---\    /-----
    //                        v  v
    //             ---------------------------
    //            | aggregateToSlidingWindowP |
    //             ---------------------------
    private void addSlidingWindowSingleStage(Planner p, SlidingWindowDef wDef) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                aggregateToSlidingWindowP(
                        keyFns,
                        nCopies(keyFns.size(), (DistributedToLongFunction<JetEvent>) JetEvent::timestamp),
                        TimestampKind.EVENT,
                        wDef.toSlidingWindowPolicy(),
                        aggrOp,
                        mapToOutputFn
                ));
        p.addEdges(this, pv.v, (e, ord) -> e.partitioned(keyFns.get(ord)));
    }

    //              ---------       ---------
    //             | source0 | ... | sourceN |
    //              ---------       ---------
    //                  |               |
    //                local           local
    //             partitioned     partitioned
    //                  v               v
    //                 --------------------
    //                | accumulateByFrameP |
    //                 --------------------
    //                           |
    //                      distributed
    //                      partitioned
    //                           v
    //              -------------------------
    //             | combineToSlidingWindowP |
    //              -------------------------
    private void addSlidingWindowTwoStage(Planner p, SlidingWindowDef wDef) {
        String namePrefix = p.uniqueVertexName(name(), "-step");
        SlidingWindowPolicy winPolicy = wDef.toSlidingWindowPolicy();
        Vertex v1 = p.dag.newVertex(namePrefix + '1', accumulateByFrameP(
                keyFns,
                nCopies(keyFns.size(), (DistributedToLongFunction<JetEvent>) JetEvent::timestamp),
                TimestampKind.EVENT,
                winPolicy,
                aggrOp));
        v1.localParallelism(localParallelism());
        PlannerVertex pv2 = p.addVertex(this, namePrefix + '2', localParallelism(),
                combineToSlidingWindowP(winPolicy, aggrOp, mapToOutputFn));
        p.addEdges(this, v1, (e, ord) -> e.partitioned(keyFns.get(ord), HASH_CODE));
        p.dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }

    private void addSessionWindow(Planner p, SessionWindowDef wDef) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                aggregateToSessionWindowP(
                        wDef.sessionTimeout(),
                        nCopies(keyFns.size(), (DistributedToLongFunction<JetEvent>) JetEvent::timestamp),
                        keyFns,
                        aggrOp,
                        mapToOutputFn
                ));
        p.addEdges(this, pv.v, (e, ord) -> e.partitioned(keyFns.get(ord)));
    }
}
