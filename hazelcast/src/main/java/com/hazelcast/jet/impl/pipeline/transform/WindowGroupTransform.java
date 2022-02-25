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
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.function.KeyedWindowResultFunction;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.SessionWindowDefinition;
import com.hazelcast.jet.pipeline.SlidingWindowDefinition;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.transform.AggregateTransform.FIRST_STAGE_VERTEX_NAME_SUFFIX;
import static java.util.Collections.nCopies;

public class WindowGroupTransform<K, R> extends AbstractTransform {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("rawtypes")
    private static final KeyedWindowResultFunction JET_EVENT_KEYED_WINDOW_RESULT_FN =
            (winStart, winEnd, key, windowResult, isEarly) ->
                    jetEvent(winEnd - 1, new KeyedWindowResult<>(winStart, winEnd, key, windowResult, isEarly));

    @Nonnull
    private final WindowDefinition wDef;
    @Nonnull
    private final List<FunctionEx<?, ? extends K>> keyFns;
    @Nonnull
    private final AggregateOperation<?, ? extends R> aggrOp;

    public WindowGroupTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull WindowDefinition wDef,
            @Nonnull List<FunctionEx<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation<?, ? extends R> aggrOp
    ) {
        super(createName(wDef), upstream);
        this.wDef = wDef;
        this.keyFns = keyFns;
        this.aggrOp = aggrOp;
    }

    private static String createName(WindowDefinition wDef) {
        return WindowAggregateTransform.createName(wDef);
    }

    @Override
    public long preferredWatermarkStride() {
        return WindowAggregateTransform.preferredWatermarkStride(wDef);
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, false);
        if (wDef instanceof SessionWindowDefinition) {
            addSessionWindow(p, (SessionWindowDefinition) wDef);
        } else if (aggrOp.combineFn() == null || wDef.earlyResultsPeriod() > 0 || shouldRebalanceAnyInput()) {
            addSlidingWindowSingleStage(p, (SlidingWindowDefinition) wDef);
        } else {
            addSlidingWindowTwoStage(p, (SlidingWindowDefinition) wDef);
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
    private void addSlidingWindowSingleStage(Planner p, SlidingWindowDefinition wDef) {

        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                aggregateToSlidingWindowP(
                        keyFns,
                        nCopies(keyFns.size(), (ToLongFunctionEx<JetEvent<?>>) JetEvent::timestamp),
                        TimestampKind.EVENT,
                        slidingWinPolicy(wDef.windowSize(), wDef.slideBy()),
                        wDef.earlyResultsPeriod(),
                        aggrOp,
                        jetEventOfKeyedWindowResultFn()
                ));
        p.addEdges(this, pv.v, (e, ord) -> e.distributed().partitioned(keyFns.get(ord)));
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
    private void addSlidingWindowTwoStage(Planner p, SlidingWindowDefinition wDef) {
        SlidingWindowPolicy winPolicy = slidingWinPolicy(wDef.windowSize(), wDef.slideBy());
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, accumulateByFrameP(
                keyFns,
                nCopies(keyFns.size(), (ToLongFunctionEx<JetEvent<?>>) JetEvent::timestamp),
                TimestampKind.EVENT,
                winPolicy,
                aggrOp));
        v1.localParallelism(determinedLocalParallelism());
        PlannerVertex pv2 = p.addVertex(this, name(), determinedLocalParallelism(),
                combineToSlidingWindowP(winPolicy, aggrOp, jetEventOfKeyedWindowResultFn()));
        p.addEdges(this, v1, (e, ord) -> e.partitioned(keyFns.get(ord), HASH_CODE));
        p.dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
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
    //            | aggregateToSessionWindowP |
    //             ---------------------------
    private void addSessionWindow(Planner p, SessionWindowDefinition wDef) {
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                aggregateToSessionWindowP(
                        wDef.sessionTimeout(),
                        wDef.earlyResultsPeriod(),
                        nCopies(keyFns.size(), (ToLongFunctionEx<JetEvent<?>>) JetEvent::timestamp),
                        keyFns,
                        aggrOp,
                        jetEventOfKeyedWindowResultFn()
                ));
        p.addEdges(this, pv.v, (e, ord) -> e.distributed().partitioned(keyFns.get(ord)));
    }

    @SuppressWarnings("unchecked")
    private static <K, R> KeyedWindowResultFunction<K, R, JetEvent<? extends KeyedWindowResult<K, ? extends R>>>
    jetEventOfKeyedWindowResultFn() {
        return JET_EVENT_KEYED_WINDOW_RESULT_FN;
    }
}
