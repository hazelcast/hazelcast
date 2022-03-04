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

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.function.KeyedWindowResultFunction;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.util.ConstantFunctionEx;
import com.hazelcast.jet.pipeline.SessionWindowDefinition;
import com.hazelcast.jet.pipeline.SlidingWindowDefinition;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.transform.AggregateTransform.FIRST_STAGE_VERTEX_NAME_SUFFIX;
import static java.util.Collections.nCopies;

public class WindowAggregateTransform<A, R> extends AbstractTransform {

    private static final long serialVersionUID = 1L;

    private static final int MAX_WATERMARK_STRIDE = 100;
    private static final int MIN_WMS_PER_SESSION = 100;
    @SuppressWarnings("rawtypes")
    private static final KeyedWindowResultFunction JET_EVENT_WINDOW_RESULT_FN =
            (start, end, ignoredKey, result, isEarly) ->
                    jetEvent(end - 1, new WindowResult<>(start, end, result, isEarly));

    @Nonnull
    private final AggregateOperation<A, ? extends R> aggrOp;
    @Nonnull
    private final WindowDefinition wDef;

    public WindowAggregateTransform(
            @Nonnull List<Transform> upstream,
            @Nonnull WindowDefinition wDef,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp
    ) {
        super(createName(wDef), upstream);
        this.aggrOp = aggrOp;
        this.wDef = wDef;
    }

    static String createName(WindowDefinition wDef) {
        if (wDef instanceof SlidingWindowDefinition) {
            return "sliding-window";
        } else if (wDef instanceof SessionWindowDefinition) {
            return "session-window";
        } else {
            throw new IllegalArgumentException(wDef.getClass().getName());
        }
    }

    /**
     * Returns the optimal watermark stride for this window definition.
     * Watermarks that are more spaced out are better for performance, but they
     * hurt the responsiveness of a windowed pipeline stage. The Planner will
     * determine the actual stride, which may be an integer fraction of the
     * value returned here.
     */
    static long preferredWatermarkStride(WindowDefinition wDef) {
        if (wDef instanceof SlidingWindowDefinition) {
            return ((SlidingWindowDefinition) wDef).slideBy();
        } else if (wDef instanceof SessionWindowDefinition) {
            long timeout = ((SessionWindowDefinition) wDef).sessionTimeout();
            return Math.min(MAX_WATERMARK_STRIDE, Math.max(1, timeout / MIN_WMS_PER_SESSION));
        } else {
            throw new IllegalArgumentException(wDef.getClass().getName());
        }
    }

    @Override
    public long preferredWatermarkStride() {
        return preferredWatermarkStride(wDef);
    }

    @Override
    public void addToDag(Planner p, Context context) {
        if (wDef instanceof SessionWindowDefinition) {
            addSessionWindow(p, (SessionWindowDefinition) wDef);
        } else if (aggrOp.combineFn() == null || wDef.earlyResultsPeriod() > 0) {
            addSlidingWindowSingleStage(p, (SlidingWindowDefinition) wDef);
        } else {
            addSlidingWindowTwoStage(p, (SlidingWindowDefinition) wDef, context);
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
    //             ---------------------------
    //            | aggregateToSlidingWindowP | local parallelism = 1
    //             ---------------------------
    private void addSlidingWindowSingleStage(Planner p, SlidingWindowDefinition wDef) {
        determinedLocalParallelism(1);
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                aggregateToSlidingWindowP(
                        nCopies(aggrOp.arity(), new ConstantFunctionEx<>(name().hashCode())),
                        nCopies(aggrOp.arity(), (ToLongFunctionEx<JetEvent<?>>) JetEvent::timestamp),
                        TimestampKind.EVENT,
                        slidingWinPolicy(wDef.windowSize(), wDef.slideBy()),
                        wDef.earlyResultsPeriod(),
                        aggrOp,
                        jetEventOfWindowResultFn()
                ));
        p.addEdges(this, pv.v, edge -> edge.distributed().allToOne(name().hashCode()));
    }

    // WHEN PRESERVE ORDER IS NOT ACTIVE
    //               --------        ---------
    //              | source0 | ... | sourceN |
    //               --------        ---------
    //                   |               |
    //                 local           local
    //                unicast         unicast
    //                   v               v
    //                  --------------------
    //                 | accumulateByFrameP | keyFn = constantKey()
    //                  --------------------
    //                           |
    //                      distributed
    //                       all-to-one
    //                           v
    //               -------------------------
    //              | combineToSlidingWindowP | local parallelism = 1
    //               -------------------------
    // WHEN PRESERVE ORDER IS ACTIVE
    //               --------        ---------
    //              | source0 | ... | sourceN |
    //               --------        ---------
    //                   |               |
    //                isolated        isolated
    //                   v               v
    //                  --------------------
    //                 | accumulateByFrameP | keyFn = constantKey()
    //                  --------------------
    //                           |
    //                      distributed
    //                       all-to-one
    //                           v
    //               -------------------------
    //              | combineToSlidingWindowP | local parallelism = 1
    //               -------------------------
    private void addSlidingWindowTwoStage(Planner p, SlidingWindowDefinition wDef, Context context) {
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, p.isPreserveOrder());
        SlidingWindowPolicy winPolicy = slidingWinPolicy(wDef.windowSize(), wDef.slideBy());
        Vertex v1 = p.dag.newVertex(name() + FIRST_STAGE_VERTEX_NAME_SUFFIX, accumulateByFrameP(
                nCopies(aggrOp.arity(), new ConstantFunctionEx<>(name().hashCode())),
                nCopies(aggrOp.arity(), (ToLongFunctionEx<JetEvent<?>>) JetEvent::timestamp),
                TimestampKind.EVENT,
                winPolicy,
                aggrOp
        ));

        v1.localParallelism(determinedLocalParallelism());
        if (p.isPreserveOrder()) {
            p.addEdges(this, v1, Edge::isolated);
        } else {
            // when preserveOrder is false, we use requested parallelism
            // for 1st stage: edge to it is local-unicast, each processor
            // can process part of the input which will be combined into
            // one result in 2nd stage.
            p.addEdges(this, v1);
        }
        determinedLocalParallelism(1);
        PlannerVertex pv2 = p.addVertex(this, name(), determinedLocalParallelism(),
                combineToSlidingWindowP(winPolicy, aggrOp, jetEventOfWindowResultFn()));

        p.dag.edge(between(v1, pv2.v).distributed().allToOne(name().hashCode()));
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
    //             ---------------------------
    //            | aggregateToSessionWindowP | local parallelism = 1
    //             ---------------------------
    private void addSessionWindow(Planner p, SessionWindowDefinition wDef) {
        determinedLocalParallelism(1);
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                aggregateToSessionWindowP(
                        wDef.sessionTimeout(),
                        wDef.earlyResultsPeriod(),
                        nCopies(aggrOp.arity(), (ToLongFunctionEx<JetEvent<?>>) JetEvent::timestamp),
                        nCopies(aggrOp.arity(), new ConstantFunctionEx<>(name().hashCode())),
                        aggrOp,
                        jetEventOfWindowResultFn()));
        p.addEdges(this, pv.v, edge -> edge.distributed().allToOne(name().hashCode()));
    }

    @SuppressWarnings("unchecked")
    private static <K, R> KeyedWindowResultFunction<K, R, JetEvent<R>> jetEventOfWindowResultFn() {
        return JET_EVENT_WINDOW_RESULT_FN;
    }
}
