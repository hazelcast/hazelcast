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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.impl.TopologicalSorter.checkTopologicalSort;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
public class Planner {

    private static final ILogger LOGGER = Logger.getLogger(Planner.class);

    /**
     * Maximum gap between two consecutive watermarks. This is not technically
     * necessary, but improves debugging by avoiding too large gaps between WMs
     * and the user can better observe if input to WM coalescing is lagging.
     */
    private static final int MAXIMUM_WATERMARK_GAP = 1000;

    public final DAG dag = new DAG();
    public final Map<Transform, PlannerVertex> xform2vertex = new HashMap<>();
    private final PipelineImpl pipeline;

    Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    public boolean isPreserveOrder() {
        return pipeline.isPreserveOrder();
    }

    @SuppressWarnings("rawtypes")
    DAG createDag(Context context) {
        pipeline.makeNamesUnique();
        Map<Transform, List<Transform>> adjacencyMap = pipeline.adjacencyMap();
        validateNoLeakage(adjacencyMap);
        checkTopologicalSort(adjacencyMap.entrySet());
        // Find the greatest common denominator of all frame lengths
        // appearing in the pipeline
        long frameSizeGcd = Util.gcd(adjacencyMap.keySet().stream()
                                                 .map(Transform::preferredWatermarkStride)
                                                 .filter(frameSize -> frameSize > 0)
                                                 .mapToLong(i -> i)
                                                 .toArray());
        if (frameSizeGcd == 0) {
            // even if there are no window aggregations, we want the watermarks for latency debugging
            frameSizeGcd = MAXIMUM_WATERMARK_GAP;
        }
        if (frameSizeGcd > MAXIMUM_WATERMARK_GAP) {
            frameSizeGcd = Util.gcd(frameSizeGcd, MAXIMUM_WATERMARK_GAP);
        }
        LoggingUtil.logFine(LOGGER, "Watermarks in the pipeline will be throttled to %d", frameSizeGcd);
        // Update watermark throttling frame length on all transforms with the determined length
        for (Transform transform : adjacencyMap.keySet()) {
            if (transform instanceof StreamSourceTransform) {
                StreamSourceTransform t = (StreamSourceTransform) transform;
                EventTimePolicy policy = t.getEventTimePolicy();
                if (policy != null) {
                    t.setEventTimePolicy(withFrameSize(policy, frameSizeGcd));
                }
            } else if (transform instanceof TimestampTransform) {
                TimestampTransform t = (TimestampTransform) transform;
                t.setEventTimePolicy(withFrameSize(t.getEventTimePolicy(), frameSizeGcd));
            }
        }

        // fuse subsequent map/filter/flatMap transforms into one
        Map<Transform, List<Transform>> originalParents = new HashMap<>();
        List<Transform> transforms = new ArrayList<>(adjacencyMap.keySet());
        for (int i = 0; i < transforms.size(); i++) {
            Transform transform = transforms.get(i);
            List<Transform> chain = findFusableChain(transform, adjacencyMap);
            if (chain == null) {
                continue;
            }
            // remove transforms in the chain and replace the parent with a fused transform
            transforms.removeAll(chain.subList(1, chain.size()));
            Transform fused = fuseFlatMapTransforms(chain);
            transforms.set(i, fused);
            Transform lastInChain = chain.get(chain.size() - 1);
            for (Transform downstream : adjacencyMap.get(lastInChain)) {
                originalParents.put(downstream, new ArrayList<>(downstream.upstream()));
                downstream.upstream().replaceAll(p -> p == lastInChain ? fused : p);
            }
        }

        for (Transform transform : transforms) {
            transform.addToDag(this, context);
        }

        // restore original parents
        for (Entry<Transform, List<Transform>> en : originalParents.entrySet()) {
            List<Transform> upstream = en.getKey().upstream();
            for (int i = 0; i < upstream.size(); i++) {
                en.getKey().upstream().set(i, en.getValue().get(i));
            }
        }

        return dag;
    }

    private static List<Transform> findFusableChain(
            @Nonnull Transform transform,
            @Nonnull Map<Transform, List<Transform>> adjacencyMap
    ) {
        ArrayList<Transform> chain = new ArrayList<>();
        for (;;) {
            if (!(transform instanceof MapTransform || transform instanceof FlatMapTransform)) {
                break;
            }
            chain.add(transform);
            List<Transform> downstream = adjacencyMap.get(transform);
            if (downstream.size() != 1) {
                break;
            }
            Transform nextTransform = downstream.get(0);
            if (nextTransform.localParallelism() != transform.localParallelism()
                    || nextTransform.shouldRebalanceInput(0)
            ) {
                break;
            }
            transform = nextTransform;
        }
        return chain.size() > 1 ? chain : null;
    }

    @SuppressWarnings("rawtypes")
    private static Transform fuseFlatMapTransforms(List<Transform> chain) {
        assert chain.size() > 1 : "chain.size()=" + chain.size();
        assert chain.get(0).upstream().size() == 1;

        int lastFlatMap = 0;
        FunctionEx<Object, Traverser> flatMapFn = null;
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i) instanceof FlatMapTransform) {
                FunctionEx<Object, Traverser> function = ((FlatMapTransform) chain.get(i)).flatMapFn();
                FunctionEx<Object, Object> inputMapFn = mergeMapFunctions(chain.subList(lastFlatMap, i));
                if (inputMapFn != null) {
                    flatMapFn = flatMapFn == null
                            ? (Object t) -> {
                                Object mappedValue = inputMapFn.apply(t);
                                return mappedValue != null ? function.apply(mappedValue) : Traversers.empty();
                            }
                            : flatMapFn.andThen(r -> r.map(inputMapFn).flatMap(function));
                } else {
                    flatMapFn = flatMapFn == null
                            ? function::apply
                            : flatMapFn.andThen(r -> r.flatMap(function));
                }
                lastFlatMap = i + 1;
            }
        }

        FunctionEx trailingMapFn = mergeMapFunctions(chain.subList(lastFlatMap, chain.size()));
        String name = chain.stream().map(Transform::name).collect(Collectors.joining(", ", "fused(", ")"));
        Transform fused;
        if (flatMapFn == null) {
            fused = new MapTransform(name, chain.get(0).upstream().get(0), trailingMapFn);
        } else {
            if (trailingMapFn != null) {
                flatMapFn = flatMapFn.andThen(t -> t.map(trailingMapFn));
            }
            fused = new FlatMapTransform(name, chain.get(0).upstream().get(0), flatMapFn);
        }
        // if the first stage of the chain is rebalanced, then we set
        // the rebalance flag of the created fused stage. Only consider
        // the case when first element of the chain is rebalanced
        // because there isn't any other case. If any stage in the
        // middle includes rebalance, then those stages are not fused
        // by findFusableChain().
        fused.setRebalanceInput(0, chain.get(0).shouldRebalanceInput(0));
        return fused;
    }

    @SuppressWarnings("rawtypes")
    private static FunctionEx mergeMapFunctions(List<Transform> chain) {
        if (chain.isEmpty()) {
            return null;
        }
        List<FunctionEx> functions = toList(chain, t -> ((MapTransform) t).mapFn());
        return t -> {
            Object result = t;
            for (int i = 0; i < functions.size() && result != null; i++) {
                result = functions.get(i).apply(result);
            }
            return result;
        };
    }

    private static void validateNoLeakage(Map<Transform, List<Transform>> adjacencyMap) {
        List<Transform> leakages = adjacencyMap
                .entrySet().stream()
                .filter(e -> !(e.getKey() instanceof SinkTransform))
                .filter(e -> e.getValue().isEmpty())
                .map(Entry::getKey)
                .collect(toList());
        if (!leakages.isEmpty()) {
            throw new IllegalArgumentException("These transforms have nothing attached to them: " + leakages);
        }
    }

    public PlannerVertex addVertex(Transform transform, String name, int localParallelism,
                                   SupplierEx<Processor> procSupplier) {
        return addVertex(transform, name, localParallelism, ProcessorMetaSupplier.of(procSupplier));
    }

    public PlannerVertex addVertex(Transform transform, String name, int localParallelism,
                                   ProcessorSupplier procSupplier) {
        return addVertex(transform, name, localParallelism, ProcessorMetaSupplier.of(procSupplier));
    }

    public PlannerVertex addVertex(Transform transform, String name, int localParallelism,
                                   ProcessorMetaSupplier metaSupplier) {
        PlannerVertex pv = new PlannerVertex(dag.newVertex(name, metaSupplier));
        pv.v.localParallelism(localParallelism);
        xform2vertex.put(transform, pv);
        return pv;
    }

    public void addEdges(Transform transform, Vertex toVertex, ObjIntConsumer<Edge> configureEdgeFn) {
        int destOrdinal = 0;
        for (Transform fromTransform : transform.upstream()) {
            PlannerVertex fromPv = xform2vertex.get(fromTransform);
            Edge edge = from(fromPv.v, fromPv.nextAvailableOrdinal()).to(toVertex, destOrdinal);
            dag.edge(edge);
            configureEdgeFn.accept(edge, destOrdinal);
            applyRebalancing(edge, transform);
            destOrdinal++;
        }
    }

    public static void applyRebalancing(Edge edge, Transform toTransform) {
        int destOrdinal = edge.getDestOrdinal();
        if (!toTransform.shouldRebalanceInput(destOrdinal)) {
            return;
        }
        edge.distributed();
        FunctionEx<?, ?> keyFn = toTransform.partitionKeyFnForInput(destOrdinal);
        if (keyFn != null) {
            edge.partitioned(keyFn);
        }
        if (edge.getRoutingPolicy() == Edge.RoutingPolicy.ISOLATED) {
            throw new IllegalArgumentException("Using rebalance without a key directly breaks the order. " +
                    "When the \"preserveOrder\" property is active, rebalance without a key is not allowed to use");
        }
    }

    public void addEdges(Transform transform, Vertex toVertex, Consumer<Edge> configureEdgeFn) {
        addEdges(transform, toVertex, (e, ord) -> configureEdgeFn.accept(e));
    }

    public void addEdges(Transform transform, Vertex toVertex) {
        addEdges(transform, toVertex, e -> { });
    }

    /**
     * Returns a new instance with emit policy replaced with the given
     * argument.
     */
    @Nonnull
    private static <T> EventTimePolicy<T> withFrameSize(
            EventTimePolicy<T> original, long watermarkThrottlingFrameSize
    ) {
        return eventTimePolicy(original.timestampFn(), original.wrapFn(), original.newWmPolicyFn(),
                watermarkThrottlingFrameSize, 0, original.idleTimeoutMillis());
    }

    public static <E> List<E> tailList(List<E> list) {
        return list.subList(1, list.size());
    }

    public static class PlannerVertex {
        public final Vertex v;

        private int availableOrdinal;

        PlannerVertex(Vertex v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return v.toString();
        }

        public int nextAvailableOrdinal() {
            return availableOrdinal++;
        }
    }
}
