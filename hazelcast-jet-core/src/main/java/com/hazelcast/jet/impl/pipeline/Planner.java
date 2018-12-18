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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.noWatermarks;
import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
public class Planner {

    /**
     * Maximum gap between two consecutive watermarks. This is not technically
     * necessary, but improves debugging by avoiding too large gaps between WMs
     * and the user can better observe if input to WM coalescing is lagging.
     */
    private static final int MAXIMUM_WATERMARK_GAP = 1_000;

    public final DAG dag = new DAG();
    public final Map<Transform, PlannerVertex> xform2vertex = new HashMap<>();

    private final PipelineImpl pipeline;
    private final Set<String> vertexNames = new HashSet<>();

    Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    DAG createDag() {
        Map<Transform, List<Transform>> adjacencyMap = pipeline.adjacencyMap();
        validateNoLeakage(adjacencyMap);

        // Find the greatest common denominator of all frame lengths
        // appearing in the pipeline
        long frameSizeGcd = Util.gcd(adjacencyMap.keySet().stream()
                                                 .map(Transform::preferredWatermarkStride)
                                                 .filter(frameSize -> frameSize > 0)
                                                 .mapToLong(i -> i)
                                                 .toArray());
        // Update watermark emission policy on all wm gen params
        // with the GCD frame length
        WatermarkEmissionPolicy emitPolicy = frameSizeGcd > 0
                ? emitByFrame(tumblingWinPolicy(frameSizeGcd), MAXIMUM_WATERMARK_GAP)
                : noWatermarks();
        for (Transform transform : adjacencyMap.keySet()) {
            if (transform instanceof StreamSourceTransform) {
                StreamSourceTransform t = (StreamSourceTransform) transform;
                if (t.getEventTimePolicy() != null) {
                    t.setEventTimePolicy(t.getEventTimePolicy().withEmitPolicy(emitPolicy));
                }
            } else if (transform instanceof TimestampTransform) {
                TimestampTransform t = (TimestampTransform) transform;
                t.setEventTimePolicy(t.getEventTimePolicy().withEmitPolicy(emitPolicy));
            }
        }

        Iterable<Transform> sorted = topologicalSort(adjacencyMap, Object::toString);
        for (Transform transform : sorted) {
            transform.addToDag(this);
        }
        return dag;
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
                                   DistributedSupplier<Processor> procSupplier) {
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

    public void addEdges(Transform transform, Vertex toVertex, BiConsumer<Edge, Integer> configureEdgeFn) {
        int destOrdinal = 0;
        for (Transform fromTransform : transform.upstream()) {
            PlannerVertex fromPv = xform2vertex.get(fromTransform);
            Edge edge = from(fromPv.v, fromPv.nextAvailableOrdinal()).to(toVertex, destOrdinal);
            dag.edge(edge);
            configureEdgeFn.accept(edge, destOrdinal);
            destOrdinal++;
        }
    }

    public void addEdges(Transform transform, Vertex toVertex, Consumer<Edge> configureEdgeFn) {
        addEdges(transform, toVertex, (e, ord) -> configureEdgeFn.accept(e));
    }

    public void addEdges(Transform transform, Vertex toVertex) {
        addEdges(transform, toVertex, e -> { });
    }

    /**
     * Makes the proposed name unique in the DAG by adding an optional "-N"
     * between the name and the suffix.
     *
     * @return unique name to be used for the vertex
     */
    @Nonnull
    public String uniqueVertexName(@Nonnull String proposedName) {
        return uniqueName(vertexNames, proposedName);
    }

    /**
     * Returns a unique name for a proposed name given a set of previously known names
     * by adding an optional "-N" between the name and the suffix.
     *
     * @return unique name to be used
     */
    @Nonnull
    static String uniqueName(
            @Nonnull Set<String> knownNames, @Nonnull String proposedName
    ) {
        for (int index = 1; ; index++) {
            String candidate = proposedName
                    + (index == 1 ? "" : "-" + index);
            if (knownNames.add(candidate)) {
                return candidate;
            }
        }
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
