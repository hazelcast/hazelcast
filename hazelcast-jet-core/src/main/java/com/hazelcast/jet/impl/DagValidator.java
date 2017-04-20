/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

/**
 * Validates a DAG against cycles and other malformations.
 */
public class DagValidator {
    private final Deque<Vertex> topologicalVertexStack = new ArrayDeque<>();

    public Collection<Vertex> validate(Map<String, Vertex> verticesByName, Set<Edge> edges) {
        topologicalVertexStack.clear();
        checkTrue(!verticesByName.isEmpty(), "DAG must contain at least one vertex");
        Map<String, List<Edge>> outgoingEdgeMap = edges.stream().collect(groupingBy(Edge::getSourceName));
        validateOutboundEdgeOrdinals(outgoingEdgeMap);
        validateInboundEdgeOrdinals(edges.stream().collect(groupingBy(Edge::getDestName)));
        detectCycles(outgoingEdgeMap,
                verticesByName.entrySet().stream()
                              .collect(toMap(Entry::getKey, v -> new AnnotatedVertex(v.getValue()))));
        return topologicalVertexStack;
    }

    private static void validateInboundEdgeOrdinals(Map<String, List<Edge>> incomingEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : incomingEdgeMap.entrySet()) {
            String vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getDestOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Input ordinals for vertex " + vertex + " are not ordered. "
                            + "Actual: " + Arrays.toString(ordinals) + " Expected: "
                            + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    private static void validateOutboundEdgeOrdinals(Map<String, List<Edge>> outgoingEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : outgoingEdgeMap.entrySet()) {
            String vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getSourceOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Output ordinals for vertex " + vertex + " are not ordered. "
                            + "Actual: " + Arrays.toString(ordinals) + " Expected: "
                            + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    // Adaptation of Tarjan's algorithm for connected components.
    // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    private void detectCycles(Map<String, List<Edge>> edgeMap, Map<String, AnnotatedVertex> vertexMap)
            throws IllegalArgumentException {
        // boxed integer so it is passed by reference.
        Integer nextIndex = 0;
        Deque<AnnotatedVertex> stack = new ArrayDeque<>();
        for (AnnotatedVertex av : vertexMap.values()) {
            if (av.index == -1) {
                assert stack.isEmpty();
                strongConnect(av, vertexMap, edgeMap, stack, nextIndex);
            }
        }
    }

    // part of Tarjan's algorithm for connected components.
    // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    private void strongConnect(
            AnnotatedVertex av,
            Map<String, AnnotatedVertex> vertexMap,
            Map<String, List<Edge>> edgeMap,
            Deque<AnnotatedVertex> stack, Integer nextIndex) throws IllegalArgumentException {
        av.index = nextIndex;
        av.lowlink = nextIndex;
        nextIndex++;
        stack.addLast(av);
        av.onstack = true;

        List<Edge> edges = edgeMap.get(av.v.getName());

        if (edges != null) {
            for (Edge e : edgeMap.get(av.v.getName())) {
                AnnotatedVertex outVertex = vertexMap.get(e.getDestName());

                if (outVertex.index == -1) {
                    strongConnect(outVertex, vertexMap, edgeMap, stack, nextIndex);
                    av.lowlink = Math.min(av.lowlink, outVertex.lowlink);
                } else if (outVertex.onstack) {
                    // strongly connected component detected, but we will wait till later so
                    // that the full cycle can be displayed. Update lowlink in case outputVertex
                    // should be considered the root of this component.
                    av.lowlink = Math.min(av.lowlink, outVertex.index);
                }
            }
        }

        if (av.lowlink == av.index) {
            AnnotatedVertex pop = stack.removeLast();
            pop.onstack = false;
            if (pop != av) {
                // there was something on the stack other than this "av".
                // this indicates there is a scc/cycle. It comprises all nodes from top of stack to "av"
                StringBuilder message = new StringBuilder();
                message.append(av.v.getName()).append(" <- ");
                for (; pop != av; pop = stack.removeLast()) {
                    message.append(pop.v.getName()).append(" <- ");
                    pop.onstack = false;
                }
                message.append(av.v.getName());
                throw new IllegalArgumentException("DAG contains a cycle: " + message);
            } else {
                // detect self-cycle
                if (edgeMap.containsKey(pop.v.getName())) {
                    for (Edge edge : edgeMap.get(pop.v.getName())) {
                        if (edge.getDestName().equals(pop.v.getName())) {
                            throw new IllegalArgumentException("DAG contains a self-cycle on vertex:" + pop.v.getName());
                        }
                    }
                }
            }
            topologicalVertexStack.addLast(av.v);
        }
    }

    private static final class AnnotatedVertex {
        Vertex v;
        int index;
        int lowlink;
        boolean onstack;

        private AnnotatedVertex(Vertex v) {
            this.v = v;
            index = -1;
            lowlink = -1;
        }
    }
}
