/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

/**
 * Javadoc pending
 */
public class DAG implements IdentifiedDataSerializable, Iterable<Vertex> {

    private Set<Edge> edges = new HashSet<>();
    private Map<String, Vertex> vertices = new HashMap<>();

    private Deque<Vertex> topologicalVertexStack = new ArrayDeque<>();

    /**
     * Adds the given vertex
     */
    public DAG addVertex(Vertex vertex) {
        if (vertices.containsKey(vertex.getName())) {
            throw new IllegalArgumentException("Vertex " + vertex.getName() + " is already defined.");
        }

        vertices.put(vertex.getName(), vertex);
        return this;
    }

    /**
     * Adds the given edge
     */
    public DAG addEdge(Edge edge) {
        if (!vertices.containsValue(edge.getSource())) {
            throw new IllegalArgumentException(
                    "Input vertex " + edge.getSource() + " doesn't exist!");
        }

        if (!vertices.containsValue(edge.getDestination())) {
            throw new IllegalArgumentException(
                    "Output vertex " + edge.getDestination() + " doesn't exist!");
        }

        if (edges.contains(edge)) {
            throw new IllegalArgumentException(
                    "Edge " + edge + " already defined!");
        }

        if (getInboundEdges(edge.getDestination())
                .stream().anyMatch(e -> e.getInputOrdinal() == edge.getInputOrdinal())) {
            throw new IllegalArgumentException("Another edge with same destination ordinal "
                    + edge.getInputOrdinal() + " exists");
        }

        if (getOutboundEdges(edge.getSource())
                .stream().anyMatch(e -> e.getOutputOrdinal() == edge.getOutputOrdinal())) {
            throw new IllegalArgumentException("Another edge with same source ordinal "
                    + edge.getOutputOrdinal() + " exists");
        }


        edges.add(edge);
        return this;
    }

    /**
     * Returns the input edges for a given vertex
     */
    public List<Edge> getInboundEdges(Vertex vertex) {
        if (!vertex.equals(getVertex(vertex.getName()))) {
            throw new IllegalArgumentException("Given vertex " + vertex + " could not be found in this DAG");
        }

        List<Edge> inputEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getDestination().equals(vertex)) {
                inputEdges.add(edge);
            }
        }
        return inputEdges;
    }

    /**
     * Returns the output edges for a given vertex
     */
    public List<Edge> getOutboundEdges(Vertex vertex) {
        if (!vertex.equals(getVertex(vertex.getName()))) {
            throw new IllegalArgumentException("Given vertex " + vertex + " could not be found in this DAG");
        }

        List<Edge> inputEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getSource().equals(vertex)) {
                inputEdges.add(edge);
            }
        }
        return inputEdges;
    }

    /**
     * Returns the vertex with the given name
     */
    public Vertex getVertex(String vertexName) {
        return vertices.get(vertexName);
    }

    /**
     * Returns iterator for vertices in topological order
     */
    public Iterator<Vertex> reverseIterator() {
        validate();
        return Collections.unmodifiableCollection(topologicalVertexStack).iterator();
    }

    /**
     * Returns iterator for vertices in reverse topological order
     */
    public Iterator<Vertex> iterator() {
        validate();
        List<Vertex> vertices = new ArrayList<>(topologicalVertexStack);
        Collections.reverse(vertices);
        return Collections.unmodifiableCollection(vertices).iterator();
    }

    void validate() throws IllegalArgumentException {
        topologicalVertexStack.clear();
        checkTrue(!vertices.isEmpty(), "DAG must contain at least one vertex");
        Map<Vertex, List<Edge>> outgoingEdgeMap = edges.stream().collect(groupingBy(Edge::getSource));
        validateAgainstMultigraph(edges);
        validateOutboundEdgeOrdinals(outgoingEdgeMap);
        validateInboundEdgeOrdinals(edges.stream().collect(groupingBy(Edge::getDestination)));
        detectCycles(outgoingEdgeMap,
                vertices.entrySet().stream()
                        .collect(toMap(Map.Entry::getValue, v -> new AnnotatedVertex(v.getValue()))));
    }

    private static void validateAgainstMultigraph(Collection<Edge> edges) {
        final Set<SimpleImmutableEntry<String, String>> distinctSrcDest = new HashSet<>();
        for (Edge e : edges) {
            final SimpleImmutableEntry<String, String> srcDestId =
                    new SimpleImmutableEntry<>(e.getSource().getName(), e.getDestination().getName());
            if (!distinctSrcDest.add(srcDestId)) {
                throw new IllegalArgumentException(
                        String.format("Duplicate edge: %s -> %s", srcDestId.getKey(), srcDestId.getValue()));
            }
        }
    }

    private static void validateInboundEdgeOrdinals(Map<Vertex, List<Edge>> incomingEdgeMap) {
        for (Map.Entry<Vertex, List<Edge>> entry : incomingEdgeMap.entrySet()) {
            Vertex vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getInputOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Input ordinals for vertex " + vertex + " are not ordered. "
                            + "Actual: " + Arrays.toString(ordinals) + " Expected: "
                            + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    private static void validateOutboundEdgeOrdinals(Map<Vertex, List<Edge>> outgoingEdgeMap) {
        for (Map.Entry<Vertex, List<Edge>> entry : outgoingEdgeMap.entrySet()) {
            Vertex vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getOutputOrdinal).sorted().toArray();
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
    private void detectCycles(Map<Vertex, List<Edge>> edgeMap, Map<Vertex, AnnotatedVertex> vertexMap)
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
            Map<Vertex, AnnotatedVertex> vertexMap,
            Map<Vertex, List<Edge>> edgeMap,
            Deque<AnnotatedVertex> stack, Integer nextIndex) throws IllegalArgumentException {
        av.index = nextIndex;
        av.lowlink = nextIndex;
        nextIndex++;
        stack.addLast(av);
        av.onstack = true;

        List<Edge> edges = edgeMap.get(av.v);

        if (edges != null) {
            for (Edge e : edgeMap.get(av.v)) {
                AnnotatedVertex outVertex = vertexMap.get(e.getDestination());

                if (outVertex.index == -1) {
                    strongConnect(outVertex, vertexMap, edgeMap, stack, nextIndex);
                    av.lowlink = Math.min(av.lowlink, outVertex.lowlink);
                } else if (outVertex.onstack) {
                    // strongly connected component detected, but we will wait till later so that the full cycle can be displayed.
                    // update lowlink in case outputVertex should be considered the root of this component.
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
                if (edgeMap.containsKey(pop.v)) {
                    for (Edge edge : edgeMap.get(pop.v)) {
                        if (edge.getDestination().equals(pop.v)) {
                            throw new IllegalArgumentException("DAG contains a self-cycle on vertex:" + pop.v.getName());
                        }
                    }
                }
            }
            topologicalVertexStack.addLast(av.v);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(vertices.size());

        for (Map.Entry<String, Vertex> entry : vertices.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(edges.size());

        for (Edge edge : edges) {
            out.writeObject(edge);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int vertexCount = in.readInt();

        for (int i = 0; i < vertexCount; i++) {
            String key = in.readObject();
            Vertex value = in.readObject();
            vertices.put(key, value);
        }

        int edgeCount = in.readInt();

        for (int i = 0; i < edgeCount; i++) {
            Edge edge = in.readObject();
            edges.add(edge);
        }
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetDataSerializerHook.DAG;
    }

    private static final class AnnotatedVertex {
        Vertex v;

        //for Tarjan's algorithm
        int index;
        //for Tarjan's algorithm
        int lowlink;
        //for Tarjan's algorithm
        boolean onstack;

        private AnnotatedVertex(Vertex v) {
            this.v = v;
            index = -1;
            lowlink = -1;
        }

    }

}
