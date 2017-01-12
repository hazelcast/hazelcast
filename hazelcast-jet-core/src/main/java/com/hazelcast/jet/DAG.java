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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.SerializationConstants;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

/**
 * Describes a computation to be performed by the Jet computation engine.
 * A {@link Vertex vertex} represents a unit of data processing and an
 * {@link Edge edge} represents the path along which the data travels to
 * the next vertex.
 * <p>
 * The work of a single vertex is parallelized and distributed, so that on
 * each member there are several instances of the {@link Processor} type
 * corresponding to it. Whenever possible, each instance should be
 * tasked with only a slice of the total data and a partitioning strategy
 * can be employed to ensure that the data sent to each vertex is collated
 * by a partitioning key.
 * <p>
 * There are three basic kind of vertex:
 * <ol><li>
 *     <em>source</em> with just outbound edges;
 * </li><li>
 *     <em>processor</em> with both inbound and outbound edges;
 * </li><li>
 *     <em>sink</em> with just inbound edges.
 * </li></ol>
 * Data travels from sources to sinks and is transformed and reshaped
 * as it passes through the processors.
 */
public class DAG implements IdentifiedDataSerializable, Iterable<Vertex> {

    private Set<Edge> edges = new HashSet<>();
    private Map<String, Vertex> vertices = new HashMap<>();

    private Deque<Vertex> topologicalVertexStack = new ArrayDeque<>();

    /**
     * Adds a vertex to the DAG. The vertex name must be unique.
     */
    public DAG vertex(Vertex vertex) {
        if (vertices.containsKey(vertex.getName())) {
            throw new IllegalArgumentException("Vertex " + vertex.getName() + " is already defined.");
        }

        vertices.put(vertex.getName(), vertex);
        return this;
    }

    /**
     * Adds an edge to the DAG. The vertices it connects must already be present
     * in the DAG. It is an error to add an edge that connects the same two
     * vertices as another existing edge. It is an error to connect an edge to
     * a vertex at the same ordinal as another existing edge. However, inbound
     * and outbound ordinals are independent, so there can be two edges at the
     * same ordinal, one inbound and one outbound.
     */
    public DAG edge(Edge edge) {
        if (!containsVertex(edge.getSource())) {
            throw new IllegalArgumentException("Source vertex " + edge.getSource() + " doesn't exist!");
        }
        if (!containsVertex(edge.getDestination())) {
            throw new IllegalArgumentException("Destination vertex " + edge.getDestination() + " doesn't exist!");
        }
        if (edges.contains(edge)) {
            throw new IllegalArgumentException("Edge " + edge + " already defined!");
        }
        if (getInboundEdges(edge.getDestination())
                .stream().anyMatch(e -> e.getDestOrdinal() == edge.getDestOrdinal())) {
            throw new IllegalArgumentException("Another edge with same destination ordinal "
                    + edge.getDestOrdinal() + " exists");
        }
        if (getOutboundEdges(edge.getSource())
                .stream().anyMatch(e -> e.getSourceOrdinal() == edge.getSourceOrdinal())) {
            throw new IllegalArgumentException("Another edge with same source ordinal "
                    + edge.getSourceOrdinal() + " exists");
        }
        edges.add(edge);
        return this;
    }

    /**
     * Returns the inbound edges connected to the vertex with the given name.
     */
    public List<Edge> getInboundEdges(String vertexName) {
        if (!containsVertex(vertexName)) {
            throw new IllegalArgumentException("No vertex with name '" + vertexName + "' found in this DAG");
        }

        List<Edge> inboundEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getDestination().equals(vertexName)) {
                inboundEdges.add(edge);
            }
        }
        return inboundEdges;
    }

    /**
     * Returns the outbound edges connected to the vertex with the given name.
     */
    public List<Edge> getOutboundEdges(String vertexName) {
        if (!containsVertex(vertexName)) {
            throw new IllegalArgumentException("No vertex with name '" + vertexName + "' found in this DAG");
        }

        List<Edge> outboundEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getSource().equals(vertexName)) {
                outboundEdges.add(edge);
            }
        }
        return outboundEdges;
    }

    /**
     * Returns the vertex with the given name.
     */
    public Vertex getVertex(String vertexName) {
        return vertices.get(vertexName);
    }

    /**
     * Returns an iterator over the DAG's vertices in topological order.
     */
    public Iterator<Vertex> reverseIterator() {
        validate();
        return Collections.unmodifiableCollection(topologicalVertexStack).iterator();
    }

    /**
     * Returns an iterator over the DAG's vertices in reverse topological order.
     */
    @Override
    public Iterator<Vertex> iterator() {
        validate();
        List<Vertex> vertices = new ArrayList<>(topologicalVertexStack);
        Collections.reverse(vertices);
        return Collections.unmodifiableCollection(vertices).iterator();
    }

    void validate() throws IllegalArgumentException {
        topologicalVertexStack.clear();
        checkTrue(!vertices.isEmpty(), "DAG must contain at least one vertex");
        Map<String, List<Edge>> outgoingEdgeMap = edges.stream().collect(groupingBy(Edge::getSource));
        validateAgainstMultigraph(edges);
        validateOutboundEdgeOrdinals(outgoingEdgeMap);
        validateInboundEdgeOrdinals(edges.stream().collect(groupingBy(Edge::getDestination)));
        detectCycles(outgoingEdgeMap,
                vertices.entrySet().stream()
                        .collect(toMap(Entry::getKey, v -> new AnnotatedVertex(v.getValue()))));
    }

    @Override
    public String toString() {
        return "Vertices " + vertices + "\nEdges " + edges;
    }

    private boolean containsVertex(String vertexName) {
        return vertices.containsKey(vertexName);
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
                if (edgeMap.containsKey(pop.v.getName())) {
                    for (Edge edge : edgeMap.get(pop.v.getName())) {
                        if (edge.getDestination().equals(pop.v.getName())) {
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
        return SerializationConstants.FACTORY_ID;
    }

    @Override
    public int getId() {
        return SerializationConstants.DAG;
    }

    private static void validateAgainstMultigraph(Collection<Edge> edges) {
        final Set<SimpleImmutableEntry<String, String>> distinctSrcDest = new HashSet<>();
        for (Edge e : edges) {
            final SimpleImmutableEntry<String, String> srcDestId =
                    new SimpleImmutableEntry<>(e.getSource(), e.getDestination());
            if (!distinctSrcDest.add(srcDestId)) {
                throw new IllegalArgumentException(
                        String.format("Duplicate edge: %s -> %s", srcDestId.getKey(), srcDestId.getValue()));
            }
        }
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
