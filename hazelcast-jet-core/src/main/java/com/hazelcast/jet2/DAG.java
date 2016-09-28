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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkTrue;

public class DAG implements IdentifiedDataSerializable, Iterable<Vertex> {

    private Set<Edge> edges = new HashSet<>();
    private Map<String, Vertex> vertices = new HashMap<>();

    private Deque<Vertex> topologicalVertexStack = new ArrayDeque<>();

    public DAG addVertex(Vertex vertex) {
        if (vertices.containsKey(vertex.getName())) {
            throw new IllegalArgumentException("Vertex " + vertex.getName() + " is already defined.");
        }

        vertices.put(vertex.getName(), vertex);
        return this;
    }

    public DAG addEdge(Edge edge) {
        if (!vertices.containsValue(edge.getInputVertex())) {
            throw new IllegalArgumentException(
                    "Input vertex " + edge.getInputVertex() + " doesn't exist!");
        }

        if (!vertices.containsValue(edge.getOutputVertex())) {
            throw new IllegalArgumentException(
                    "Output vertex " + edge.getOutputVertex() + " doesn't exist!");
        }

        if (edges.contains(edge)) {
            throw new IllegalArgumentException(
                    "Edge " + edge + " already defined!");
        }
        edges.add(edge);
        return this;
    }

    /**
     * Returns the input edges for a given vertex
     */
    public List<Edge> getInputEdges(Vertex vertex) {
        if (!vertex.equals(getVertex(vertex.getName()))) {
            throw new IllegalArgumentException("Given vertex " + vertex + " could not be found in this DAG");
        }

        List<Edge> inputEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getOutputVertex().equals(vertex)) {
                inputEdges.add(edge);
            }
        }
        return inputEdges;
    }

    /**
     * Returns the output edges for a given vertex
     */
    public List<Edge> getOutputEdges(Vertex vertex) {
        if (!vertex.equals(getVertex(vertex.getName()))) {
            throw new IllegalArgumentException("Given vertex " + vertex + " could not be found in this DAG");
        }

        List<Edge> inputEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getInputVertex().equals(vertex)) {
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
        verify();
        return Collections.unmodifiableCollection(topologicalVertexStack).iterator();
    }

    /**
     * Returns iterator for vertices in reverse topological order
     */
    public Iterator<Vertex> iterator() {
        verify();
        List<Vertex> vertices = new ArrayList<>(topologicalVertexStack);
        Collections.reverse(vertices);
        return Collections.unmodifiableCollection(vertices).iterator();
    }

    private void verify() throws IllegalStateException {
        checkTrue(!vertices.isEmpty(), "DAG must contain at least one vertex");

        // check for valid vertices, duplicate vertex names,
        // and prepare for cycle detection
        Map<String, AnnotatedVertex> vertexMap = new HashMap<>();
        Map<Vertex, Set<String>> inboundVertexMap = new HashMap<>();
        Map<Vertex, Set<String>> outboundVertexMap = new HashMap<>();

        for (Map.Entry<String, Vertex> v : vertices.entrySet()) {
            if (vertexMap.containsKey(v.getKey())) {
                throw new IllegalStateException("DAG contains multiple vertices with name: " + v.getKey());
            }
            vertexMap.put(v.getKey(), new AnnotatedVertex(v.getValue()));
        }

        Map<Vertex, List<Edge>> edgeMap = new HashMap<>();
        checkEdges(inboundVertexMap, outboundVertexMap, edgeMap);

        // check input and output names don't collide with vertex names
        detectCycles(edgeMap, vertexMap);
    }


    private void checkEdges(Map<Vertex, Set<String>> inboundVertexMap,
                            Map<Vertex, Set<String>> outboundVertexMap,
                            Map<Vertex, List<Edge>> edgeMap) {
        for (Edge e : edges) {
            // Construct structure for cycle detection
            Vertex inputVertex = e.getInputVertex();
            Vertex outputVertex = e.getOutputVertex();
            List<Edge> edgeList = edgeMap.get(inputVertex);
            if (edgeList == null) {
                edgeList = new ArrayList<>();
                edgeMap.put(inputVertex, edgeList);
            }
            edgeList.add(e);

            // Construct map for Input name verification
            Set<String> inboundSet = inboundVertexMap.get(outputVertex);
            if (inboundSet == null) {
                inboundSet = new HashSet<>();
                inboundVertexMap.put(outputVertex, inboundSet);
            }
            inboundSet.add(inputVertex.getName());

            // Construct map for Output name verification
            Set<String> outboundSet = outboundVertexMap.get(inputVertex);
            if (outboundSet == null) {
                outboundSet = new HashSet<>();
                outboundVertexMap.put(inputVertex, outboundSet);
            }

            outboundSet.add(outputVertex.getName());
        }
    }

    // Adaptation of Tarjan's algorithm for connected components.
    // http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    private void detectCycles(Map<Vertex, List<Edge>> edgeMap, Map<String, AnnotatedVertex> vertexMap)
            throws IllegalStateException {
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
            Map<Vertex, List<Edge>> edgeMap,
            Deque<AnnotatedVertex> stack, Integer nextIndex) throws IllegalStateException {
        av.index = nextIndex;
        av.lowlink = nextIndex;
        nextIndex++;
        stack.addLast(av);
        av.onstack = true;

        List<Edge> edges = edgeMap.get(av.v);

        if (edges != null) {
            for (Edge e : edgeMap.get(av.v)) {
                AnnotatedVertex outVertex = vertexMap.get(e.getOutputVertex().getName());

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
                throw new IllegalStateException("DAG contains a cycle: " + message);
            } else {
                // detect self-cycle
                if (edgeMap.containsKey(pop.v)) {
                    for (Edge edge : edgeMap.get(pop.v)) {
                        if (edge.getOutputVertex().equals(pop.v)) {
                            throw new IllegalStateException("DAG contains a self-cycle on vertex:" + pop.v.getName());
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
