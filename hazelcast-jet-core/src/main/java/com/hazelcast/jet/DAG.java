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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.dag.TopologicalOrderIterator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Direct acyclic graph representation
 * <p>
 * DAG describes topology of calculation flow
 * <p>
 * <pre>
 *
 *     Vertex1 -&gt; Vertex2  -&gt; Vertex3
 *                            -&gt; Vertex4
 * </pre>
 * <p>
 * Data will be passed from vertex to vertex
 */
public class DAG implements IdentifiedDataSerializable {
    private String name;
    private Set<Edge> edges = new HashSet<Edge>();
    private Map<String, Vertex> vertices = new HashMap<String, Vertex>();

    private transient volatile boolean validated;
    private transient Stack<Vertex> topologicalVertexStack = new Stack<Vertex>();

    /**
     * Empty constructor
     */
    public DAG() {

    }

    /**
     * Create a DAG with a given name
     *
     * @param name name of the DAG
     */
    public DAG(String name) {
        this.name = name;
    }

    /**
     * @param vertex vertex to add to the DAG
     * @return the DAG
     */
    public DAG addVertex(Vertex vertex) {
        if (vertices.containsKey(vertex.getName())) {
            throw new IllegalArgumentException("Vertex " + vertex.getName() + " already defined!");
        }

        vertices.put(vertex.getName(), vertex);
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
     * Return vertex with corresponding name
     *
     * @param vertexName name of the vertex
     * @return corresponding vertex
     */
    public Vertex getVertex(String vertexName) {
        return vertices.get(vertexName);
    }

    /**
     * @return collections of DAG's vertices
     */
    public Collection<Vertex> getVertices() {
        return this.vertices.values();
    }

    /**
     * @return name of the DAG
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return iterator over DAG's vertices on accordance with DAG's topology
     */
    public Iterator<Vertex> getTopologicalVertexIterator() {
        if (!validated) {
            throw new IllegalStateException("Graph should be validated before");
        }

        Stack<Vertex> stack = new Stack<Vertex>();
        stack.addAll(this.topologicalVertexStack);
        return new TopologicalOrderIterator(stack);
    }

    /**
     * @return iterator over DAG's vertices on accordance with DAG's reverted topology
     */
    public Iterator<Vertex> getRevertedTopologicalVertexIterator() {
        if (!validated) {
            throw new IllegalStateException("Graph should be validated before");
        }

        Stack<Vertex> stack = new Stack<Vertex>();
        stack.addAll(topologicalVertexStack);

        Stack<Vertex> reversedStack = new Stack<Vertex>();
        while (!stack.empty()) {
            reversedStack.push(stack.pop());
        }

        return new TopologicalOrderIterator(reversedStack);
    }

    /**
     * Validate DAG's consistency
     * <p>
     * It checks:
     * <p>
     * <pre>
     *        duplicate of vertices names
     *        duplicate of edges names
     *        absence of loops on DAG
     * </pre>
     *
     * @throws IllegalStateException if DAG validation fails
     */
    public void validate() throws IllegalStateException {
        if (vertices.isEmpty()) {
            throw new IllegalStateException("Invalid dag containing 0 vertices");
        }

        // check for valid vertices, duplicate vertex names,
        // and prepare for cycle detection
        Map<String, AnnotatedVertex> vertexMap = new HashMap<String, AnnotatedVertex>();
        Map<Vertex, Set<String>> inboundVertexMap = new HashMap<Vertex, Set<String>>();
        Map<Vertex, Set<String>> outboundVertexMap = new HashMap<Vertex, Set<String>>();

        for (Map.Entry<String, Vertex> v : vertices.entrySet()) {
            if (vertexMap.containsKey(v.getKey())) {
                throw new IllegalStateException("DAG contains multiple vertices" + " with name: " + v.getKey());
            }

            vertexMap.put(v.getKey(), new AnnotatedVertex(v.getValue()));
        }

        Map<Vertex, List<Edge>> edgeMap = new HashMap<Vertex, List<Edge>>();
        checkEdges(inboundVertexMap, outboundVertexMap, edgeMap);

        // check input and output names don't collide with vertex names
        checkVertices(vertexMap);
        checkVerticesNames(inboundVertexMap, outboundVertexMap);
        detectCycles(edgeMap, vertexMap);
        this.validated = true;
    }

    private void checkVerticesNames(Map<Vertex, Set<String>> inboundVertexMap, Map<Vertex, Set<String>> outboundVertexMap) {
        for (Map.Entry<Vertex, Set<String>> entry : inboundVertexMap.entrySet()) {
            Vertex vertex = entry.getKey();
            for (Source source : vertex.getSources()) {
                if (entry.getValue().contains(source.getName())) {
                    throw new IllegalStateException("Vertex: "
                            + vertex.getName()
                            + " contains an incoming vertex and Input with the same name: "
                            + source);
                }
            }
        }

        // Check for valid OutputNames
        for (Map.Entry<Vertex, Set<String>> entry : outboundVertexMap.entrySet()) {
            Vertex vertex = entry.getKey();
            for (Sink sink : vertex.getSinks()) {
                if (entry.getValue().contains(sink.getName())) {
                    throw new IllegalStateException(
                            "Vertex: "
                                    + vertex.getName()
                                    + " contains an outgoing vertex and Output with the same name: "
                                    + sink
                    );
                }
            }
        }
    }

    private void checkVertices(Map<String, AnnotatedVertex> vertexMap) {
        for (Vertex vertex : vertices.values()) {
            for (Source source : vertex.getSources()) {
                if (vertexMap.containsKey(source.getName())) {
                    throw new IllegalStateException("Vertex: "
                            + vertex.getName()
                            + " contains a source with the same name as vertex: "
                            + source
                    );
                }
            }
            for (Sink sink : vertex.getSinks()) {
                if (vertexMap.containsKey(sink.getName())) {
                    throw new IllegalStateException("Vertex: "
                            + vertex.getName()
                            + " contains a sink with the same name as vertex: "
                            + sink
                    );
                }
            }
        }
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
                edgeList = new ArrayList<Edge>();
                edgeMap.put(inputVertex, edgeList);
            }
            edgeList.add(e);

            // Construct map for Input name verification
            Set<String> inboundSet = inboundVertexMap.get(outputVertex);
            if (inboundSet == null) {
                inboundSet = new HashSet<String>();
                inboundVertexMap.put(outputVertex, inboundSet);
            }
            inboundSet.add(inputVertex.getName());

            // Construct map for Output name verification
            Set<String> outboundSet = outboundVertexMap.get(inputVertex);
            if (outboundSet == null) {
                outboundSet = new HashSet<String>();
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
        Stack<AnnotatedVertex> stack = new Stack<AnnotatedVertex>();
        for (AnnotatedVertex av : vertexMap.values()) {
            if (av.index == -1) {
                assert stack.empty();
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
            Stack<AnnotatedVertex> stack, Integer nextIndex) throws IllegalStateException {
        av.index = nextIndex;
        av.lowlink = nextIndex;
        nextIndex++;
        stack.push(av);
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
            AnnotatedVertex pop = stack.pop();
            pop.onstack = false;
            if (pop != av) {
                // there was something on the stack other than this "av".
                // this indicates there is a scc/cycle. It comprises all nodes from top of stack to "av"
                StringBuilder message = new StringBuilder();
                message.append(av.v.getName()).append(" <- ");
                for (; pop != av; pop = stack.pop()) {
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

            topologicalVertexStack.push(av.v);
        }
    }

    /**
     * Add edge to dag
     *
     * @param edge corresponding edge
     * @return DAG itself
     */
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(this.name);

        out.writeInt(this.vertices.size());

        for (Map.Entry<String, Vertex> entry : this.vertices.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(this.edges.size());

        for (Edge edge : this.edges) {
            out.writeObject(edge);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.name = in.readUTF();

        int vertexCount = in.readInt();

        for (int i = 0; i < vertexCount; i++) {
            String key = in.readObject();
            Vertex value = in.readObject();
            this.vertices.put(key, value);
        }

        int edgeCount = in.readInt();

        for (int i = 0; i < edgeCount; i++) {
            Edge edge = in.readObject();
            this.edges.add(edge);
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
