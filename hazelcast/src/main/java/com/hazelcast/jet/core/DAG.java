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

package com.hazelcast.jet.core;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.config.EdgeConfig.DEFAULT_QUEUE_SIZE;
import static com.hazelcast.jet.core.Edge.DISTRIBUTE_TO_ALL;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static com.hazelcast.jet.impl.pipeline.transform.AggregateTransform.FIRST_STAGE_VERTEX_NAME_SUFFIX;
import static com.hazelcast.jet.impl.util.Util.escapeGraphviz;
import static java.util.Collections.emptyList;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;

/**
 * Describes a computation to be performed by the Jet computation engine.
 * A {@link Vertex vertex} represents a unit of data processing and an
 * {@link Edge edge} represents the path along which the data travels to
 * the next vertex.
 * <p>
 * The work of a single vertex is parallelized and distributed, so that there are
 * several instances of the {@link Processor} type on each member
 * corresponding to it. Whenever possible, each instance should be
 * tasked with only a slice of the total data and a partitioning strategy
 * can be employed to ensure that the data sent to each vertex is collated
 * by a partitioning key.
 * <p>
 * There are three basic kinds of vertices:
 * <ol><li>
 *     <em>source</em> with just outbound edges;
 * </li><li>
 *     <em>processor</em> with both inbound and outbound edges;
 * </li><li>
 *     <em>sink</em> with just inbound edges.
 * </li></ol>
 * Data travels from sources to sinks and is transformed and reshaped
 * as it passes through the processors.
 *
 * @since Jet 3.0
 */
public class DAG implements IdentifiedDataSerializable, Iterable<Vertex> {
    //Note: This lock prevents only some changes to the DAG. It cannot prevent changing user-supplied
    // objects like processor suppliers or various lambdas.
    private transient boolean locked;

    private final Set<Edge> edges = new LinkedHashSet<>();
    private final Map<String, Vertex> nameToVertex = new HashMap<>();
    // Transient field:
    private final Set<Vertex> verticesByIdentity = newSetFromMap(new IdentityHashMap<>());

    /**
     * Creates a vertex from a {@code Supplier<Processor>} and adds it to this DAG.
     *
     * @see Vertex#Vertex(String, SupplierEx)
     *
     * @param name the unique name of the vertex
     * @param simpleSupplier the simple, parameterless supplier of {@code Processor} instances
     */
    @Nonnull
    public Vertex newVertex(
            @Nonnull String name, @Nonnull SupplierEx<? extends Processor> simpleSupplier
    ) {
        throwIfLocked();
        return addVertex(new Vertex(name, simpleSupplier));
    }

    /**
     * Creates a vertex from a {@code Supplier<Processor>} and adds it to this
     * DAG. The vertex will be given a unique name created from the {@code
     * namePrefix}.
     *
     * @see Vertex#Vertex(String, SupplierEx)
     *
     * @param namePrefix the prefix for unique name of the vertex
     * @param simpleSupplier the simple, parameterless supplier of {@code Processor} instances
     * @since Jet 4.4
     */
    @Nonnull
    public Vertex newUniqueVertex(
            @Nonnull String namePrefix, @Nonnull SupplierEx<? extends Processor> simpleSupplier
    ) {
        throwIfLocked();
        return addVertex(new Vertex(uniqueName(namePrefix), simpleSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorSupplier} and adds it to this DAG.
     *
     * @see Vertex#Vertex(String, ProcessorSupplier)
     *
     * @param name the unique name of the vertex
     * @param processorSupplier the supplier of {@code Processor} instances which will be used on all members
     */
    @Nonnull
    public Vertex newVertex(@Nonnull String name, @Nonnull ProcessorSupplier processorSupplier) {
        throwIfLocked();
        return addVertex(new Vertex(name, processorSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorSupplier} and adds it to this
     * DAG. The vertex will be given a unique name created from the {@code
     * namePrefix}.
     *
     * @see Vertex#Vertex(String, ProcessorSupplier)
     *
     * @param namePrefix the prefix for unique name of the vertex
     * @param processorSupplier the supplier of {@code Processor} instances which will be used on all members
     * @since Jet 4.4
     */
    @Nonnull
    public Vertex newUniqueVertex(@Nonnull String namePrefix, @Nonnull ProcessorSupplier processorSupplier) {
        throwIfLocked();
        return addVertex(new Vertex(uniqueName(namePrefix), processorSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorMetaSupplier} and adds it to this DAG.
     *
     * @see Vertex#Vertex(String, ProcessorMetaSupplier)
     *
     * @param name the unique name of the vertex
     * @param metaSupplier the meta-supplier of {@code ProcessorSupplier}s for each member
     *
     */
    @Nonnull
    public Vertex newVertex(@Nonnull String name, @Nonnull ProcessorMetaSupplier metaSupplier) {
        throwIfLocked();
        return addVertex(new Vertex(name, metaSupplier));
    }

    /**
     * Creates a vertex from a {@code ProcessorMetaSupplier} and adds it to
     * this DAG. The vertex will be given a unique name created from the {@code
     * namePrefix}.
     *
     * @see Vertex#Vertex(String, ProcessorMetaSupplier)
     *
     * @param namePrefix the prefix for unique name of the vertex
     * @param metaSupplier the meta-supplier of {@code ProcessorSupplier}s for each member
     * @since Jet 4.4
     */
    @Nonnull
    public Vertex newUniqueVertex(@Nonnull String namePrefix, @Nonnull ProcessorMetaSupplier metaSupplier) {
        throwIfLocked();
        return addVertex(new Vertex(uniqueName(namePrefix), metaSupplier));
    }

    /**
     * Adds a vertex to this DAG. The vertex name must be unique.
     */
    @Nonnull
    public DAG vertex(@Nonnull Vertex vertex) {
        throwIfLocked();
        addVertex(vertex);
        return this;
    }

    /**
     * Adds an edge to this DAG. The vertices it connects must already be
     * present in the DAG. It is an error to connect an edge to a vertex at the
     * same ordinal as another existing edge. However, inbound and outbound
     * ordinals are independent, so there can be two edges at the same ordinal,
     * one inbound and one outbound.
     * <p>
     * Jet supports multigraphs, that is you can add two edges between the same
     * two vertices. However, they have to have different ordinals.
     */
    @Nonnull
    public DAG edge(@Nonnull Edge edge) {
        throwIfLocked();
        if (edge.getDestination() == null) {
            throw new IllegalArgumentException("Edge has no destination");
        }
        assert edge.getDestName() != null;
        if (!containsVertex(edge.getSource())) {
            throw new IllegalArgumentException(
                    containsVertexName(edge.getSource())
                            ? "This DAG has a vertex called '" + edge.getSourceName()
                            + "', but the supplied edge's source is a different vertex with the same name"
                            : "Source vertex '" + edge.getSourceName() + "' is not in this DAG"
            );
        }
        if (!containsVertex(edge.getDestination())) {
            throw new IllegalArgumentException(
                    containsVertexName(edge.getDestination())
                            ? "This DAG has a vertex called '" + edge.getDestName()
                            + "', but the supplied edge's destination is a different vertex with the same name"
                            : "Destination vertex '" + edge.getDestName() + "' is not in this DAG");
        }
        if (getInboundEdges(edge.getDestName())
                .stream().anyMatch(e -> e.getDestOrdinal() == edge.getDestOrdinal())) {
            throw new IllegalArgumentException("Vertex '" + edge.getDestName()
                    + "' already has an inbound edge at ordinal " + edge.getDestOrdinal()
                    + (edge.getSourceOrdinal() == 0 && edge.getDestOrdinal() == 0
                    ? ", use Edge.from().to() to specify another ordinal" : ""));
        }
        if (getOutboundEdges(edge.getSourceName())
                .stream().anyMatch(e -> e.getSourceOrdinal() == edge.getSourceOrdinal())) {
            throw new IllegalArgumentException("Vertex '" + edge.getSourceName()
                    + "' already has an outbound edge at ordinal " + edge.getSourceOrdinal()
                    + (edge.getSourceOrdinal() == 0 && edge.getDestOrdinal() == 0
                    ? ", use Edge.from().to() to specify another ordinal" : ""));
        }
        if (edge.getSource() == edge.getDestination()) {
            throw new IllegalArgumentException("Attempted to add an edge from " + edge.getSourceName() + " to itself");
        }
        boolean success = edges.add(edge);
        assert success : "Duplicate edge added: " + edge;
        return this;
    }

    /**
     * Returns the inbound edges connected to the vertex with the given name.
     */
    @Nonnull
    public List<Edge> getInboundEdges(@Nonnull String vertexName) {
        if (!nameToVertex.containsKey(vertexName)) {
            throw new IllegalArgumentException("No vertex with name '" + vertexName + "' found in this DAG");
        }
        List<Edge> inboundEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getDestName().equals(vertexName)) {
                inboundEdges.add(edge);
            }
        }
        return inboundEdges;
    }

    /**
     * Returns the outbound edges connected to the vertex with the given name.
     */
    @Nonnull
    public List<Edge> getOutboundEdges(@Nonnull String vertexName) {
        if (!nameToVertex.containsKey(vertexName)) {
            throw new IllegalArgumentException("No vertex with name '" + vertexName + "' found in this DAG");
        }
        List<Edge> outboundEdges = new ArrayList<>();
        for (Edge edge : edges) {
            if (edge.getSourceName().equals(vertexName)) {
                outboundEdges.add(edge);
            }
        }
        return outboundEdges;
    }

    /**
     * Returns the vertex with the given name, or {@code null} if there is no
     * vertex with that name.
     */
    @Nullable
    public Vertex getVertex(@Nonnull String vertexName) {
        return nameToVertex.get(vertexName);
    }

    /**
     * Returns an iterator over the DAG's vertices in topological order.
     */
    @Nonnull @Override
    public Iterator<Vertex> iterator() {
        return validate().iterator();
    }

    /**
     * Creates a {@code Vertex} name that is unique in this DAG and starts with
     * the given prefix.
     */
    private String uniqueName(String namePrefix) {
        String name = namePrefix;
        for (int i = 2; nameToVertex.containsKey(name); i++) {
            name = namePrefix + '-' + i;
        }
        return name;
    }

    private Vertex addVertex(Vertex vertex) {
        if (nameToVertex.containsKey(vertex.getName())) {
            throw new IllegalArgumentException("Vertex " + vertex.getName() + " is already defined.");
        }
        verticesByIdentity.add(vertex);
        nameToVertex.put(vertex.getName(), vertex);
        return vertex;
    }

    private boolean containsVertex(Vertex vertex) {
        return verticesByIdentity.contains(vertex);
    }

    private boolean containsVertexName(Vertex vertex) {
        return nameToVertex.containsKey(vertex.getName());
    }

    /**
     * @return iterable over vertices in topological order
     */
    // exposed for testing
    Iterable<Vertex> validate() {
        checkTrue(!nameToVertex.isEmpty(), "DAG must contain at least one vertex");
        Map<String, List<Edge>> inboundEdgeMap = edges.stream().collect(groupingBy(Edge::getDestName));
        Map<String, List<Edge>> outboundEdgeMap = edges.stream().collect(groupingBy(Edge::getSourceName));
        validateInboundEdgeOrdinals(inboundEdgeMap);
        validateOutboundEdgeOrdinals(outboundEdgeMap);

        Map<Vertex, List<Vertex>> adjacencyMap = new HashMap<>();
        for (Edge edge : edges) {
            adjacencyMap.computeIfAbsent(edge.getSource(), x -> new ArrayList<>())
                        .add(edge.getDestination());
        }
        for (Vertex v : nameToVertex.values()) {
            adjacencyMap.putIfAbsent(v, emptyList());
        }
        return topologicalSort(adjacencyMap, Vertex::getName);
    }

    private static void validateInboundEdgeOrdinals(Map<String, List<Edge>> inboundEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : inboundEdgeMap.entrySet()) {
            String vertex = entry.getKey();
            int[] ordinals = entry.getValue().stream().mapToInt(Edge::getDestOrdinal).sorted().toArray();
            for (int i = 0; i < ordinals.length; i++) {
                if (ordinals[i] != i) {
                    throw new IllegalArgumentException("Input ordinals for vertex " + vertex
                            + " are not properly numbered. Actual: " + Arrays.toString(ordinals)
                            + " Expected: " + Arrays.toString(IntStream.range(0, ordinals.length).toArray()));
                }
            }
        }
    }

    private static void validateOutboundEdgeOrdinals(Map<String, List<Edge>> outboundEdgeMap) {
        for (Map.Entry<String, List<Edge>> entry : outboundEdgeMap.entrySet()) {
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

    @Override
    @Nonnull
    public String toString() {
        return toString(LOCAL_PARALLELISM_USE_DEFAULT);
    }

    /**
     * Returns a string representation of the DAG.
     *
     * @param defaultLocalParallelism the local parallelism that will be shown if
     *                                neither overridden on the vertex nor the
     *                                preferred parallelism is defined by
     *                                meta-supplier
     */
    @Nonnull
    public String toString(int defaultLocalParallelism) {
        final StringBuilder b = new StringBuilder("dag\n");
        for (Vertex v : this) {
            b.append("    .vertex(\"").append(v.getName()).append("\")");
            int localParallelism = v.determineLocalParallelism(defaultLocalParallelism);
            if (localParallelism != LOCAL_PARALLELISM_USE_DEFAULT) {
                b.append(".localParallelism(").append(localParallelism).append(')');
            }
            b.append('\n');
        }
        for (Edge e : edges) {
            b.append("    .edge(").append(e).append(")\n");
        }
        return b.toString();
    }

    /**
     * Returns a JSON representation of the DAG.
     * <p>
     * <strong>Note:</strong> the exact structure of the JSON is unspecified.
     *
     * @param defaultLocalParallelism the local parallelism that will be shown if neither overridden on the
     *                                vertex nor the preferred parallelism is defined by meta-supplier
     */
    @Nonnull
    public JsonObject toJson(int defaultLocalParallelism) {
        JsonObject dag = new JsonObject();
        JsonArray vertices = new JsonArray();
        for (Vertex v : this) {
            JsonObject vertex = new JsonObject();
            vertex.add("name", v.getName());
            vertex.add("parallelism", v.determineLocalParallelism(defaultLocalParallelism));
            vertices.add(vertex);
        }
        dag.add("vertices", vertices);

        JsonArray edges = new JsonArray();
        for (Edge e : this.edges) {
            JsonObject edge = new JsonObject();
            edge.add("from", e.getSourceName());
            edge.add("fromOrdinal", e.getSourceOrdinal());
            edge.add("to", e.getDestName());
            edge.add("toOrdinal", e.getDestOrdinal());
            edge.add("priority", e.getPriority());
            edge.add("distributedTo", String.valueOf(e.getDistributedTo()));
            edge.add("type", StringUtil.lowerCaseInternal(e.getRoutingPolicy().toString()));
            edges.add(edge);
        }
        dag.add("edges", edges);
        return dag;
    }


    /**
     * Returns a DOT format (graphviz) representation of the DAG.
     */
    @Nonnull
    public String toDotString() {
        return toDotString(LOCAL_PARALLELISM_USE_DEFAULT, DEFAULT_QUEUE_SIZE);
    }

    /**
     * Returns a DOT format (graphviz) representation of the DAG, annotates
     * the vertices using supplied default parallelism value, and the edges
     * using supplied default queue size value.
     *
     * @param defaultLocalParallelism the local parallelism that will be shown if
     *                                neither overridden on the vertex nor the
     *                                preferred parallelism is defined by
     *                                meta-supplier
     * @param defaultQueueSize the queue size that will be shown if not overridden
     *                         on the edge
     */
    @Nonnull
    public String toDotString(int defaultLocalParallelism, int defaultQueueSize) {
        final StringBuilder builder = new StringBuilder(512);
        builder.append("digraph DAG {\n");
        int clusterCount = 0;

        for (Vertex v : this) {
            int localParallelism = v.determineLocalParallelism(defaultLocalParallelism);
            String parallelism = localParallelism == LOCAL_PARALLELISM_USE_DEFAULT ?
                defaultLocalParallelism == LOCAL_PARALLELISM_USE_DEFAULT ?
                    "default"
                    : String.valueOf(defaultLocalParallelism)
                : String.valueOf(localParallelism);
            builder.append("\t\"")
                   .append(escapeGraphviz(v.getName()))
                   .append("\" [localParallelism=").append(parallelism).append("]")
                   .append(";\n");
        }

        Map<String, int[]> inOutCounts = new HashMap<>();
        for (Edge edge : edges) {
            inOutCounts.computeIfAbsent(edge.getSourceName(), v -> new int[2])[0]++;
            inOutCounts.computeIfAbsent(edge.getDestName(), v -> new int[2])[1]++;
        }

        for (Vertex v: this) {
            List<Edge> out = getOutboundEdges(v.getName());
            for (Edge e : out) {
                List<String> attributes = new ArrayList<>();
                String edgeLabel = getEdgeLabel(e);
                if (!StringUtil.isNullOrEmpty(edgeLabel)) {
                    attributes.add("label=\"" + edgeLabel + "\"");
                }
                if (inOutCounts.get(e.getDestName())[1] > 1) {
                    attributes.add("headlabel=" + e.getDestOrdinal());
                }
                if (inOutCounts.get(e.getSourceName())[0] > 1) {
                    attributes.add("taillabel=" + e.getSourceOrdinal());
                }
                int queueSize = e.getConfig() == null ? defaultQueueSize :
                        e.getConfig().getQueueSize();
                attributes.add("queueSize=" + queueSize);

                boolean inSubgraph = e.getSourceName().equals(e.getDestName() + FIRST_STAGE_VERTEX_NAME_SUFFIX);
                if (inSubgraph) {
                    builder.append("\tsubgraph cluster_").append(clusterCount++).append(" {\n")
                           .append("\t");
                }
                String source = escapeGraphviz(e.getSourceName());
                String destination = escapeGraphviz(e.getDestName());
                builder.append("\t")
                       .append("\"").append(source).append("\"")
                       .append(" -> ")
                       .append("\"").append(destination).append("\"");
                if (attributes.size() > 0) {
                    builder.append(attributes.stream().collect(joining(", ", " [", "]")));
                }
                builder.append(";\n");
                if (inSubgraph) {
                    builder.append("\t}\n");
                }
            }
        }
        builder.append("}");
        return builder.toString();
    }

    private String getEdgeLabel(Edge e) {
        List<String> labels = new ArrayList<>();
        if (DISTRIBUTE_TO_ALL.equals(e.getDistributedTo())) {
            labels.add("distributed");
        } else if (e.getDistributedTo() != null) {
            labels.add("distributed to " + e.getDistributedTo());
        }
        if (e.getRoutingPolicy() != RoutingPolicy.UNICAST) {
            labels.add(StringUtil.lowerCaseInternal(e.getRoutingPolicy().toString()));
        }
        if (e.getOrderComparator() != null) {
            labels.add("ordered");
        }
        return String.join("-", labels);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(nameToVertex.size());

        for (Map.Entry<String, Vertex> entry : nameToVertex.entrySet()) {
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
            nameToVertex.put(key, value);
        }

        int edgeCount = in.readInt();

        for (int i = 0; i < edgeCount; i++) {
            Edge edge = in.readObject();
            edge.restoreSourceAndDest(nameToVertex);
            edges.add(edge);
        }

        verticesByIdentity.addAll(nameToVertex.values());
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetDataSerializerHook.DAG;
    }

    private void throwIfLocked() {
        if (locked) {
            throw new IllegalStateException("DAG is already locked");
        }
    }

    /**
     * Used to prevent further mutations to the DAG after submitting it for execution.
     * <p>
     * It's not a public API, can be removed in the future.
     */
    @PrivateApi
    public void lock() {
        locked = true;
        verticesByIdentity.forEach(Vertex::lock);
        edges.forEach(Edge::lock);
    }
}
