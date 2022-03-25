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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.jet.core.Edge;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * A utility to deploy DAG on nodes. It helps to find which vertices never
 * receive data on some nodes and avoid creating processors on those nodes and
 * also queues, sender and receiver tasklets to those nodes.
 *
 * <pre>{@code
 * /--------|--------\
 * | Node 1 | Node 2 |
 * |--------|--------|
 * |   V1       V1   |
 * |   ||       //   |
 * |   ||    //      |
 * |   ||  //        |
 * |   \/|/          |
 * |   V2            |
 * \_________________/
 * }</pre>
 *
 * The V2 operator doesn't need to be created on the second node. We also do not
 * need to create V1 -> V2 edge from the first to the second node.
 * <p>
 * We don't use Java streams here for a better performance.
 */
class DagNodeUtil {
    private final Address localAddress;
    private final BitSet localVertices;
    private final Map<String, Set<Address>> targets = new HashMap<>();
    private final Map<String, Set<Address>> sources = new HashMap<>();

    @SuppressWarnings("checkstyle:executablestatementcount")
    DagNodeUtil(List<VertexDef> vertices, Set<Address> allAddresses, Address localAddress) {
        this.localAddress = localAddress;
        Int2ObjectHashMap<Set<Address>> vertexOnAddress = new Int2ObjectHashMap<>();
        Map<String, Set<Connection>> edgesToConnections = new HashMap<>();

        Queue<VertexDef> queue = new ArrayDeque<>(vertices.size());
        BitSet visited = new BitSet(vertices.size());

        for (VertexDef vertex : vertices) {
            if (vertex.inboundEdges().isEmpty()) {
                vertexOnAddress.put(vertex.vertexId(), allAddresses);
                queue.add(vertex);
            } else {
                vertexOnAddress.put(vertex.vertexId(), new HashSet<>());
            }
        }

        while (!queue.isEmpty()) {
            VertexDef current = queue.poll();
            visited.set(current.vertexId());

            Set<Address> currentVertexAddresses = vertexOnAddress.get(current.vertexId());
            for (EdgeDef outboundEdge : current.outboundEdges()) {
                traverse(outboundEdge, currentVertexAddresses, allAddresses, vertexOnAddress, edgesToConnections);
                if (allSourcesVisited(outboundEdge.destVertex(), visited) &&
                        !visited.get(outboundEdge.destVertex().vertexId())) {
                    queue.add(outboundEdge.destVertex());
                }
            }
        }

        localVertices = new BitSet(vertices.size());
        populateFields(vertexOnAddress, edgesToConnections);
    }

    private void populateFields(
            Int2ObjectHashMap<Set<Address>> vertexOnAddress,
            Map<String, Set<Connection>> edgesToConnections
    ) {
        for (Entry<Integer, Set<Address>> entry : vertexOnAddress.entrySet()) {
            if (entry.getValue().contains(localAddress)) {
                localVertices.set(entry.getKey());
            }
        }

        for (String edgeId : edgesToConnections.keySet()) {
            sources.put(edgeId, new HashSet<>());
            targets.put(edgeId, new HashSet<>());
        }

        for (Entry<String, Set<Connection>> entry : edgesToConnections.entrySet()) {
            for (Connection connection : entry.getValue()) {
                if (connection.isTo(localAddress)) {
                    sources.computeIfAbsent(entry.getKey(), x -> new HashSet<>()).add(connection.from);
                }
                if (connection.isFrom(localAddress)) {
                    targets.computeIfAbsent(entry.getKey(), x -> new HashSet<>()).add(connection.to);
                }
            }
        }
    }

    private void traverse(
            EdgeDef outboundEdge,
            Set<Address> currentVertexAddresses,
            Set<Address> allAddresses,
            Int2ObjectHashMap<Set<Address>> vertexOnAddress,
            Map<String, Set<Connection>> edgesToConnections
    ) {
        Set<Connection> connections = edgesToConnections.computeIfAbsent(outboundEdge.edgeId(), edgeDef -> new HashSet<>());
        if (outboundEdge.isLocal()) {
            for (Address address : currentVertexAddresses) {
                connections.add(new Connection(address, address));
                Set<Address> addresses = vertexOnAddress.get(outboundEdge.destVertex().vertexId());
                if (addresses.size() != allAddresses.size()) {
                    addresses.add(address);
                }
            }
        } else if (outboundEdge.getDistributedTo().equals(Edge.DISTRIBUTE_TO_ALL)) {
            for (Address sourceAddress : currentVertexAddresses) {
                for (Address targetAddress : allAddresses) {
                    connections.add(new Connection(sourceAddress, targetAddress));
                }
            }
            vertexOnAddress.put(outboundEdge.destVertex().vertexId(), allAddresses);
        } else {
            for (Address address : currentVertexAddresses) {
                connections.add(new Connection(address, outboundEdge.getDistributedTo()));
            }
            Set<Address> addresses = vertexOnAddress.get(outboundEdge.destVertex().vertexId());
            if (addresses.size() != allAddresses.size()) {
                addresses.add(outboundEdge.getDistributedTo());
            }
        }
    }

    private boolean allSourcesVisited(VertexDef current, BitSet visited) {
        for (EdgeDef inboundEdge : current.inboundEdges()) {
            if (!visited.get(inboundEdge.sourceVertex().vertexId())) {
                return false;
            }
        }
        return true;
    }

    Set<Address> getEdgeTargets(EdgeDef edge) {
        return targets.get(edge.edgeId());
    }

    Set<Address> getEdgeSources(EdgeDef edge) {
        return sources.get(edge.edgeId());
    }

    boolean vertexExists(VertexDef vertex) {
        return localVertices.get(vertex.vertexId());
    }

    int numRemoteSources(EdgeDef edge) {
        Set<Address> edgeSources = sources.get(edge.edgeId());
        return edgeSources.contains(localAddress) ? edgeSources.size() - 1 : edgeSources.size();
    }

    private static class Connection {
        final Address from;
        final Address to;
        private final int hashCode;

        Connection(Address from, Address to) {
            this.from = from;
            this.to = to;
            this.hashCode = Objects.hash(from, to);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Connection that = (Connection) o;
            return hashCode == that.hashCode && Objects.equals(from, that.from) && Objects.equals(to, that.to);
        }

        boolean isFrom(Address from) {
            return from.equals(this.from);
        }

        boolean isTo(Address to) {
            return to.equals(this.to);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
