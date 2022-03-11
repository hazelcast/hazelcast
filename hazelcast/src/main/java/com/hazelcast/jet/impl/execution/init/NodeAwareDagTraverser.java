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
import com.hazelcast.jet.core.Edge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * The traverser job is to find out which vertices and edges should be physically created from DAG. For example:
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
 * The V2 should not be created on second node. We also do not need to create V1 -> V2 edge from first to second node.
 *
 * I don't use Java streams here for a better performance.
 */
class NodeAwareDagTraverser {
    private final Map<Integer, Set<Address>> vertexOnAddress = new HashMap<>();
    private final Map<String, Set<Connection>> edgesToConnections = new HashMap<>();

    NodeAwareDagTraverser(List<VertexDef> vertices, Set<Address> nodesAddresses) {
        Queue<VertexDef> queue = new LinkedList<>();
        Set<Integer> visited = new HashSet<>();

        for (VertexDef vertex : vertices) {
            if (vertex.inboundEdges().isEmpty()) {
                vertexOnAddress.put(vertex.vertexId(), nodesAddresses);
                queue.add(vertex);
            } else {
                vertexOnAddress.put(vertex.vertexId(), new HashSet<>());
            }
        }

        while (!queue.isEmpty()) {
            VertexDef current = queue.poll();
            visited.add(current.vertexId());

            Set<Address> currentVertexAddresses = vertexOnAddress.get(current.vertexId());
            for (EdgeDef outboundEdge : current.outboundEdges()) {
                traverse(outboundEdge, currentVertexAddresses, nodesAddresses);
                if (allSourcesVisited(outboundEdge.destVertex(), visited) &&
                        !visited.contains(outboundEdge.destVertex().vertexId())) {
                    queue.add(outboundEdge.destVertex());
                }
            }
        }
    }

    private void traverse(EdgeDef outboundEdge, Set<Address> currentVertexAddresses, Set<Address> allAddresses) {
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

    private boolean allSourcesVisited(VertexDef current, Set<Integer> visited) {
        for (EdgeDef inboundEdge : current.inboundEdges()) {
            if (!visited.contains(inboundEdge.sourceVertex().vertexId())) {
                return false;
            }
        }
        return true;
    }

    Set<Connection> getAllConnectionsForEdge(EdgeDef edge) {
        return edgesToConnections.get(edge.edgeId());
    }

    boolean edgeExistsForConnection(EdgeDef edge, Address from, Address to) {
        Connection connection = new Connection(from, to);
        return edgesToConnections.get(edge.edgeId()).contains(connection);
    }

    boolean edgeExistsForConnectionTo(EdgeDef edge, Address to) {
        for (Connection connection : edgesToConnections.get(edge.edgeId())) {
            if (connection.isTo(to)) {
                return true;
            }
        }
        return false;
    }

    boolean vertexExistsOnNode(VertexDef vertex, Address nodeAddress) {
        return vertexOnAddress.get(vertex.vertexId()).contains(nodeAddress);
    }

    int numberOfConnections(EdgeDef edge, Address toAddress, Set<Address> fromAddresses) {
        Set<Connection> connections = edgesToConnections.get(edge.edgeId());

        int i = 0;
        for (Connection connection : connections) {
            if (toAddress.equals(connection.to) && fromAddresses.contains(connection.from)) {
                i++;
            }
        }
        return i;
    }

    static class Connection {
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
