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

class DagTraverser {
    private static final boolean DISABLE = !true;

    private final Map<VertexDef, Set<Address>> vertexOnAddress = new HashMap<>();
    private final Map<EdgeDef, Set<Connection>> edgesToConnections = new HashMap<>();
    private final Queue<VertexDef> queue = new LinkedList<>();
    private final Set<VertexDef> visited = new HashSet<>();

    DagTraverser(List<VertexDef> vertices, Set<Address> allAddresses) {
        for (VertexDef vertex : vertices) {
            if (vertex.inboundEdges().isEmpty()) {
                vertexOnAddress.put(vertex, allAddresses);
                queue.add(vertex);
            }
        }

        while (!queue.isEmpty()) {
            VertexDef current = queue.poll();
            if (!allSourcesVisited(current) || visited.contains(current)) {
                continue;
            }

            Set<Address> currentVertexAddresses = vertexOnAddress.get(current);
            for (EdgeDef outboundEdge : current.outboundEdges()) {
                traverse(outboundEdge, currentVertexAddresses, allAddresses);
                queue.add(outboundEdge.destVertex());
            }
            visited.add(current);
        }
    }

    boolean containsEdge(EdgeDef edge, Address from, Address to) {
        Connection connection = new Connection(from, to);
        return DISABLE || edgesToConnections.get(edge).contains(connection);
    }

    boolean containsVertex(VertexDef vertex, Address thisAddress) {
        return DISABLE || vertexOnAddress.get(vertex).contains(thisAddress);
    }

    int numberOfConnections(EdgeDef edge, Address toAddress, Set<Address> fromAddresses) {
        if (DISABLE) {
            return fromAddresses.size();
        }
        Set<Connection> connections = edgesToConnections.get(edge);
        return (int) connections.stream()
                .filter(connection -> toAddress.equals(connection.to) && fromAddresses.contains(connection.from))
                .count();
    }

    int numberOfRemotesIncoming(EdgeDef edge, Set<Address> remoteAddresses) {
        if (DISABLE) {
            return remoteAddresses.size();
        }
        Set<Connection> connections = edgesToConnections.get(edge);
        return (int) connections.stream()
                .filter(connection -> remoteAddresses.contains(connection.to))
                .count();
    }

    int numberOfRemotesOutgoing(EdgeDef edge, Set<Address> remoteAddresses) {
        if (DISABLE) {
            return remoteAddresses.size();
        }
        Set<Connection> connections = edgesToConnections.get(edge);
        return (int) connections.stream()
                .filter(connection -> remoteAddresses.contains(connection.from))
                .count();
    }


    private void traverse(EdgeDef outboundEdge, Set<Address> currentVertexAddresses, Set<Address> allAddresses) {
        Set<Connection> connections = edgesToConnections.computeIfAbsent(outboundEdge, edgeDef -> new HashSet<>());
        if (outboundEdge.isLocal()) {
            for (Address address : currentVertexAddresses) {
                connections.add(new Connection(address, address));
                vertexOnAddress.computeIfAbsent(outboundEdge.destVertex(), vertexDef -> new HashSet<>()).add(address);
            }
        } else if (outboundEdge.getDistributedTo().equals(Edge.DISTRIBUTE_TO_ALL)) {
            for (Address sourceAddress : currentVertexAddresses) {
                for (Address targetAddress : allAddresses) {
                    connections.add(new Connection(sourceAddress, targetAddress));
                }
            }
            vertexOnAddress.put(outboundEdge.destVertex(), allAddresses);
        } else {
            for (Address address : currentVertexAddresses) {
                connections.add(new Connection(address, outboundEdge.getDistributedTo()));
            }
            vertexOnAddress.computeIfAbsent(outboundEdge.destVertex(), vertexDef -> new HashSet<>())
                    .add(outboundEdge.getDistributedTo());
        }
    }

    private boolean allSourcesVisited(VertexDef current) {
        return current.inboundEdges().stream().allMatch(edgeDef -> visited.contains(edgeDef.sourceVertex()));
    }

    static class Connection {
        private final Address from;
        private final Address to;

        public Connection(Address from, Address to) {
            this.from = from;
            this.to = to;
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
            return Objects.equals(from, that.from) && Objects.equals(to, that.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }
    }
}
