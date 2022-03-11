package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeAwareDagTraverserTest extends TestCase {
    public static final ProcessorSupplier MOCK_PROCESSOR_SUPPLIER = null;
    public static final int MOCK_DEFAULT_PARALLELISM = 1;

    private static Address node1Address;
    private static Address node2Address;
    private static Set<Address> allAddresses;

    @BeforeClass
    public static void initTest() throws UnknownHostException {
        node1Address = new Address("127.0.0.1", 5701);
        node2Address = new Address("127.0.0.1", 5702);
        allAddresses = Sets.newSet(node1Address, node2Address);
    }

    /*
     * DAG: V1 -(local)-> V2 with local edge on two nodes, result:
     *
     * Node 1 -> Node 1: V1 -> V2
     * Node 2 -> Node 2: V1 -> V2
     *
     * Node 1 -> Node 2: _empty_
     * Node 2 -> Node 1: _empty_
     */
    @Test
    public void when_traversingDAGWithLocalEdge_then_noRemoteEdges() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, null);

        NodeAwareDagTraverser traverser = new NodeAwareDagTraverser(vertices, allAddresses);

        assertTrue(traverser.vertexExistsOnNode(vertex_1, node1Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_1, node2Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_2, node1Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_2, node2Address));

        Set<NodeAwareDagTraverser.Connection> connections_1_2 = traverser.getAllConnectionsForEdge(edge_1_2);
        assertEquals(2, connections_1_2.size());
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node1Address, node1Address));
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node2Address, node2Address));
    }

    @Test
    public void when_traversingDAGAndCheckNumberOfConnections() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, null);

        NodeAwareDagTraverser traverser = new NodeAwareDagTraverser(vertices, allAddresses);
        assertEquals(1, traverser.numberOfConnections(edge_1_2, node1Address, allAddresses));
        assertEquals(1, traverser.numberOfConnections(edge_1_2, node2Address, allAddresses));
        assertEquals(0, traverser.numberOfConnections(edge_1_2, node1Address, Sets.newSet(node2Address)));
        assertEquals(0, traverser.numberOfConnections(edge_1_2, node2Address, Sets.newSet(node1Address)));
    }

    @Test
    public void when_traversingDATAAndCheckExistenceOfConnectionTo() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, null);

        NodeAwareDagTraverser traverser = new NodeAwareDagTraverser(vertices, allAddresses);
        assertTrue(traverser.edgeExistsForConnectionTo(edge_1_2, node1Address));
        assertTrue(traverser.edgeExistsForConnectionTo(edge_1_2, node2Address));
    }

    /*
     * DAG: V1 -(dist to one)-> V2 with distributed to one edge on two nodes, result:
     *
     * Node 1 -> Node 1: V1 -> V2
     * Node 2 -> Node 2: _empty_
     *
     * Node 1 -> Node 2: _empty_
     * Node 2 -> Node 1: V1 -> V2
     */
    @Test
    public void when_traversingDAGWithDistributedToOneEdge_then_remoteEdgePresent() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, node1Address);

        NodeAwareDagTraverser traverser = new NodeAwareDagTraverser(vertices, allAddresses);

        assertTrue(traverser.vertexExistsOnNode(vertex_1, node1Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_1, node2Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_2, node1Address));
        assertFalse(traverser.vertexExistsOnNode(vertex_2, node2Address));

        Set<NodeAwareDagTraverser.Connection> connections_1_2 = traverser.getAllConnectionsForEdge(edge_1_2);
        assertEquals(2, connections_1_2.size());
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node1Address, node1Address));
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node2Address, node1Address));
    }

    /*
     * DAG: V1 -(dist to all)-> V2 on two nodes, result:
     *
     * Node 1 -> Node 1: V1 -> V2
     * Node 2 -> Node 2: V1 -> V2
     *
     * Node 1 -> Node 2: V1 -> V2
     * Node 2 -> Node 1: V1 -> V2
     */
    @Test
    public void when_traversingDAGWithDistributedToAllEdge_then_remoteEdgesPresent() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, Edge.DISTRIBUTE_TO_ALL);

        NodeAwareDagTraverser traverser = new NodeAwareDagTraverser(vertices, allAddresses);

        assertTrue(traverser.vertexExistsOnNode(vertex_1, node1Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_1, node2Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_2, node1Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_2, node2Address));

        Set<NodeAwareDagTraverser.Connection> connections_1_2 = traverser.getAllConnectionsForEdge(edge_1_2);
        assertEquals(4, connections_1_2.size());
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node1Address, node1Address));
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node2Address, node1Address));
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node1Address, node2Address));
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node2Address, node2Address));
    }

    /*
     * DAG: V1 -(dist to one)-> V2 -(local)-> V3 with , result:
     *
     * Node 1 -> Node 1: V1 -> V2, V2 -> V3
     * Node 2 -> Node 2:
     *
     * Node 1 -> Node 2: V1 -> V2
     * Node 2 -> Node 1: _empty_
     */
    @Test
    public void when_traversingDAGWithLocalEdgeAfterDistributedToOne_then_oneRemoteEdge() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        VertexDef vertex_3 = createMockVertex(3);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2, vertex_3);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, node1Address);
        EdgeDef edge_2_3 = createEdge(vertex_2, vertex_3, null);

        NodeAwareDagTraverser traverser = new NodeAwareDagTraverser(vertices, allAddresses);

        assertTrue(traverser.vertexExistsOnNode(vertex_1, node1Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_1, node2Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_2, node1Address));
        assertFalse(traverser.vertexExistsOnNode(vertex_2, node2Address));
        assertTrue(traverser.vertexExistsOnNode(vertex_3, node1Address));
        assertFalse(traverser.vertexExistsOnNode(vertex_3, node2Address));

        Set<NodeAwareDagTraverser.Connection> connections_1_2 = traverser.getAllConnectionsForEdge(edge_1_2);
        assertEquals(2, connections_1_2.size());
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node1Address, node1Address));
        assertTrue(traverser.edgeExistsForConnection(edge_1_2, node2Address, node1Address));

        Set<NodeAwareDagTraverser.Connection> connections_2_3 = traverser.getAllConnectionsForEdge(edge_2_3);
        assertEquals(1, connections_2_3.size());
        assertTrue(traverser.edgeExistsForConnection(edge_2_3, node1Address, node1Address));
    }

    private EdgeDef createEdge(VertexDef from, VertexDef to, Address distributedTo) {
        EdgeDef edge = new MockEdgeDef(from, to, distributedTo);
        from.outboundEdges().add(edge);
        to.inboundEdges().add(edge);
        return edge;
    }

    private VertexDef createMockVertex(int id) {
        return new VertexDef(id, id + "", MOCK_PROCESSOR_SUPPLIER, MOCK_DEFAULT_PARALLELISM);
    }

    private static class MockEdgeDef extends EdgeDef {
        MockEdgeDef(VertexDef sourceVertex, VertexDef destVertex, Address distributedTo) {
            this.sourceVertex = sourceVertex;
            this.destVertex = destVertex;
            this.distributedTo = distributedTo;
            this.id = sourceVertex.vertexId() + "_" + destVertex.vertexId();
        }
    }
}