/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DagNodeUtilTest {
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

        DagNodeUtil util = new DagNodeUtil(vertices, allAddresses, node1Address);

        assertTrue(util.vertexExists(vertex_1));
        assertTrue(util.vertexExists(vertex_2));

        assertEquals(singleton(node1Address), util.getEdgeSources(edge_1_2));
        assertEquals(singleton(node1Address), util.getEdgeTargets(edge_1_2));
    }

    @Test
    public void when_traversingDAGAndCheckNumberOfConnections() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, null);

        DagNodeUtil util = new DagNodeUtil(vertices, allAddresses, node1Address);
        assertEquals(0, util.numRemoteSources(edge_1_2));
    }

    @Test
    public void when_traversingDATAAndCheckExistenceOfConnectionTo() {
        VertexDef vertex_1 = createMockVertex(1);
        VertexDef vertex_2 = createMockVertex(2);
        List<VertexDef> vertices = Arrays.asList(vertex_1, vertex_2);
        EdgeDef edge_1_2 = createEdge(vertex_1, vertex_2, null);

        DagNodeUtil util = new DagNodeUtil(vertices, allAddresses, node1Address);
        assertTrue(util.getEdgeSources(edge_1_2).contains(node1Address));
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

        DagNodeUtil util1 = new DagNodeUtil(vertices, allAddresses, node1Address);
        DagNodeUtil util2 = new DagNodeUtil(vertices, allAddresses, node2Address);

        assertTrue(util1.vertexExists(vertex_1));
        assertTrue(util2.vertexExists(vertex_1));
        assertTrue(util1.vertexExists(vertex_2));
        assertFalse(util2.vertexExists(vertex_2));

        assertEquals(allAddresses, util1.getEdgeSources(edge_1_2));
        assertEquals(singleton(node1Address), util1.getEdgeTargets(edge_1_2));
        assertEquals(emptySet(), util2.getEdgeSources(edge_1_2));
        assertEquals(singleton(node1Address), util2.getEdgeTargets(edge_1_2));
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

        DagNodeUtil util1 = new DagNodeUtil(vertices, allAddresses, node1Address);
        DagNodeUtil util2 = new DagNodeUtil(vertices, allAddresses, node2Address);

        assertTrue(util1.vertexExists(vertex_1));
        assertTrue(util1.vertexExists(vertex_2));
        assertTrue(util2.vertexExists(vertex_1));
        assertTrue(util2.vertexExists(vertex_2));

        assertEquals(allAddresses, util1.getEdgeSources(edge_1_2));
        assertEquals(allAddresses, util1.getEdgeTargets(edge_1_2));
        assertEquals(allAddresses, util2.getEdgeSources(edge_1_2));
        assertEquals(allAddresses, util2.getEdgeTargets(edge_1_2));
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

        DagNodeUtil util1 = new DagNodeUtil(vertices, allAddresses, node1Address);
        DagNodeUtil util2 = new DagNodeUtil(vertices, allAddresses, node2Address);

        assertTrue(util1.vertexExists(vertex_1));
        assertTrue(util2.vertexExists(vertex_1));
        assertTrue(util1.vertexExists(vertex_2));
        assertFalse(util2.vertexExists(vertex_2));
        assertTrue(util1.vertexExists(vertex_3));
        assertFalse(util2.vertexExists(vertex_3));

        assertEquals(allAddresses, util1.getEdgeSources(edge_1_2));
        assertEquals(singleton(node1Address), util1.getEdgeTargets(edge_1_2));
        assertEquals(emptySet(), util2.getEdgeSources(edge_1_2));
        assertEquals(singleton(node1Address), util2.getEdgeTargets(edge_1_2));

        assertEquals(singleton(node1Address), util1.getEdgeSources(edge_2_3));
        assertEquals(singleton(node1Address), util1.getEdgeTargets(edge_2_3));
        assertEquals(emptySet(), util2.getEdgeSources(edge_2_3));
        assertEquals(emptySet(), util2.getEdgeTargets(edge_2_3));
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
