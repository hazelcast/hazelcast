/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import org.junit.Test;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static org.junit.Assert.assertEquals;

public class CreateDagVisitorTest {

    private static final SupplierEx<Processor> PROCESSOR_SUPPLIER = noopP();

    @Test
    public void when_loweringDownParallelismBetweenTwoDefaultParallelismVertices_then_parallelismIsChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        CreateDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(a.getLocalParallelism(), 4);
        assertEquals(b.getLocalParallelism(), 4);
    }

    @Test
    public void when_loweringDownParallelismBetweenThreeDefaultParallelismVertices_then_parallelismIsChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        dag.edge(between(b, c));
        CreateDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(a.getLocalParallelism(), 4);
        assertEquals(b.getLocalParallelism(), 4);
        assertEquals(c.getLocalParallelism(), 4);
    }

    @Test
    public void when_loweringDownParallelismBetweenOneDefaultParallelismVertex_then_parallelismIsNotChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);

        a.localParallelism(1);
        dag.edge(between(a, b));
        CreateDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(a.getLocalParallelism(), 1);
        assertEquals(b.getLocalParallelism(), LOCAL_PARALLELISM_USE_DEFAULT);
    }

    @Test
    public void when_loweringDownParallelismWithSingleCooperativeThreadCount_then_parallelismIsNotChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);

        dag.edge(between(a, b));
        CreateDagVisitor.decreaseParallelism(dag, 1);

        assertEquals(a.getLocalParallelism(), LOCAL_PARALLELISM_USE_DEFAULT);
        assertEquals(b.getLocalParallelism(), LOCAL_PARALLELISM_USE_DEFAULT);
    }

    @Test
    public void when_loweringDownParallelismOnLocalEdge_then_edgeIsIsolated() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        CreateDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(Edge.RoutingPolicy.ISOLATED, dag.getOutboundEdges(a.getName()).get(0).getRoutingPolicy());
    }

    @Test
    public void when_loweringDownParallelismOnDistributedEdge_then_parallelismIsNotChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        Edge edge = dag.getOutboundEdges(a.getName()).get(0);
        edge.distributed();

        CreateDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(a.getLocalParallelism(), LOCAL_PARALLELISM_USE_DEFAULT);
        assertEquals(b.getLocalParallelism(), LOCAL_PARALLELISM_USE_DEFAULT);
    }

    @Test
    public void when_loweringDownParallelismOnDistributedEdge_then_edgeRoutingPolicyIsNotChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        Edge edge = dag.getOutboundEdges(a.getName()).get(0);
        edge.distributed();
        Edge.RoutingPolicy routingPolicy = edge.getRoutingPolicy();

        CreateDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(routingPolicy, edge.getRoutingPolicy());
    }
}
