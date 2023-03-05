/*
 * Copyright 2023 Hazelcast Inc.
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
        CreateTopLevelDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(4, a.getLocalParallelism());
        assertEquals(4, b.getLocalParallelism());
    }

    @Test
    public void when_loweringDownParallelismBetweenThreeDefaultParallelismVertices_then_parallelismIsChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        dag.edge(between(b, c));
        CreateTopLevelDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(4, a.getLocalParallelism());
        assertEquals(4, b.getLocalParallelism());
        assertEquals(4, c.getLocalParallelism());
    }

    @Test
    public void when_loweringDownParallelismBetweenOneDefaultParallelismVertex_then_parallelismIsNotChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);

        a.localParallelism(1);
        dag.edge(between(a, b));
        CreateTopLevelDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(1, a.getLocalParallelism());
        assertEquals(LOCAL_PARALLELISM_USE_DEFAULT, b.getLocalParallelism());
    }

    @Test
    public void when_loweringDownParallelismWithSingleCooperativeThreadCount_then_parallelismIsNotChanged() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);

        dag.edge(between(a, b));
        CreateTopLevelDagVisitor.decreaseParallelism(dag, 1);

        assertEquals(LOCAL_PARALLELISM_USE_DEFAULT, a.getLocalParallelism());
        assertEquals(LOCAL_PARALLELISM_USE_DEFAULT, b.getLocalParallelism());
    }

    @Test
    public void when_loweringDownParallelismOnLocalEdge_then_edgeIsIsolated() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
        CreateTopLevelDagVisitor.decreaseParallelism(dag, 16);

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

        CreateTopLevelDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(LOCAL_PARALLELISM_USE_DEFAULT, a.getLocalParallelism());
        assertEquals(LOCAL_PARALLELISM_USE_DEFAULT, b.getLocalParallelism());
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

        CreateTopLevelDagVisitor.decreaseParallelism(dag, 16);

        assertEquals(routingPolicy, edge.getRoutingPolicy());
    }
}
