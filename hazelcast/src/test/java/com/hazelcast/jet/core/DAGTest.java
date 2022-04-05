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
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DAGTest {

    private static final SupplierEx<Processor> PROCESSOR_SUPPLIER = noopP();

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void when_newVertex_then_hasIt() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        assertSame(a, dag.getVertex("a"));
    }

    @Test
    public void when_newUniqueVertex_then_hasIt() {
        DAG dag = new DAG();
        Vertex a0 = dag.newUniqueVertex("a", PROCESSOR_SUPPLIER);
        assertSame(a0, dag.getVertex("a"));

        Vertex a1 = dag.newUniqueVertex("a", PROCESSOR_SUPPLIER);
        assertSame(a1, dag.getVertex("a-2"));
    }

    @Test
    public void when_connectKnownVertices_then_success() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
    }

    @Test
    public void when_selfEdge_then_illegalArgument() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a, a));
    }

    @Test
    public void when_unknownSource_then_illegalArgument() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a, b));
    }

    @Test
    public void when_unknownDestination_then_illegalArgument() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a, b));
    }

    @Test
    public void when_addEdgeWithoutDestination_then_illegalArgument() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Edge has no destination");

        // When
        dag.edge(from(a));
    }

    @Test
    public void when_differentSourceWithSameName_then_illegalArgument() {
        // Given
        Vertex a1 = new Vertex("a", Processors.mapP(Object::hashCode));
        Vertex a2 = new Vertex("a", Processors.mapP(Object::toString));
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        DAG dag = new DAG()
                .vertex(a1)
                .vertex(b);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a2, b));
    }

    @Test
    public void when_differentDestinationWithSameName_then_illegalArgument() {
        // Given
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b1 = new Vertex("b", Processors.mapP(Object::toString));
        Vertex b2 = new Vertex("b", Processors.mapP(Object::hashCode));
        DAG dag = new DAG()
                .vertex(a)
                .vertex(b1);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a, b2));
    }

    @Test
    public void inboundEdges() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        Vertex d = dag.newVertex("d", PROCESSOR_SUPPLIER);
        Edge e1 = from(a).to(d, 0);
        Edge e2 = from(b).to(d, 1);
        Edge e3 = from(c).to(d, 2);
        dag.edge(e1)
           .edge(e2)
           .edge(e3)
           .edge(from(a, 1).to(b));

        // When
        Set<Edge> edges = new HashSet<>(dag.getInboundEdges("d"));

        // Then
        assertEquals(new HashSet<>(asList(e1, e2, e3)), edges);
    }

    @Test
    public void outboundEdges() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        Vertex d = dag.newVertex("d", PROCESSOR_SUPPLIER);
        Edge e1 = from(a, 0).to(b);
        Edge e2 = from(a, 1).to(c);
        Edge e3 = from(a, 2).to(d);
        dag.edge(e1)
           .edge(e2)
           .edge(e3)
           .edge(from(b).to(c, 1));

        // When
        Set<Edge> edges = new HashSet<>(dag.getOutboundEdges("a"));

        // Then
        assertEquals(new HashSet<>(asList(e1, e2, e3)), edges);
    }

    @Test
    public void when_iterator_then_topologicalOrder() {
        // We'll build this DAG:
        // a --> b \
        //           --> e --> f
        // c --> d /

        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        Vertex d = dag.newVertex("d", PROCESSOR_SUPPLIER);
        Vertex e = dag.newVertex("e", PROCESSOR_SUPPLIER);
        Vertex f = dag.newVertex("f", PROCESSOR_SUPPLIER);

        dag.edge(between(a, b))
           .edge(between(b, e))
           .edge(between(c, d))
           .edge(from(d).to(e, 1))
           .edge(between(e, f));

        // When
        List<Vertex> sorted = new ArrayList<>();
        for (Vertex v : dag) {
            sorted.add(v);
        }

        // Then
        // Assert that for every edge x -> y, x is before y in the ordering.
        assertTrue(sorted.indexOf(a) < sorted.indexOf(b));
        assertTrue(sorted.indexOf(b) < sorted.indexOf(e));
        assertTrue(sorted.indexOf(c) < sorted.indexOf(d));
        assertTrue(sorted.indexOf(d) < sorted.indexOf(e));
        assertTrue(sorted.indexOf(e) < sorted.indexOf(f));
    }

    @Test
    public void when_cycle_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex x = dag.newVertex("x", PROCESSOR_SUPPLIER);
        Vertex y = dag.newVertex("y", PROCESSOR_SUPPLIER);
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b))
           .edge(between(b, c))
           .edge(between(c, a))
           .edge(between(x, y))
           .edge(from(y).to(a, 1));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.anyOf(
                Matchers.containsString("a -> b -> c -> a"),
                Matchers.containsString("b -> c -> a -> b"),
                Matchers.containsString("c -> a -> b -> c")
        ));

        // When
        dag.validate();
    }

    @Test
    public void when_duplicateOutputOrdinal_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(b, 0));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(a, 0).to(c, 0));
    }

    @Test
    public void when_gapInOutputOrdinal_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(b, 0))
           .edge(from(a, 2).to(c, 0));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.validate();
    }

    @Test
    public void when_duplicateInputOrdinal_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(c, 0));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(b, 0).to(c, 0));
    }

    @Test
    public void when_gapInInputOrdinal_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(c, 0))
           .edge(from(b, 0).to(c, 2));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.validate();
    }

    @Test
    public void when_multigraph_then_valid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(b, 0));

        // When
        dag.edge(from(a, 1).to(b, 1));
    }

    @Test
    public void when_duplicateZeroDestOrdinal_then_errorAdvisesFromTo() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(between(a, c));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Edge.from().to()");

        // When
        dag.edge(between(b, c));
    }

    @Test
    public void when_duplicateNonZeroDestOrdinal_then_errorDoesntAdviseFromTo() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a).to(c, 1));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(not(containsString("Edge.from().to()")));

        // When
        dag.edge(from(b).to(c, 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void when_mutatingLockedDag_then_fail() {
        DAG dag = new DAG();

        List<Supplier> mutatingMethods = Arrays.asList(
                () -> dag.newVertex(null, (SupplierEx<? extends Processor>) null),
                () -> dag.newVertex(null, (ProcessorSupplier) null),
                () -> dag.newVertex(null, (ProcessorMetaSupplier) null),
                () -> dag.newUniqueVertex(null, (SupplierEx<? extends Processor>) null),
                () -> dag.newUniqueVertex(null, (ProcessorSupplier) null),
                () -> dag.newUniqueVertex(null, (ProcessorMetaSupplier) null),
                () -> dag.vertex(null),
                () -> dag.edge(null)
        );

        dag.lock();
        for (Supplier mutatingMethod : mutatingMethods) {
            assertThrows(IllegalStateException.class, mutatingMethod::get);
        }
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void when_mutatingLockedVertexInDag_then_fail() {
        DAG dag = new DAG();
        Vertex vertex = dag.newVertex("", (ProcessorMetaSupplier) addresses -> address -> null);

        List<Supplier> mutatingMethods = Arrays.asList(
                () -> vertex.localParallelism(1),
                () -> {
                    vertex.updateMetaSupplier(null);
                    return null;
                }
        );

        dag.lock();
        for (Supplier mutatingMethod : mutatingMethods) {
            assertThrows(IllegalStateException.class, mutatingMethod::get);
        }
    }
    @Test
    @SuppressWarnings("rawtypes")
    public void when_mutatingLockedEdgeInDag_then_fail() {
        DAG dag = new DAG();
        Vertex vertex1 = dag.newVertex("1", (ProcessorMetaSupplier) addresses -> address -> null);
        Vertex vertex2 = dag.newVertex("2", (ProcessorMetaSupplier) addresses -> address -> null);
        Edge edge = between(vertex1, vertex2);
        dag.edge(edge);

        List<Supplier> mutatingMethods = Arrays.asList(
                () -> edge.to(null),
                () -> edge.to(null, 0),
                () -> edge.priority(0),
                () -> edge.unicast(),
                () -> edge.partitioned(null),
                () -> edge.partitioned(null, null),
                () -> edge.allToOne(null),
                () -> edge.broadcast(),
                () -> edge.isolated(),
                () -> edge.ordered(null),
                () -> edge.fanout(),
                () -> edge.local(),
                () -> edge.distributed(),
                () -> edge.distributeTo(null),
                () -> edge.setConfig(null)
        );

        dag.lock();
        for (Supplier mutatingMethod : mutatingMethods) {
            assertThrows(IllegalStateException.class, mutatingMethod::get);
        }
    }
}
