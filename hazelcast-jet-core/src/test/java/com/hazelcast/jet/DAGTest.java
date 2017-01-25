/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@Category(QuickTest.class)
public class DAGTest {

    private static final Supplier<Processor> PROCESSOR_SUPPLIER = TestProcessor::new;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void when_newVertex_then_hasIt() {
        final DAG dag = new DAG();
        final Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        assertSame(a, dag.getVertex("a"));
    }

    @Test
    public void when_connectKnownVertices_then_success() {
        final DAG dag = new DAG();
        final Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        final Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b));
    }

    @Test
    public void when_unknownSource_then_illegalArgument() {
        // Given
        final DAG dag = new DAG();
        final Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        final Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a, b));
    }

    @Test
    public void when_unknownDestination_then_illegalArgument() {
        // Given
        final DAG dag = new DAG();
        final Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        final Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(between(a, b));
    }

    @Test
    public void when_addEdgeWithoutDestination_then_illegalArgument() {
        // Given
        final DAG dag = new DAG();
        final Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(a));
    }

    @Test
    public void when_differentSourceWithSameName_then_illegalArgument() {
        // Given
        Vertex a1 = new Vertex("a", Processors.map(Object::hashCode));
        Vertex a2 = new Vertex("a", Processors.map(Object::toString));
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        final DAG dag = new DAG()
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
        Vertex b1 = new Vertex("b", Processors.map(Object::toString));
        Vertex b2 = new Vertex("b", Processors.map(Object::hashCode));
        final DAG dag = new DAG()
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
        final DAG dag = new DAG();
        final Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        final Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        final Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        final Vertex d = dag.newVertex("d", PROCESSOR_SUPPLIER);
        final Edge e1 = from(a).to(d, 0);
        final Edge e2 = from(b).to(d, 1);
        final Edge e3 = from(c).to(d, 2);
        dag.edge(e1)
           .edge(e2)
           .edge(e3)
           .edge(from(a, 1).to(b));

        // When
        final Set<Edge> edges = new HashSet<>(dag.getInboundEdges("d"));

        // Then
        assertEquals(new HashSet<>(asList(e1, e2, e3)), edges);
    }

    @Test
    public void outboundEdges() {
        // Given
        final DAG dag = new DAG();
        final Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        final Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        final Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        final Vertex d = dag.newVertex("d", PROCESSOR_SUPPLIER);
        final Edge e1 = from(a, 0).to(b);
        final Edge e2 = from(a, 1).to(c);
        final Edge e3 = from(a, 2).to(d);
        dag.edge(e1)
           .edge(e2)
           .edge(e3)
           .edge(from(b).to(c, 1));

        // When
        final Set<Edge> edges = new HashSet<>(dag.getOutboundEdges("a"));

        // Then
        assertEquals(new HashSet<>(asList(e1, e2, e3)), edges);
    }

    @Test
    public void iteratorOrder() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(b, 0))
           .edge(from(b, 0).to(c, 0))
           .edge(from(a, 1).to(c, 1));

        // When
        Iterator<Vertex> iterator = dag.iterator();
        Vertex v1 = iterator.next();
        Vertex v2 = iterator.next();
        Vertex v3 = iterator.next();

        // Then
        assertEquals(a, v1);
        assertEquals(b, v2);
        assertEquals(c, v3);
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void reverseIteratorOrder() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        Vertex c = dag.newVertex("c", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(b, 0))
           .edge(from(b, 0).to(c, 0))
           .edge(from(a, 1).to(c, 1));

        // When
        Iterator<Vertex> iterator = dag.reverseIterator();
        Vertex v1 = iterator.next();
        Vertex v2 = iterator.next();
        Vertex v3 = iterator.next();

        // Then
        assertEquals(c, v1);
        assertEquals(b, v2);
        assertEquals(a, v3);
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void when_cycleInGraph_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(between(a, b))
           .edge(between(b, a));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

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
    public void when_multigraph_then_invalid() {
        // Given
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", PROCESSOR_SUPPLIER);
        Vertex b = dag.newVertex("b", PROCESSOR_SUPPLIER);
        dag.edge(from(a, 0).to(b, 0));

        // Then
        exceptionRule.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(a, 1).to(b, 1));
    }

    private static class TestProcessor extends AbstractProcessor {
        @Override
        public boolean tryProcess(int ordinal, @Nonnull Object item) {
            return true;
        }
    }
}
