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

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.Iterator;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
public class DAGTest {

    private static final SimpleProcessorSupplier PROCESSOR_SUPPLIER = TestProcessor::new;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_iteratorOrder() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.vertex(c)
           .vertex(b)
           .vertex(a)
           .edge(from(a, 0).to(b, 0))
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
    public void test_reverseIteratorOrder() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.vertex(c)
           .vertex(b)
           .vertex(a)
           .edge(from(a, 0).to(b, 0))
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
    public void when_cycleInGraph_then_error() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        dag.vertex(a)
           .vertex(b)
           .edge(between(a, b))
           .edge(between(b, a));

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        dag.validate();
    }

    @Test
    public void when_duplicateOutputOrdinal_then_error() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.vertex(a)
           .vertex(b)
           .vertex(c)
           .edge(from(a, 0).to(b, 0));

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(a, 0).to(c, 0));
    }

    @Test
    public void when_gapInOutputOrdinal_then_error() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.vertex(a)
           .vertex(b)
           .vertex(c)
           .edge(from(a, 0).to(b, 0))
           .edge(from(a, 2).to(c, 0));

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        dag.validate();
    }

    @Test
    public void when_duplicateInputOrdinal_then_error() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.vertex(a)
           .vertex(b)
           .vertex(c)
           .edge(from(a, 0).to(c, 0));

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(b, 0).to(c, 0));
    }

    @Test
    public void when_gapInInputOrdinal_then_error() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.vertex(a)
           .vertex(b)
           .vertex(c)
           .edge(from(a, 0).to(c, 0))
           .edge(from(b, 0).to(c, 2));

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        dag.validate();
    }

    @Test
    public void when_multigraph_then_error() {
        // Given
        DAG dag = new DAG();
        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        dag.vertex(a)
           .vertex(b)
           .edge(from(a, 0).to(b, 0));

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        dag.edge(from(a, 1).to(b, 1));
    }

    private static class TestProcessor extends AbstractProcessor {
        @Override
        public boolean tryProcess(int ordinal, Object item) {
            return true;
        }
    }
}
