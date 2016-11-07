/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.jet2.impl.AbstractProcessor;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
public class DAGTest {

    private static final SimpleProcessorSupplier PROCESSOR_SUPPLIER = TestProcessor::new;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_iterator() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.addVertex(c)
                .addVertex(b)
                .addVertex(a)
                .addEdge(new Edge(a, 0, b, 0))
                .addEdge(new Edge(b, 0, c, 0))
                .addEdge(new Edge(a, 1, c, 1));

        Iterator<Vertex> iterator = dag.iterator();
        assertEquals(a, iterator.next());
        assertEquals(b, iterator.next());
        assertEquals(c, iterator.next());
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void test_reverseIterator() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.addVertex(c)
                .addVertex(b)
                .addVertex(a)
                .addEdge(new Edge(a, 0, b, 0))
                .addEdge(new Edge(b, 0, c, 0))
                .addEdge(new Edge(a, 1, c, 1));

        Iterator<Vertex> iterator = dag.reverseIterator();
        assertEquals(c, iterator.next());
        assertEquals(b, iterator.next());
        assertEquals(a, iterator.next());
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void test_cycleDetection() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);

        dag.addVertex(a)
                .addVertex(b)
                .addEdge(new Edge(a, b))
                .addEdge(new Edge(b, a));

        expectedException.expect(IllegalArgumentException.class);
        dag.verify();
    }

    @Test
    public void test_duplicateOutputOrdinal() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);

        dag.addVertex(a)
                .addVertex(b)
                .addVertex(c)
                .addEdge(new Edge(a, 0, b, 0));

        expectedException.expect(IllegalArgumentException.class);
        dag.addEdge(new Edge(a, 0, c, 0));
    }

    @Test
    public void test_gapInOutputOrdinal() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);

        dag.addVertex(a)
                .addVertex(b)
                .addVertex(c)
                .addEdge(new Edge(a, 0, b, 0))
                .addEdge(new Edge(a, 2, c, 0));

        expectedException.expect(IllegalArgumentException.class);
        dag.verify();
    }

    @Test
    public void test_duplicateInputOrdinal() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);

        dag.addVertex(a)
                .addVertex(b)
                .addVertex(c)
                .addEdge(new Edge(a, 0, c, 0));

        expectedException.expect(IllegalArgumentException.class);
        dag.addEdge(new Edge(b, 0, c, 0));
    }

    @Test
    public void test_gapInputOrdinal() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);

        dag.addVertex(a)
                .addVertex(b)
                .addVertex(c)
                .addEdge(new Edge(a, 0, c, 0))
                .addEdge(new Edge(b, 0, c, 2));

        expectedException.expect(IllegalArgumentException.class);
        dag.verify();
    }

    private static class TestProcessor extends AbstractProcessor {

        @Override
        public boolean process(int ordinal, Object item) {
            return true;
        }

        @Override
        public boolean complete(int ordinal) {
            return true;
        }

        @Override
        public boolean complete() {
            return true;
        }
    }

}
