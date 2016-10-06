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

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
public class DAGTest {

    private static final ProcessorSupplier PROCESSOR_SUPPLIER = context -> new TestProcessor();

    @Test
    public void testIterator() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.addVertex(c)
           .addVertex(b)
           .addVertex(a)
           .addEdge(new Edge(a, b))
           .addEdge(new Edge(b, c))
           .addEdge(new Edge(a, c));

        Iterator<Vertex> iterator = dag.iterator();
        assertEquals(a, iterator.next());
        assertEquals(b, iterator.next());
        assertEquals(c, iterator.next());
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void testReverseIterator() {
        DAG dag = new DAG();

        Vertex a = new Vertex("a", PROCESSOR_SUPPLIER);
        Vertex b = new Vertex("b", PROCESSOR_SUPPLIER);
        Vertex c = new Vertex("c", PROCESSOR_SUPPLIER);
        dag.addVertex(c)
           .addVertex(b)
           .addVertex(a)
           .addEdge(new Edge(a, b))
           .addEdge(new Edge(b, c))
           .addEdge(new Edge(a, c));

        Iterator<Vertex> iterator = dag.reverseIterator();
        assertEquals(c, iterator.next());
        assertEquals(b, iterator.next());
        assertEquals(a, iterator.next());
        assertEquals(false, iterator.hasNext());
    }

    private static class TestProcessor implements Processor {

        @Override
        public boolean process(String input, Object item, Outbox collector) {
            return true;
        }

        @Override
        public boolean complete(Outbox collector) {
            return true;
        }
    }

}
