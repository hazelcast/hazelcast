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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.impl.ListConsumer;
import com.hazelcast.jet2.impl.ListProducer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ForwardingTest extends HazelcastTestSupport {

    private static final List<Integer> NUMBERS = IntStream.range(0, 10).
            boxed().collect(Collectors.toList());

    private TestHazelcastInstanceFactory factory;
    private JetEngine jetEngine;

    @Before
    public void setupEngine() {
        factory = new TestHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_single() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", () -> new ListProducer(NUMBERS, 4))
                .parallelism(1);

        int parallelism = 4;
        MultiListSupplier multiListSupplier = new MultiListSupplier();
        Vertex consumer = new Vertex("consumer", multiListSupplier)
                .parallelism(parallelism);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer));

        execute(dag);

        Set<Object> combined = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {
            combined.addAll(multiListSupplier.getListAt(i));
        }
        assertEquals(new HashSet<>(NUMBERS), combined);
    }

    @Test
    public void when_broadcast() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", () -> new ListProducer(NUMBERS, 4))
                .parallelism(1);

        int parallelism = 4;
        MultiListSupplier multiListSupplier = new MultiListSupplier();
        Vertex consumer = new Vertex("consumer", multiListSupplier)
                .parallelism(parallelism);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer).broadcast());

        execute(dag);

        for (int i = 0; i < parallelism; i++) {
            assertEquals(NUMBERS, multiListSupplier.getListAt(i));
        }
    }

    @Test
    public void when_partitioned() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", () -> new ListProducer(NUMBERS, 4))
                .parallelism(1);

        int parallelism = 2;
        MultiListSupplier multiListSupplier = new MultiListSupplier();
        Vertex consumer = new Vertex("consumer", multiListSupplier)
                .parallelism(parallelism);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer).partitioned(
                        (item, numPartitions) -> (int) item % numPartitions
                ));

        execute(dag);

        assertEquals(Arrays.asList(0, 2, 4, 6, 8), multiListSupplier.getListAt(0));
        assertEquals(Arrays.asList(1, 3, 5, 7, 9), multiListSupplier.getListAt(1));

    }

    private void execute(DAG dag) {
        Job job = jetEngine.newJob(dag);
        job.execute();
    }

    private static class MultiListSupplier implements ProcessorSupplier {

        private int index;
        private ListConsumer[] consumers;

        public MultiListSupplier() {
        }

        @Override
        public void init(ProcessorSupplierContext context) {
            consumers = new ListConsumer[context.perNodeParallelism()];
            Arrays.setAll(consumers, i -> new ListConsumer());
        }

        @Override
        public Processor get() {
            return consumers[index++];
        }

        public List<Object> getListAt(int i) {
            return consumers[i].getList();
        }
    }
}
