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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet.job.Job;
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.sink.ListSink;
import com.hazelcast.jet.dag.source.ListSource;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ConsumerProducerTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static HazelcastInstance instance;

    @BeforeClass
    public static void setUp() throws Exception {
        instance = createCluster(NODE_COUNT);
    }

    @Test
    public void testFinalization_whenEmptyProducerWithNoConsumer() throws Exception {
        final Job job = JetEngine.getJob(instance, "emptyProducerNoConsumer");
        DAG dag = new DAG();

        IList<String> sourceList = getList(instance);
        Vertex producer = createVertex("producer", FinalizingProcessor.class);
        producer.addSource(new ListSource(sourceList));

        dag.addVertex(producer);

        job.submit(dag);
        execute(job);
    }

    @Test
    public void testFinalization_whenEmptyProducerWithConsumer() throws Exception {
        final Job job = JetEngine.getJob(instance, "emptyProducerWithConsumer");
        DAG dag = new DAG();

        IList<String> sourceList = getList(instance);
        IList<String> sinkList = getList(instance);

        Vertex producer = createVertex("producer", FinalizingProcessor.class);
        producer.addSource(new ListSource(sourceList));
        Vertex consumer = createVertex("consumer", TestProcessors.Noop.class);
        consumer.addSink(new ListSink(sinkList));

        dag.addVertex(producer);
        dag.addVertex(consumer);
        dag.addEdge(new Edge("", producer, consumer));

        job.submit(dag);
        execute(job);

        assertEquals(TASK_COUNT * NODE_COUNT, sinkList.size());
    }

    @Test
    public void testArrayProducer() throws Exception {
        Job job = JetEngine.getJob(instance, "arrayProducer");
        IList<Integer[]> sourceList = getList(instance);
        IList<Integer[]> sinkList = getList(instance);
        DAG dag = new DAG();
        int count = 10;
        for (int i = 0; i < count; i++) {
            sourceList.add(new Integer[] {i});
        }
        Vertex vertex = createVertex("vertex", TestProcessors.Noop.class, 1);
        vertex.addSource(new ListSource(sourceList));
        vertex.addSink(new ListSink(sinkList));
        dag.addVertex(vertex);
        job.submit(dag);
        execute(job);
        for (int i = 0; i < count; i++) {
            assertEquals(i, (int) sinkList.get(i)[0]);
        }
    }

    public static class FinalizingProcessor
            implements ContainerProcessor<Tuple2<Integer, String>, Tuple2<Integer, String>> {
        @Override
        public boolean finalizeProcessor(ConsumerOutputStream<Tuple2<Integer, String>> outputStream,
                                         ProcessorContext processorContext) throws Exception {
            outputStream.consume(new JetTuple2<>(0, "empty"));
            return true;
        }
    }

}
