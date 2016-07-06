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
import com.hazelcast.core.IMap;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.tap.MapSource;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class AccumulatorTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int COUNT = 10000;

    private static HazelcastInstance instance;

    @BeforeClass
    public static void setUp() throws Exception {
        instance = createCluster(NODE_COUNT);
    }

    @Test
    public void testLongAccumulator() throws IOException, ExecutionException, InterruptedException {
        IMap<Integer, Integer> map = getMap(instance);
        fillMapWithInts(map, COUNT);

        final Application application = JetEngine.getJetApplication(instance, "emptyProducerNoConsumer");
        DAG dag = new DAG();
        Vertex vertex = createVertex("accumulator", AccumulatorProcessor.class);
        vertex.addSource(new MapSource(map));
        dag.addVertex(vertex);

        application.submit(dag);
        try {
            application.execute().get();
            Accumulator accumulator = application.getAccumulators().get(AccumulatorProcessor.ACCUMULATOR_KEY);
            assertEquals(COUNT, (long) accumulator.getLocalValue());
        } finally {
            application.finalizeApplication().get();
        }
    }

    public static class AccumulatorProcessor implements ContainerProcessor {

        public static final String ACCUMULATOR_KEY = "counter";

        @Override
        public void beforeProcessing(ProcessorContext processorContext) {
            processorContext.setAccumulator(ACCUMULATOR_KEY, new LongCounter());
        }

        @Override
        public boolean process(ProducerInputStream inputStream,
                               ConsumerOutputStream outputStream,
                               String sourceName,
                               ProcessorContext processorContext) throws Exception {
            Accumulator<Long, Long> accumulator = processorContext.<Long, Long>getAccumulator(ACCUMULATOR_KEY);
            accumulator.add((long) inputStream.size());
            return true;
        }
    }
}
