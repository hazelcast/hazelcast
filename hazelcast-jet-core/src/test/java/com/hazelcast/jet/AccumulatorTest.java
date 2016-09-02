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
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.source.MapSource;
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.processor.TaskContext;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.job.Job;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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

        DAG dag = new DAG();
        Vertex vertex = createVertex("accumulator", AccumulatorProcessor.class);
        vertex.addSource(new MapSource(map));
        dag.addVertex(vertex);

        final Job job = JetEngine.getJob(instance, "emptyProducerNoConsumer", dag);
        try {
            job.execute().get();
            Accumulator accumulator = job.getAccumulators().get(AccumulatorProcessor.ACCUMULATOR_KEY);
            assertEquals(COUNT, (long) accumulator.getLocalValue());
        } finally {
            job.destroy();
        }
    }

    public static class AccumulatorProcessor implements Processor {

        public static final String ACCUMULATOR_KEY = "counter";
        private LongCounter accumulator;

        @Override
        public void before(TaskContext processorContext) {
            this.accumulator = new LongCounter();
            processorContext.setAccumulator(ACCUMULATOR_KEY, accumulator);
        }

        @Override
        public boolean process(InputChunk input,
                               OutputCollector output,
                               String sourceName) throws Exception {
            accumulator.add((long) input.size());
            return true;
        }
    }
}
