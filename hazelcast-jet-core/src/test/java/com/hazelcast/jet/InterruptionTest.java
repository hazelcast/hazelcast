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
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.source.MapSource;
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.job.Job;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class InterruptionTest extends JetTestSupport {

    private static int COUNT = 10000;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() throws InterruptedException {
        factory = new TestHazelcastInstanceFactory();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testInterruptSlowApplication() throws Exception {
        int nodeCount = 2;
        HazelcastInstance instance = createCluster(factory, nodeCount);
        IMap<Integer, Integer> map = getMap(instance);
        fillMapWithInts(map, COUNT);

        DAG dag = new DAG();
        Vertex vertex = createVertex("vertex", SlowProcessor.class);
        vertex.addSource(new MapSource(map));
        dag.addVertex(vertex);
        final Job job = JetEngine.getJob(instance, "testInterrupt", dag);

        AtomicBoolean interrupted = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                SlowProcessor.latch.await();
                job.interrupt().get();
                interrupted.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        try {
            job.execute().get();
            fail("The job was not interrupted");
        } catch (ExecutionException e) {
            assertTrue(interrupted.get());
        } finally {
            job.destroy();
        }
    }

    @Test
    public void testExceptionInProcessor_whenMultipleNodes() throws Exception {
        int nodeCount = 3;
        HazelcastInstance instance = createCluster(factory, nodeCount);
        IMap<Integer, Integer> map = getMap(instance);
        fillMapWithInts(map, COUNT);

        DAG dag = new DAG();
        Vertex vertex = createVertex("vertex", ExceptionProcessor.class);
        vertex.addSource(new MapSource(map));
        dag.addVertex(vertex);
        final Job job = JetEngine.getJob(instance, "testExceptionMultipleNodes", dag);

        try {
            execute(job);
            fail("The job should not execute successfully.");
        } catch (ExecutionException e) {
            CombinedJetException ex = (CombinedJetException) e.getCause();
            List<Throwable> errors = ex.getErrors();
            assertEquals(nodeCount, errors.size());

            for (Throwable error : errors) {
                assertEquals(ExceptionProcessor.ERROR_MESSAGE, error.getMessage());
            }
        }
    }

    @Test
    public void testExceptionInProcessor_whenSingleNode() throws Exception {
        HazelcastInstance instance = createCluster(factory, 1);
        IMap<Integer, Integer> map = getMap(instance);
        fillMapWithInts(map, COUNT);

        DAG dag = new DAG();
        Vertex vertex = createVertex("vertex", ExceptionProcessor.class);
        vertex.addSource(new MapSource(map));
        dag.addVertex(vertex);
        final Job job = JetEngine.getJob(instance, "testExceptionSingleNode", dag);

        try {
            execute(job);
            fail("The job should not execute successfully.");
        } catch (ExecutionException e) {
            RuntimeException exception = (RuntimeException) e.getCause();
            assertEquals(ExceptionProcessor.ERROR_MESSAGE, exception.getCause().getMessage());
        }
    }

    public static class ExceptionProcessor implements Processor {

        private static final String ERROR_MESSAGE = "exception";

        @Override
        public boolean process(InputChunk input, OutputCollector output, String sourceName, ProcessorContext context) throws Exception {
            throw new Exception(ERROR_MESSAGE);
        }
    }

    public static class SlowProcessor implements Processor {

        private static final CountDownLatch latch = new CountDownLatch(PARALLELISM);
        private boolean started;

        @Override
        public boolean process(InputChunk input,
                               OutputCollector output,
                               String sourceName, ProcessorContext context) throws Exception {
            if (!started) {
                latch.countDown();
                started = true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            return false;
        }
    }
}
