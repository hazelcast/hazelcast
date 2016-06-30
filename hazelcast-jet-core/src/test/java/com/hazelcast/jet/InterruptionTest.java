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
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.tap.MapSource;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.processor.ContainerProcessor;
import com.hazelcast.jet.processor.ProcessorDescriptor;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InterruptionTest extends JetTestSupport {

    public static final int TASK_COUNT = 4;
    public static final int NODE_COUNT = 2;
    public static final int COUNT = 1000;
    private static HazelcastInstance instance;

    @BeforeClass
    public static void setUp() throws Exception {
        instance = createCluster(NODE_COUNT);
    }

    @Test
    public void tesInterruptSlowApplication() throws Exception {
        final Application application = JetEngine.getJetApplication(instance, "testInterrupt");
        IMap<Integer, Integer> map = getMap(instance);
        fillMapWithInts(map, 100);

        DAG dag = new DAG();
        Vertex vertex = new Vertex("vertex", ProcessorDescriptor.builder(SlowProcessor.class)
                .withTaskCount(TASK_COUNT).build());
        vertex.addSource(new MapSource(map));
        dag.addVertex(vertex);
        application.submit(dag);

        AtomicBoolean interrupted = new AtomicBoolean(false);
        new Thread(() -> {
            try {
                SlowProcessor.latch.await();
                application.interrupt().get();
                interrupted.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        try {
            application.execute().get();
            fail("The application was not interrupted");
        } catch (ExecutionException e) {
            assertTrue(interrupted.get());
            //TODO: messy chained exceptions
        } finally {
            application.finalizeApplication().get();
        }
    }

    public static class SlowProcessor implements ContainerProcessor {

        private static final CountDownLatch latch = new CountDownLatch(TASK_COUNT * NODE_COUNT);
        private boolean started;

        @Override
        public boolean process(ProducerInputStream inputStream,
                               ConsumerOutputStream outputStream,
                               String sourceName, ProcessorContext processorContext) throws Exception {
            if (!started) {
                latch.countDown();
                started = true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            return false;
        }
    }
}
