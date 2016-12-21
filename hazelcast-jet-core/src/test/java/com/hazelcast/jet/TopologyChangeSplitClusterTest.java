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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.AbstractProducer;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class TopologyChangeSplitClusterTest extends SplitBrainTestSupport {

    private static final int PARALLELISM = 4;

    private static final int NODE_COUNT = 4;

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private Future<Void> future;


    @Override
    protected int[] brains() {
        return new int[]{NODE_COUNT / 2, NODE_COUNT / 2};
    }

    @Override
    protected void onBeforeSetup() {
        MockSupplier.completeCount.set(0);
        MockSupplier.initCount.set(0);
        MockSupplier.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * PARALLELISM);

        factory = new TestHazelcastFactory();
        Config config = new Config();
        config.getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
    }


    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        // Given
        JetEngine jetEngine = JetEngine.get(instances[0], "jetEngine");
        DAG dag = new DAG();
        Vertex test = new Vertex("test", (ProcessorMetaSupplier) address -> new MockSupplier(StuckProcessor::new))
                .parallelism(PARALLELISM);
        dag.addVertex(test);
        future = jetEngine.newJob(dag).execute();
        StuckProcessor.executionStarted.await();
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
        // When
        try {
            StuckProcessor.proceedLatch.countDown();
            future.get();
            fail("Job execution should fail");
        } catch (Exception ignored) {
        }

        // Then
        assertEquals(NODE_COUNT, MockSupplier.initCount.get());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(NODE_COUNT, MockSupplier.completeCount.get());
                assertEquals(NODE_COUNT, MockSupplier.completeErrors.size());
                for (int i = 0; i < NODE_COUNT; i++) {
                    assertInstanceOf(TopologyChangedException.class, MockSupplier.completeErrors.get(i));
                }
            }
        });
    }

    private static class MockSupplier implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger completeCount = new AtomicInteger();
        static List<Throwable> completeErrors = new CopyOnWriteArrayList<>();

        private final RuntimeException initError;
        private final SimpleProcessorSupplier supplier;

        private boolean initCalled;

        public MockSupplier(SimpleProcessorSupplier supplier) {
            this(null, supplier);
        }

        public MockSupplier(RuntimeException initError, SimpleProcessorSupplier supplier) {
            this.initError = initError;
            this.supplier = supplier;
        }

        @Override
        public void init(Context context) {
            initCalled = true;
            initCount.incrementAndGet();

            if (initError != null) {
                throw initError;
            }
        }

        @Override
        public List<Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            completeErrors.add(error);
            completeCount.incrementAndGet();
            if (!initCalled) {
                throw new IllegalStateException("Complete called without calling init()");
            }
            if (initCount.get() != NODE_COUNT) {
                throw new IllegalStateException("Complete called without init being called on all the nodes");
            }
        }
    }

    private static final class StuckProcessor extends AbstractProducer {
        static CountDownLatch executionStarted;
        static CountDownLatch proceedLatch;

        @Override
        public boolean complete() {
            executionStarted.countDown();
            try {
                proceedLatch.await();
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
            return false;
        }
    }
}
