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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.Util.peel;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionLifecycleTest extends HazelcastTestSupport {

    private static final int NODE_COUNT = 2;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TestHazelcastFactory factory;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_procSupplierInit_then_completeCalled() throws Throwable {
        // Given
        HazelcastInstance instance = factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        DAG dag = new DAG();
        Vertex test = new Vertex("test", (ProcessorMetaSupplier) address -> new TestSupplier(null));
        dag.addVertex(test);

        // When
        jetEngine.newJob(dag).execute().get();

        // Then
        assertEquals(NODE_COUNT, TestSupplier.initCount.get());
        assertEquals(NODE_COUNT, TestSupplier.completeCount.get());
        assertEquals(NODE_COUNT, TestSupplier.completeErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertNull(TestSupplier.completeErrors.get(i));
        }
    }

    @Test
    @Ignore //TODO: fails because of future returns eagerly on failure
    public void when_procSupplierFailsOnInit_then_completeCalledWithError() throws Throwable {
        // Given
        HazelcastInstance instance = factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG();
        Vertex test = new Vertex("test", (ProcessorMetaSupplier) address -> new TestSupplier(e));
        dag.addVertex(test);

        // When
        try {
            jetEngine.newJob(dag).execute().get();
            fail("Job execution should fail");
        } catch (ExecutionException expected) {
            Throwable cause = peel(expected);
            assertEquals(e.getMessage(), cause.getMessage());
        }

        // Then
        assertEquals(NODE_COUNT, TestSupplier.initCount.get());
        assertEquals(NODE_COUNT, TestSupplier.completeCount.get());
        assertEquals(NODE_COUNT, TestSupplier.completeErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertEquals(e.getMessage(), TestSupplier.completeErrors.get(i).getMessage());
        }
    }

    private static class TestSupplier implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger completeCount = new AtomicInteger();
        static List<Throwable> completeErrors = new CopyOnWriteArrayList<>();

        private final RuntimeException initError;

        private boolean init;

        public TestSupplier(RuntimeException initError) {
            this.initError = initError;
        }

        @Override
        public void init(Context context) {
            init = true;
            initCount.incrementAndGet();

            if (initError != null) {
                throw initError;
            }
        }

        @Override
        public List<Processor> get(int count) {
            return Stream.generate(TestProcessors.Identity::new).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            completeErrors.add(error);
            completeCount.incrementAndGet();
            if (!init) {
                throw new IllegalStateException("Complete called without calling init()");
            }
            if (initCount.get() != NODE_COUNT) {
                throw new IllegalStateException("Complete called without init being called on all the nodes");
            }
        }
    }

}
