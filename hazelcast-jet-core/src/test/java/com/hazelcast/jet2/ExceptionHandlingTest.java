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
import com.hazelcast.jet2.impl.AbstractProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ExceptionHandlingTest extends HazelcastTestSupport {

    private static TestHazelcastInstanceFactory factory;
    private JetEngine jetEngine;
    private HazelcastInstance instance;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupFactory() {
        factory = new TestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_exceptionInProcessorSupplier_thenFailJob() {
        instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        final SimpleProcessorSupplier sup = () -> {
            throw e;
        };
        Vertex faulty = new Vertex("faulty", sup);
        dag.addVertex(faulty);

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        jetEngine.newJob(dag).execute();
    }

    @Test
    public void when_exceptionInProcessorMetaSupplier_thenFailJob() {
        instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex faulty = new Vertex("faulty", (ProcessorMetaSupplier) address -> {
            throw e;
        });
        dag.addVertex(faulty);

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        jetEngine.newJob(dag).execute();
    }

    @Test
    public void when_exceptionInProcessorSupplierOnOtherNode_thenFailJob() {
        factory.newHazelcastInstance();
        instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        final int localPort = instance.getCluster().getLocalMember().getAddress().getPort();
        Vertex faulty = new Vertex("faulty", (ProcessorMetaSupplier) address -> {
            if (address.getPort() == localPort) {
                return ProcessorSupplier.of(() -> new AbstractProcessor() {
                });
            } else {
                return ProcessorSupplier.of(() -> {
                    throw e;
                });
            }
        });
        dag.addVertex(faulty);

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        jetEngine.newJob(dag).execute();
    }

    @Test
    public void when_exceptionInNonBlockingTasklet_thenFailJob() {
        instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex faulty = new Vertex("faulty", () -> new FaultyProducer(e));
        Vertex consumer = new Vertex("consumer", TestProcessors.Identity::new);
        dag.addVertex(faulty)
           .addVertex(consumer)
           .addEdge(new Edge(faulty, consumer));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        jetEngine.newJob(dag).execute();
    }

    @Test
    public void when_exceptionInBlockingTasklet_thenFailJob() {
        instance = factory.newHazelcastInstance();
        jetEngine = JetEngine.get(instance, "jetEngine");

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex faulty = new Vertex("faulty", () -> new FaultyProducer(e));
        Vertex consumer = new Vertex("consumer", TestProcessors.BlockingIdentity::new);
        dag.addVertex(faulty)
           .addVertex(consumer)
           .addEdge(new Edge(faulty, consumer));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        jetEngine.newJob(dag).execute();
    }

    private static class FaultyProducer extends AbstractProcessor {

        private final RuntimeException e;

        public FaultyProducer(RuntimeException e) {
            this.e = e;
        }

        @Override
        public boolean complete() {
            throw e;
        }
    }

}
