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
import com.hazelcast.jet.TestProcessors.FaultyProducer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.TestUtil.executeAndPeel;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ExceptionHandlingTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private JetEngine jetEngine;
    private HazelcastInstance instance;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupFactory() {
        factory = new TestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    @Repeat(1000)
    public void when_exceptionInProcessorSupplier_then_failJob() throws Throwable {
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
        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void when_exceptionInProcessorMetaSupplier_then_failJob() throws Throwable {
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
        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void when_exceptionInProcessorSupplierOnOtherNode_then_failJob() throws Throwable {
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
        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void when_exceptionInNonBlockingTasklet_then_failJob() throws Throwable {
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
        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void when_exceptionInBlockingTasklet_then_failJob() throws Throwable {
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
        executeAndPeel(jetEngine.newJob(dag));
    }
}
