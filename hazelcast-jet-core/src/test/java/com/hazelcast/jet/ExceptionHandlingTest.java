/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Processors.NoopProcessor;
import com.hazelcast.jet.TestProcessors.ProcessorThatFailsInComplete;
import com.hazelcast.jet.TestProcessors.ProcessorThatFailsInInit;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.TestUtil.executeAndPeel;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ExceptionHandlingTest extends JetTestSupport {

    private JetInstance instance;
    private JetTestInstanceFactory factory;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupFactory() {
        factory = new JetTestInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_exceptionInProcessorSupplier_then_failJob() throws Throwable {
        instance = factory.newMember();

        // Given
        RuntimeException e = new RuntimeException("mock error");
        final Supplier<Processor> sup = () -> {
            throw e;
        };
        DAG dag = new DAG().vertex(new Vertex("faulty", sup));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_exceptionInProcessorMetaSupplier_then_failJob() throws Throwable {
        instance = factory.newMember();

        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("faulty", (ProcessorMetaSupplier) address -> {
            throw e;
        }));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_exceptionInProcessorSupplierOnOtherNode_then_failJob() throws Throwable {
        factory.newMember();
        instance = factory.newMember();

        // Given
        RuntimeException e = new RuntimeException("mock error");
        final int localPort = instance.getCluster().getLocalMember().getAddress().getPort();

        DAG dag = new DAG().vertex(new Vertex("faulty",
                ProcessorMetaSupplier.of(
                        (Address address) -> ProcessorSupplier.of(
                                address.getPort() == localPort ? NoopProcessor::new : () -> {
                                    throw e;
                                })
                )));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_exceptionInNonBlockingTasklet_then_failJob() throws Throwable {
        instance = factory.newMember();

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex faulty = dag.newVertex("faulty", () -> new ProcessorThatFailsInComplete(e));
        Vertex consumer = dag.newVertex("consumer", TestProcessors.Identity::new);
        dag.edge(between(faulty, consumer));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_exceptionInBlockingTasklet_then_failJob() throws Throwable {
        instance = factory.newMember();

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex faulty = dag.newVertex("faulty", () -> new ProcessorThatFailsInComplete(e));
        Vertex consumer = dag.newVertex("consumer", TestProcessors.BlockingIdentity::new);
        dag.edge(between(faulty, consumer));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_exceptionInProcessorInit_then_failJob() throws Throwable {
        instance = factory.newMember();

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        dag.newVertex("faulty", () -> new ProcessorThatFailsInInit(e));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }
}
