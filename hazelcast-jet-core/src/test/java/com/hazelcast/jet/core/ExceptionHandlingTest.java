/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.TestProcessors.ProcessorThatFailsInComplete;
import com.hazelcast.jet.core.TestProcessors.ProcessorThatFailsInInit;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.core.processor.Processors.noopP;

@RunWith(HazelcastParallelClassRunner.class)
public class ExceptionHandlingTest extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance;

    @Test
    public void when_exceptionInProcessorSupplier_then_failJob() throws Throwable {
        instance = createJetMember();

        // Given
        RuntimeException e = new RuntimeException("mock error");
        final DistributedSupplier<Processor> sup = () -> {
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
        instance = createJetMember();

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
        createJetMember();
        instance = createJetMember();

        // Given
        RuntimeException e = new RuntimeException("mock error");
        final int localPort = instance.getCluster().getLocalMember().getAddress().getPort();

        DAG dag = new DAG().vertex(new Vertex("faulty",
                ProcessorMetaSupplier.of(
                        (Address address) -> ProcessorSupplier.of(
                                address.getPort() == localPort ? noopP() : () -> {
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
        instance = createJetMember();

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
        instance = createJetMember();

        // Given
        DAG dag = new DAG();
        RuntimeException e = new RuntimeException("mock error");
        Vertex faulty = dag.newVertex("faulty", () -> new ProcessorThatFailsInComplete(e));
        Vertex consumer = dag.newVertex("consumer", noopP());
        dag.edge(between(faulty, consumer));

        // Then
        expectedException.expect(e.getClass());
        expectedException.expectMessage(e.getMessage());

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_exceptionInProcessorInit_then_failJob() throws Throwable {
        instance = createJetMember();

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

    @Test
    public void when_deserializationOnMembersFails_then_jobSubmissionFails__member() throws Throwable {
        when_deserializationOnMembersFails_then_jobSubmissionFails(createJetMember());
    }

    @Test
    public void when_deserializationOnMembersFails_then_jobSubmissionFails__client() throws Throwable {
        createJetMember();
        when_deserializationOnMembersFails_then_jobSubmissionFails(createJetClient());
    }

    public void when_deserializationOnMembersFails_then_jobSubmissionFails(JetInstance instance) throws Throwable {
        // Given
        DAG dag = new DAG();
        // this is designed to fail when member deserializes the execution plan while executing
        // the InitOperation
        dag.newVertex("faulty", (ProcessorMetaSupplier) addresses -> address -> new NotDeserializableProcessorSupplier());

        // Then
        expectedException.expect(HazelcastSerializationException.class);
        expectedException.expectMessage("fake.Class");

        // When
        executeAndPeel(instance.newJob(dag));
    }

    @Test
    public void when_deserializationOnMasterFails_then_jobSubmissionFails_member() throws Throwable {
        when_deserializationOnMasterFails_then_jobSubmissionFails(createJetMember());
    }

    @Test
    public void when_deserializationOnMasterFails_then_jobSubmissionFails_client() throws Throwable {
        createJetMember();
        when_deserializationOnMasterFails_then_jobSubmissionFails(createJetClient());
    }

    public void when_deserializationOnMasterFails_then_jobSubmissionFails(JetInstance instance) throws Throwable {
        // Given
        DAG dag = new DAG();
        // this is designed to fail when the master member deserializes the DAG
        dag.newVertex("faulty", new NotDeserializableProcessorMetaSupplier());

        // Then
        expectedException.expect(HazelcastSerializationException.class);
        expectedException.expectMessage("fake.Class");

        // When
        executeAndPeel(instance.newJob(dag));
    }

    private static class NotDeserializableProcessorSupplier implements ProcessorSupplier {
        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            throw new UnsupportedOperationException("should not get here");
        }

        private void readObject(java.io.ObjectInputStream stream) throws ClassNotFoundException {
            // simulate deserialization failure
            throw new ClassNotFoundException("fake.Class");
        }
    }

    private static class NotDeserializableProcessorMetaSupplier implements ProcessorMetaSupplier {
        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            throw new UnsupportedOperationException("should not get here");
        }

        private void readObject(java.io.ObjectInputStream stream) throws ClassNotFoundException {
            // simulate deserialization failure
            throw new ClassNotFoundException("fake.Class");
        }
    }
}
