/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.DummyStatefulP;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.PacketFiltersUtil;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CancellationException;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETING;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

// TODO this test does not test when responses are lost. There is currently no test
//   harness to simulate that.
@Category(NightlyTest.class)
public class OperationLossTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "2000");

        initialize(2, config);
    }

    @Before
    public void before() {
        TestProcessors.reset(1);
        for (JetInstance instance : instances()) {
            PacketFiltersUtil.resetPacketFiltersFrom(instance.getHazelcastInstance());
        }
    }

    @Test
    public void when_initExecutionOperationLost_then_jobRestarts() {
        when_operationLost_then_jobRestarts(JetInitDataSerializerHook.INIT_EXECUTION_OP, STARTING);
    }

    @Test
    public void when_startExecutionOperationLost_then_jobRestarts() {
        when_operationLost_then_jobRestarts(JetInitDataSerializerHook.START_EXECUTION_OP, RUNNING);
    }

    private void when_operationLost_then_jobRestarts(int operationId, JobStatus expectedStatus) {
        PacketFiltersUtil.dropOperationsFrom(instance().getHazelcastInstance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(operationId));
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", () -> new NoOutputSourceP()).localParallelism(1);
        Vertex v2 = dag.newVertex("v2", mapP(identity())).localParallelism(1);
        dag.edge(between(v1, v2).distributed());

        Job job = instance().newJob(dag);
        assertJobStatusEventually(job, expectedStatus);
        // NOT_RUNNING will occur briefly, we might miss to observe it. But restart occurs every
        // second (that's the operation heartbeat timeout) so hopefully we'll eventually succeed.
        assertJobStatusEventually(job, NOT_RUNNING);

        // now allow the job to complete normally
        PacketFiltersUtil.resetPacketFiltersFrom(instance().getHazelcastInstance());
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
    }

    @Test
    public void when_completeExecutionOperationLost_then_jobCompletes() {
        PacketFiltersUtil.dropOperationsFrom(instance().getHazelcastInstance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.COMPLETE_EXECUTION_OP));
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", () -> new DummyStatefulP()).localParallelism(1);
        Vertex v2 = dag.newVertex("v2", mapP(identity())).localParallelism(1);
        dag.edge(between(v1, v2).distributed());

        Job job = instance().newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        job.suspend();
        assertJobStatusEventually(job, COMPLETING);
        assertTrueAllTheTime(() -> assertEquals(COMPLETING, job.getStatus()), 1);
        PacketFiltersUtil.resetPacketFiltersFrom(instance().getHazelcastInstance());
        assertJobStatusEventually(job, SUSPENDED);
        job.resume();
        assertJobStatusEventually(job, RUNNING);
        cancelAndJoin(job);
    }

    @Test
    public void when_snapshotOperationLost_then_retried() {
        PacketFiltersUtil.dropOperationsFrom(instance().getHazelcastInstance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.SNAPSHOT_PHASE1_OPERATION));
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", () -> new DummyStatefulP()).localParallelism(1);
        Vertex v2 = dag.newVertex("v2", mapP(identity())).localParallelism(1);
        dag.edge(between(v1, v2).distributed());

        Job job = instance().newJob(dag, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(100));
        assertJobStatusEventually(job, RUNNING);
        JobRepository jobRepository = new JobRepository(instance());
        assertTrueEventually(() -> {
            JobExecutionRecord record = jobRepository.getJobExecutionRecord(job.getId());
            assertNotNull("null JobExecutionRecord", record);
            assertEquals("ongoingSnapshotId", 0, record.ongoingSnapshotId());
        }, 20);
        sleepSeconds(1);
        // now lift the filter and check that a snapshot is done
        logger.info("Lifting the packet filter...");
        PacketFiltersUtil.resetPacketFiltersFrom(instance().getHazelcastInstance());
        waitForFirstSnapshot(jobRepository, job.getId(), 10, false);
        cancelAndJoin(job);
    }

    @Test
    public void when_connectionDroppedWithoutMemberLeaving_then_jobRestarts() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new NoOutputSourceP()).localParallelism(1);
        Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLoggerP());
        dag.edge(between(source, sink).distributed());
        Job job = instance().newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(2, NoOutputSourceP.initCount.get()));

        Connection connection = ImdgUtil.getMemberConnection(getNodeEngineImpl(instance()), getAddress(instances()[1]));
        // When
        connection.close(null, null);
        System.out.println("connection closed");
        sleepSeconds(1);

        // Then
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        assertEquals(4, NoOutputSourceP.initCount.get());
    }

    @Test
    public void when_terminateExecutionOperationLost_then_jobTerminates() {
        PacketFiltersUtil.dropOperationsFrom(instance().getHazelcastInstance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.TERMINATE_EXECUTION_OP));
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", () -> new NoOutputSourceP()).localParallelism(1);
        Vertex v2 = dag.newVertex("v2", mapP(identity())).localParallelism(1);
        dag.edge(between(v1, v2).distributed());

        Job job = instance().newJob(dag);
        assertJobStatusEventually(job, RUNNING);
        job.cancel();
        // sleep so that the TerminateExecutionOperation is sent out, but lost
        sleepSeconds(1);
        // reset filters so that the situation can resolve
        PacketFiltersUtil.resetPacketFiltersFrom(instance().getHazelcastInstance());

        try {
            // Then
            job.join();
        } catch (CancellationException ignored) { }
    }

    @Test
    public void when_terminalSnapshotOperationLost_then_jobRestarts() {
        PacketFiltersUtil.dropOperationsFrom(instance().getHazelcastInstance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.SNAPSHOT_PHASE1_OPERATION));
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", () -> new NoOutputSourceP()).localParallelism(1);
        Vertex v2 = dag.newVertex("v2", mapP(identity())).localParallelism(1);
        dag.edge(between(v1, v2).distributed());

        Job job = instance().newJob(dag, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertJobStatusEventually(job, RUNNING, 20);
        job.restart();
        // sleep so that the SnapshotOperation is sent out, but lost
        sleepSeconds(1);
        // reset filters so that the situation can resolve
        PacketFiltersUtil.resetPacketFiltersFrom(instance().getHazelcastInstance());

        // Then
        assertTrueEventually(() -> assertEquals(4, NoOutputSourceP.initCount.get()));

        NoOutputSourceP.proceedLatch.countDown();
        job.join();
    }
}
