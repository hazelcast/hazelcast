/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.version.Version;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.SHUTDOWN_REQUEST;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.JobRepository.JOB_RECORDS_MAP_NAME;
import static com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook.INIT_EXECUTION_OP;
import static com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook.START_EXECUTION_OP;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class TopologyChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;

    private static final int PARALLELISM = 4;

    @Parameterized.Parameter
    public boolean[] liteMemberFlags;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private int nodeCount;

    private HazelcastInstance[] instances;
    private Config config;

    @Parameterized.Parameters(name = "liteMemberFlags({index})")
    public static Collection<boolean[]> parameters() {
        return Arrays.asList(new boolean[][]{
                {false, false, false},
                {true, false, false},
                {false, true, false}
        });
    }

    @Before
    public void setup() {
        nodeCount = 0;
        for (boolean isLiteMember : liteMemberFlags) {
            if (!isLiteMember) {
                nodeCount++;
            }
        }
        TestProcessors.reset(nodeCount * PARALLELISM);

        config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(PARALLELISM);

        instances = new HazelcastInstance[NODE_COUNT];

        for (int i = 0; i < NODE_COUNT; i++) {
            Config config = smallInstanceConfig();
            config.setLiteMember(liteMemberFlags[i]);
            config.getJetConfig().setCooperativeThreadCount(PARALLELISM);

            instances[i] = createHazelcastInstance(config);
        }
    }

    @Test
    public void when_addNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        createHazelcastInstance(config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(nodeCount, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(nodeCount, MockPS.closeCount.get());
            assertThat(MockPS.receivedCloseErrors, empty());
        });
    }

    @Test
    public void when_addAndRemoveNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        HazelcastInstance instance = createHazelcastInstance(config);
        instance.shutdown();
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(nodeCount, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(nodeCount, MockPS.closeCount.get());
            assertThat(MockPS.receivedCloseErrors, empty());
        });
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecution_then_jobRestarts() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        instances[2].getLifecycleService().terminate();
        NoOutputSourceP.proceedLatch.countDown();

        job.join();

        // upon non-coordinator member leave, remaining members restart and complete the job
        final int count = nodeCount * 2 - 1;
        assertEquals(count, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(count, MockPS.closeCount.get());
            assertEquals(nodeCount, MockPS.receivedCloseErrors.size());
            for (int i = 0; i < MockPS.receivedCloseErrors.size(); i++) {
                Throwable error = MockPS.receivedCloseErrors.get(i);
                assertTrue(error instanceof CancellationException);
            }
        });
    }

    @Test
    public void when_nonCoordinatorLeaves_AutoScalingOff_SnapshottingOn_then_jobSuspends() throws Throwable {
        when_nonCoordinatorLeaves_AutoScalingOff_then_jobFailsOrSuspends(true);
    }

    @Test
    public void when_nonCoordinatorLeaves_AutoScalingOff_SnapshottingOff_then_jobFails() throws Throwable {
        when_nonCoordinatorLeaves_AutoScalingOff_then_jobFailsOrSuspends(false);
    }

    private void when_nonCoordinatorLeaves_AutoScalingOff_then_jobFailsOrSuspends(boolean snapshotted) throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));
        JobConfig config = new JobConfig();
        config.setAutoScaling(false);
        config.setProcessingGuarantee(snapshotted ? EXACTLY_ONCE : NONE);

        // When
        Job job = instances[0].getJet().newJob(dag, config);
        NoOutputSourceP.executionStarted.await();

        instances[2].getLifecycleService().terminate();
        NoOutputSourceP.proceedLatch.countDown();

        assertJobStatusEventually(job, snapshotted ? SUSPENDED : FAILED, 10);
        if (!snapshotted) {
            try {
                job.join();
                fail("join didn't fail");
            } catch (Exception e) {
                assertContains(e.getMessage(), TopologyChangedException.class.getName());
                assertContains(e.getMessage(), "[127.0.0.1]:5703=" + MemberLeftException.class.getName());
            }
        }
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecution_then_clientStillGetsJobResult() throws Throwable {
        // Given
        HazelcastInstance client = createHazelcastClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Job job = client.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        instances[2].getLifecycleService().terminate();
        NoOutputSourceP.proceedLatch.countDown();

        job.join();
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_jobCompletes() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Long jobId = null;
        try {
            Job job = instances[0].getJet().newJob(dag);
            Future<Void> future = job.getFuture();
            jobId = job.getId();
            NoOutputSourceP.executionStarted.await();

            instances[0].getLifecycleService().terminate();
            // Wait for job executions terminated in non-terminated instances
            // before proceeding. Otherwise, job executions on these members
            // may complete successfully without exception and without calling
            // Processor#close.
            for (int i = 1; i < instances.length; i++) {
                JetServiceBackend jetServiceBackend = getJetServiceBackend(instances[i]);
                jetServiceBackend.getJobExecutionService().waitAllExecutionsTerminated();
            }
            NoOutputSourceP.proceedLatch.countDown();
            future.get();
            fail();
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof HazelcastInstanceNotActiveException);
        }

        // Then
        assertNotNull(jobId);
        final long completedJobId = jobId;

        JobRepository jobRepository = getJetServiceBackend(instances[1]).getJobRepository();

        assertTrueEventually(() -> {
            JobResult jobResult = jobRepository.getJobResult(completedJobId);
            assertNotNull(jobResult);
            assertTrue(jobResult.isSuccessful());
        });

        final int count = liteMemberFlags[0] ? (2 * nodeCount) : (2 * nodeCount - 1);
        assertEquals(count, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(count, MockPS.closeCount.get());
            assertEquals(nodeCount, MockPS.receivedCloseErrors.size());
            for (Throwable error : MockPS.receivedCloseErrors) {
                assertTrue("unexpected exc: " + error, error instanceof CancellationException);
            }
        });
    }

    @Test
    public void when_coordinatorLeaves_AutoScalingOff_SnapshottingOn_then_jobSuspends() throws Throwable {
        when_coordinatorLeaves_AutoScalingOff_then_jobFailsOrSuspends(true);
    }

    @Test
    public void when_coordinatorLeaves_AutoScalingOff_SnapshottingOff_then_jobFails() throws Throwable {
        when_coordinatorLeaves_AutoScalingOff_then_jobFailsOrSuspends(false);
    }

    private void when_coordinatorLeaves_AutoScalingOff_then_jobFailsOrSuspends(boolean snapshotted) throws Throwable {
        // Given
        HazelcastInstance client = createHazelcastClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));
        JobConfig config = new JobConfig();
        config.setAutoScaling(false);
        config.setProcessingGuarantee(snapshotted ? EXACTLY_ONCE : NONE);

        // When
        Job job = client.getJet().newJob(dag, config);
        NoOutputSourceP.executionStarted.await();

        instances[0].getLifecycleService().terminate();
        NoOutputSourceP.proceedLatch.countDown();

        assertTrueEventually(() -> {
            JobStatus status = null;
            while (status == null) {
                try {
                    status = job.getStatus();
                } catch (TargetNotMemberException ignored) {
                }
            }
            assertEquals(snapshotted ? SUSPENDED : FAILED, status);
        }, 10);
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_nonCoordinatorJobSubmitterStillGetsJobResult()
            throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Job job = instances[1].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        instances[0].getLifecycleService().terminate();
        NoOutputSourceP.proceedLatch.countDown();

        // Then
        job.join();
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_clientStillGetsJobResult() throws Throwable {
        // Given
        HazelcastInstance client = createHazelcastClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, nodeCount)));

        // When
        Job job = client.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        instances[0].getLifecycleService().terminate();
        NoOutputSourceP.proceedLatch.countDown();

        // Then
        job.join();
    }

    @Test
    public void when_jobParticipantHasStaleMemberList_then_jobInitRetries() {
        // Given
        dropOperationsBetween(instances[0], instances[2],
                ClusterDataSerializerHook.F_ID, singletonList(MEMBER_INFO_UPDATE));
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount + 1)));


        // When
        createHazelcastInstance(config);
        Job job = instances[0].getJet().newJob(dag);


        // Then
        assertJobStatusEventually(job, STARTING);
        assertTrueAllTheTime(() -> assertEquals(STARTING, job.getStatus()), 5);

        resetPacketFiltersFrom(instances[0]);

        job.join();
    }

    @Test
    public void when_jobParticipantReceivesStaleInitOperation_then_jobRestarts() {
        // Given
        HazelcastInstance newInstance = createHazelcastInstance(config);
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(NODE_COUNT + 1, instance);
        }

        rejectOperationsBetween(instances[0], instances[2],
                JetInitDataSerializerHook.FACTORY_ID, singletonList(INIT_EXECUTION_OP));

        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount + 1)));

        Job job = instances[0].getJet().newJob(dag);
        JetServiceBackend jetServiceBackend = getJetServiceBackend(instances[0]);

        assertTrueEventually(() -> assertFalse(jetServiceBackend.getJobCoordinationService().getMasterContexts().isEmpty()));

        MasterContext masterContext = jetServiceBackend.getJobCoordinationService().getMasterContext(job.getId());

        assertTrueEventually(() -> {
            assertEquals(STARTING, masterContext.jobStatus());
            assertNotEquals(0, masterContext.executionId());
        });

        // When
        long executionId = masterContext.executionId();

        assertTrueEventually(() -> {
            Arrays.stream(instances)
                  .filter(instance -> !instance.getCluster().getLocalMember().isLiteMember())
                  .filter(instance -> instance != instances[2])
                  .map(JetTestSupport::getJetServiceBackend)
                  .map(service -> service.getJobExecutionService().getExecutionContext(executionId))
                  .forEach(Assert::assertNotNull);
        });

        newInstance.getLifecycleService().terminate();
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(NODE_COUNT, instance);
        }

        resetPacketFiltersFrom(instances[0]);

        // Then
        job.join();
        assertNotEquals(executionId, masterContext.executionId());
    }

    @Test
    public void when_nodeIsShuttingDownDuringInit_then_jobRestarts() {
        // Given that newInstance will have a long shutdown process
        for (HazelcastInstance instance : instances) {
            warmUpPartitions(instance);
        }

        dropOperationsBetween(instances[2], instances[0],
                PartitionDataSerializerHook.F_ID, singletonList(SHUTDOWN_REQUEST));
        rejectOperationsBetween(instances[0], instances[2],
                JetInitDataSerializerHook.FACTORY_ID, singletonList(INIT_EXECUTION_OP));

        // When a job participant starts its shutdown after the job is submitted
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount - 1)));

        Job job = instances[0].getJet().newJob(dag);

        JetServiceBackend jetServiceBackend = getJetServiceBackend(instances[0]);

        assertTrueEventually(() -> assertFalse(jetServiceBackend.getJobCoordinationService().getMasterContexts().isEmpty()));

        spawn(() -> instances[2].shutdown());

        // Then, it restarts until the shutting down node is gone
        assertJobStatusEventually(job, STARTING);
        assertTrueAllTheTime(() -> assertEquals(STARTING, job.getStatus()), 5);

        resetPacketFiltersFrom(instances[2]);

        job.join();
    }

    @Test
    public void when_nodeIsShuttingDownAfterInit_then_jobRestarts() {
        // Given that the second node has not received ExecuteOperation yet
        for (HazelcastInstance instance : instances) {
            warmUpPartitions(instance);
        }
        rejectOperationsBetween(instances[0], instances[2],
                JetInitDataSerializerHook.FACTORY_ID, singletonList(START_EXECUTION_OP));

        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount - 1)));
        Job job = instances[0].getJet().newJob(dag);
        assertJobStatusEventually(job, RUNNING);

        // When a participant shuts down during execution
        instances[2].getLifecycleService().terminate();

        // Then, the job restarts and successfully completes
        job.join();
    }

    @Test
    public void when_nodeIsNotJobParticipant_then_initFails() throws Throwable {
        final long jobId = 1;
        final long executionId = 1;
        HazelcastInstance master = instances[0];
        int memberListVersion = Accessors.getClusterService(master).getMemberListVersion();
        Set<MemberInfo> memberInfos = new HashSet<>();
        for (int i = 1; i < instances.length; i++) {
            memberInfos.add(new MemberInfo(getNode(instances[i]).getLocalMember()));
        }

        Version version = instances[0].getCluster().getLocalMember().getVersion().asVersion();
        JobRecord jobRecord = new JobRecord(version, jobId, null, "", new JobConfig(), Collections.emptySet(), null);
        instances[0].getMap(JOB_RECORDS_MAP_NAME).put(jobId, jobRecord);

        InitExecutionOperation op = new InitExecutionOperation(jobId, executionId, memberListVersion, version, memberInfos, null, false);
        Future<Object> future = Accessors.getOperationService(master)
                .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, Accessors.getAddress(master))
                .invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalArgumentException.class, e.getCause());
            assertTrue("Expected: contains 'is not in participants'\nActual: '" + e.getMessage() + "'",
                    e.getMessage().contains("is not in participants"));
        }
    }
}
