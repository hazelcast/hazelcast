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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.SHUTDOWN_REQUEST;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class TopologyChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;

    private static final int PARALLELISM = 4;

    @Parameterized.Parameter
    public boolean[] liteMemberFlags;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private int nodeCount;

    private JetInstance[] instances;
    private JetConfig config;

    @Parameterized.Parameters
    public static Collection<boolean[]> parameters() {
        return Arrays.asList(new boolean[][] {
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

        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(nodeCount * PARALLELISM);

        config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);

        instances = new JetInstance[NODE_COUNT];

        for (int i = 0; i < NODE_COUNT; i++) {
            JetConfig config = new JetConfig();
            config.getHazelcastConfig().setLiteMember(liteMemberFlags[i]);
            config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);

            instances[i] = createJetMember(config);
        }
    }

    @Test
    public void when_addNodeDuringExecution_then_completeSuccessfully() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        createJetMember();
        StuckProcessor.proceedLatch.countDown();
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
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();
        JetInstance instance = createJetMember();
        instance.shutdown();
        StuckProcessor.proceedLatch.countDown();
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
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[0].newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        job.join();

        // upon non-coordinator member leave, remaining members restart and complete the job
        final int count = nodeCount * 2 - 1;
        assertEquals(count, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(count, MockPS.closeCount.get());
            assertEquals(nodeCount, MockPS.receivedCloseErrors.size());
            for (int i = 0; i < MockPS.receivedCloseErrors.size(); i++) {
                Throwable error = MockPS.receivedCloseErrors.get(i);
                assertTrue(error instanceof TopologyChangedException
                        || error instanceof HazelcastInstanceNotActiveException);
            }
        });
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecutionAndNoRestartConfigured_then_jobFails() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));
        JobConfig config = new JobConfig().setAutoRestartOnMemberFailure(false);

        // When
        Job job = instances[0].newJob(dag, config);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        Throwable ex = job.getFuture().handle((r, e) -> e).get();
        assertInstanceOf(TopologyChangedException.class, ex);
    }

    @Test
    public void when_nonCoordinatorLeavesDuringExecution_then_clientStillGetsJobResult() throws Throwable {
        // Given
        JetInstance client = createJetClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = client.newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[2].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        job.join();
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_jobCompletes() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Long jobId = null;
        try {
            Job job = instances[0].newJob(dag);
            Future<Void> future = job.getFuture();
            jobId = job.getId();
            StuckProcessor.executionStarted.await();

            instances[0].getHazelcastInstance().getLifecycleService().terminate();
            StuckProcessor.proceedLatch.countDown();

            future.get();
            fail();
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof HazelcastInstanceNotActiveException);
        }

        // Then
        assertNotNull(jobId);
        final long completedJobId = jobId;

        JobRepository jobRepository = getJetService(instances[1]).getJobRepository();

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
            for (int i = 0; i < MockPS.receivedCloseErrors.size(); i++) {
                Throwable error = MockPS.receivedCloseErrors.get(i);
                assertTrue(error instanceof TopologyChangedException
                        || error instanceof HazelcastInstanceNotActiveException);
            }
        });
    }

    @Test
    public void when_coordinatorLeavesDuringExecutionAndNoRestartConfigured_then_jobFails() throws Throwable {
        // Given
        JetInstance client = createJetClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));
        JobConfig config = new JobConfig().setAutoRestartOnMemberFailure(false);

        // When
        Job job = client.newJob(dag, config);
        StuckProcessor.executionStarted.await();

        instances[0].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        Throwable ex = job.getFuture().handle((r, e) -> e).get();
        assertInstanceOf(TopologyChangedException.class, ex);
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_nonCoordinatorJobSubmitterStillGetsJobResult()
            throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = instances[1].newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[0].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        // Then
        job.join();
    }

    @Test
    public void when_coordinatorLeavesDuringExecution_then_clientStillGetsJobResult() throws Throwable {
        // Given
        JetInstance client = createJetClient();
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, nodeCount)));

        // When
        Job job = client.newJob(dag);
        StuckProcessor.executionStarted.await();

        instances[0].getHazelcastInstance().getLifecycleService().terminate();
        StuckProcessor.proceedLatch.countDown();

        // Then
        job.join();
    }

    @Test
    public void when_jobParticipantHasStaleMemberList_then_jobInitRetries() {
        // Given
        dropOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                ClusterDataSerializerHook.F_ID, singletonList(MEMBER_INFO_UPDATE));
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount + 1)));


        // When
        createJetMember(config);
        Job job = instances[0].newJob(dag);


        // Then
        assertTrueEventually(() -> assertEquals(STARTING, job.getStatus()));
        assertTrueAllTheTime(() -> assertEquals(STARTING, job.getStatus()), 5);

        resetPacketFiltersFrom(instances[0].getHazelcastInstance());

        job.join();
    }

    @Test
    public void when_jobParticipantReceivesStaleInitOperation_then_jobRestarts() {
        // Given
        JetInstance newInstance = createJetMember(config);
        for (JetInstance instance : instances) {
            assertClusterSizeEventually(NODE_COUNT + 1, instance.getHazelcastInstance());
        }

        rejectOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(INIT_EXECUTION_OP));

        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount + 1)));

        Job job = instances[0].newJob(dag);
        JetService jetService = getJetService(instances[0]);

        assertTrueEventually(() -> assertFalse(jetService.getJobCoordinationService().getMasterContexts().isEmpty()));

        MasterContext masterContext = jetService.getJobCoordinationService().getMasterContext(job.getId());

        assertTrueEventually(() -> {
            assertEquals(STARTING, masterContext.jobStatus());
            assertNotEquals(0, masterContext.getExecutionId());
        });

        // When
        long executionId = masterContext.getExecutionId();

        assertTrueEventually(() -> {
            Arrays.stream(instances)
                  .filter(instance -> !instance.getHazelcastInstance().getCluster().getLocalMember().isLiteMember())
                  .filter(instance ->  instance != instances[2])
                  .map(JetTestSupport::getJetService)
                  .map(service -> service.getJobExecutionService().getExecutionContext(executionId))
                  .forEach(Assert::assertNotNull);
        });

        newInstance.getHazelcastInstance().getLifecycleService().terminate();
        for (JetInstance instance : instances) {
            assertClusterSizeEventually(NODE_COUNT, instance.getHazelcastInstance());
        }

        resetPacketFiltersFrom(instances[0].getHazelcastInstance());


        // Then
        job.join();
        assertNotEquals(executionId, masterContext.getExecutionId());
    }

    @Test
    public void when_nodeIsShuttingDownDuringInit_then_jobRestarts() {
        // Given that newInstance will have a long shutdown process
        for (JetInstance instance : instances) {
            warmUpPartitions(instance.getHazelcastInstance());
        }

        dropOperationsBetween(instances[2].getHazelcastInstance(), instances[0].getHazelcastInstance(),
                PartitionDataSerializerHook.F_ID, singletonList(SHUTDOWN_REQUEST));
        rejectOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(INIT_EXECUTION_OP));

        // When a job participant starts its shutdown after the job is submitted
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount - 1)));

        Job job = instances[0].newJob(dag);

        JetService jetService = getJetService(instances[0]);

        assertTrueEventually(() -> assertFalse(jetService.getJobCoordinationService().getMasterContexts().isEmpty()));

        spawn(instances[2]::shutdown);

        // Then, it restarts until the shutting down node is gone
        assertTrueEventually(() -> assertEquals(STARTING, job.getStatus()));
        assertTrueAllTheTime(() -> assertEquals(STARTING, job.getStatus()), 5);

        resetPacketFiltersFrom(instances[2].getHazelcastInstance());

        job.join();
    }

    @Test
    public void when_nodeIsShuttingDownAfterInit_then_jobRestarts() {
        // Given that the second node has not received ExecuteOperation yet
        for (JetInstance instance : instances) {
            warmUpPartitions(instance.getHazelcastInstance());
        }

        rejectOperationsBetween(instances[0].getHazelcastInstance(), instances[2].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(START_EXECUTION_OP));

        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(TestProcessors.Identity::new, nodeCount - 1)));

        Job job = instances[0].newJob(dag);

        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));

        // When a participant shuts down during execution
        instances[2].shutdown();


        // Then, the job restarts and successfully completes
        job.join();
    }

    @Test
    public void when_nodeIsNotJobParticipant_then_initFails() throws Throwable {
        final long jobId = 1;
        final long executionId = 1;
        HazelcastInstance master = instances[0].getHazelcastInstance();
        int memberListVersion = getClusterService(master).getMemberListVersion();
        Set<MemberInfo> memberInfos = new HashSet<>();
        for (int i = 1; i < instances.length; i++) {
            memberInfos.add(new MemberInfo(getNode(instances[i].getHazelcastInstance()).getLocalMember()));
        }

        JobRecord jobRecord = new JobRecord(jobId, 0, null, new JobConfig(), 2);
        instances[0].getMap(JOB_RECORDS_MAP_NAME).put(jobId, jobRecord);

        InitExecutionOperation op = new InitExecutionOperation(jobId, executionId, memberListVersion, memberInfos, null);
        Future<Object> future = getOperationService(master)
                .createInvocationBuilder(JetService.SERVICE_NAME, op, getAddress(master))
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
