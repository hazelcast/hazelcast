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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPMS;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.NotSerializableException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.assertExceptionInCauses;
import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.impl.JobClassLoaderService.JobPhase.COORDINATOR;
import static com.hazelcast.jet.impl.JobExecutionRecord.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_FORCEFUL;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.test.PacketFiltersUtil.delayOperationsFrom;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutionLifecycleTest extends SimpleTestInClusterSupport {

    private static final int MEMBER_COUNT = 2;
    private static final Throwable MOCK_ERROR = new AssertionError("mock error");

    @Parameter
    public boolean useLightJob;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private int parallelism;

    @Parameters(name = "useLightJob={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(MEMBER_COUNT, null, null);
    }

    @Before
    public void before() {
        parallelism = instance().getConfig().getJetConfig().getCooperativeThreadCount();
        TestProcessors.reset(MEMBER_COUNT * parallelism);
    }

    @Test
    public void when_jobCompletesSuccessfully_then_closeCalled() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT))));

        // When
        Job job = newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_processorCompletesSuccessfully_then_closeCalledImmediately() {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", MockP::new);
        Vertex v2 = dag.newVertex("v2", () -> new NoOutputSourceP());
        dag.edge(between(v1, v2));

        Job job = newJob(dag);
        assertTrueEventually(this::assertPClosedWithoutError);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertJobSucceeded(job);
    }

    @Test
    public void when_pmsInitThrows_then_jobFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT)).setInitError(MOCK_ERROR)));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_oneOfTwoJobsFails_then_theOtherContinues() throws Exception {
        // Given
        DAG dagFaulty = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setCompleteError(MOCK_ERROR), MEMBER_COUNT))));
        DAG dagGood = new DAG();
        dagGood.newVertex("good", () -> new NoOutputSourceP());

        // When
        Job jobGood = newJob(dagGood);
        NoOutputSourceP.executionStarted.await();
        runJobExpectFailure(dagFaulty, false);

        // Then
        assertTrueAllTheTime(() -> assertFalse(jobGood.getFuture().isDone()), 2);
        NoOutputSourceP.proceedLatch.countDown();
        jobGood.join();
    }

    @Test
    public void when_pmsGetThrows_then_jobFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT)).setGetError(MOCK_ERROR)));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_pmsCloseThrows_then_jobSucceeds() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT)).setCloseError(MOCK_ERROR)));

        // When
        Job job = newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_psInitThrows_then_jobFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT).setInitError(MOCK_ERROR))));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_psGetThrows_then_jobFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT).setGetError(MOCK_ERROR))));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_psGetOnOtherNodeThrows_then_jobFails() {
        // Given
        final int localPort = instance().getCluster().getLocalMember().getAddress().getPort();

        DAG dag = new DAG().vertex(new Vertex("faulty",
                ProcessorMetaSupplier.of(
                        (Address address) -> ProcessorSupplier.of(
                                address.getPort() == localPort ? noopP() : () -> {
                                    throw sneakyThrow(MOCK_ERROR);
                                })
                )));

        // When
        try {
            executeAndPeel(newJob(dag));
        } catch (Throwable caught) {
            // Then
            assertExceptionInCauses(MOCK_ERROR, caught);
        }
    }

    @Test
    public void when_psCloseThrows_then_jobSucceeds() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("faulty",
                new MockPMS(() -> new MockPS(MockP::new, MEMBER_COUNT).setCloseError(MOCK_ERROR))));

        // When
        Job job = newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_processorInitThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setInitError(MOCK_ERROR), MEMBER_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPClosedWithError();
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_processorProcessThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setProcessError(MOCK_ERROR), MEMBER_COUNT)));
        dag.edge(between(source, process));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPClosedWithError();
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_processorCooperativeCompleteThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setCompleteError(MOCK_ERROR), MEMBER_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPClosedWithError();
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_processorNonCooperativeCompleteThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty", new MockPMS(() -> new MockPS(() ->
                new MockP().nonCooperative().setCompleteError(MOCK_ERROR), MEMBER_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, false);

        // Then
        assertPClosedWithError();
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_processorOnSnapshotCompleteThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty", new MockPMS(() -> new MockPS(() ->
                new MockP().nonCooperative().streaming().setOnSnapshotCompleteError(MOCK_ERROR), MEMBER_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, true);
        assertTrue("onSnapshotCompleted not called", MockP.onSnapshotCompletedCalled);

        // Then
        assertPClosedWithError();
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_processorSaveToSnapshotThrows_then_failJob() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty", new MockPMS(() -> new MockPS(() ->
                new MockP().nonCooperative().streaming().setSaveToSnapshotError(MOCK_ERROR), MEMBER_COUNT)));

        // When
        Job job = runJobExpectFailure(dag, true);
        assertTrue("saveToSnapshot not called", MockP.saveToSnapshotCalled);

        // Then
        assertPClosedWithError();
        assertPsClosedWithError();
        assertPmsClosedWithError();
        assertJobFailed(job, MOCK_ERROR);
    }

    @Test
    public void when_processorCloseThrows_then_jobSucceeds() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setCloseError(MOCK_ERROR), MEMBER_COUNT)));

        // When
        Job job = newJob(dag);
        job.join();

        // Then
        assertPClosedWithoutError();
        assertPsClosedWithoutError();
        assertPmsClosedWithoutError();
        assertJobSucceeded(job);
    }

    @Test
    public void when_executionCancelled_then_jobCompletedWithCancellationException() throws Exception {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(NoOutputSourceP::new, MEMBER_COUNT))));

        // When
        Job job = newJob(dag);
        NoOutputSourceP.executionStarted.await();
        cancelAndJoin(job);
        assertTrueEventually(() -> {
            assertJobFailed(job, new CancellationException());
            assertPsClosedWithError();
            assertPmsClosedWithError();
        });
    }

    @Test
    public void when_executionCancelledBeforeStart_then_jobFutureIsCancelledOnExecute() {
        // not applicable to light jobs - we hack around with ExecutionContext
        assumeFalse(useLightJob);

        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance());
        Address localAddress = nodeEngineImpl.getThisAddress();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngineImpl.getClusterService();
        MembersView membersView = clusterService.getMembershipManager().getMembersView();
        int memberListVersion = membersView.getVersion();

        JetServiceBackend jetServiceBackend = getJetServiceBackend(instance());
        long jobId = 0;
        long executionId = 1;
        JobConfig jobConfig = new JobConfig();
        final Map<MemberInfo, ExecutionPlan> executionPlans =
                ExecutionPlanBuilder.createExecutionPlans(nodeEngineImpl, membersView.getMembers(), dag,
                        jobId, executionId, jobConfig, NO_SNAPSHOT, false, null);
        ExecutionPlan executionPlan = executionPlans.get(membersView.getMember(localAddress));

        jetServiceBackend.getJobClassLoaderService().getOrCreateClassLoader(jobConfig, jobId, COORDINATOR);
        Set<MemberInfo> participants = new HashSet<>(membersView.getMembers());
        jetServiceBackend.getJobExecutionService().initExecution(
                jobId, executionId, localAddress, memberListVersion, participants, executionPlan
        );

        ExecutionContext executionContext = jetServiceBackend.getJobExecutionService().getExecutionContext(executionId);
        executionContext.terminateExecution(null);

        // When
        CompletableFuture<Void> future = executionContext.beginExecution(jetServiceBackend.getTaskletExecutionService());

        // Then
        expectedException.expect(CancellationException.class);
        future.join();
    }

    @Test
    public void when_executionCancelledBeforeStart_then_jobIsCancelled() {
        // not applicable to light jobs - light jobs don't use the StartExecutionOperation
        assumeFalse(useLightJob);

        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));

        delayOperationsFrom(instance(), JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.START_EXECUTION_OP));

        Job job = instance().getJet().newJob(dag);
        assertJobStatusEventually(job, RUNNING); // RUNNING status is set on master before sending the StartOp
        job.cancel();
        assertThatThrownBy(() -> job.join())
                .isInstanceOf(CancellationException.class);
    }

    @Test
    public void when_jobCancelled_then_psCloseNotCalledBeforeTaskletsDone() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                new MockPS(() -> new NoOutputSourceP(10_000), MEMBER_COUNT)));

        Job job = newJob(dag);
        assertOpenEventually(NoOutputSourceP.executionStarted);

        // When
        job.cancel();

        assertTrueAllTheTime(() -> {
            // Then
            assertEquals("PS.close called before execution finished", 0, MockPS.closeCount.get());
        }, 1);

        NoOutputSourceP.proceedLatch.countDown();

        expectedException.expect(CancellationException.class);
        job.join();

        assertEquals("PS.close not called after execution finished", MEMBER_COUNT, MockPS.closeCount.get());
    }

    @Test
    public void when_deserializationOnMembersFails_then_jobSubmissionFails__member() throws Throwable {
        when_deserializationOnMembersFails_then_jobSubmissionFails(instance());
    }

    @Test
    public void when_deserializationOnMembersFails_then_jobSubmissionFails__client() throws Throwable {
        when_deserializationOnMembersFails_then_jobSubmissionFails(client());
    }

    private void when_deserializationOnMembersFails_then_jobSubmissionFails(HazelcastInstance instance) throws Throwable {
        // Given
        DAG dag = new DAG();
        // this is designed to fail when member deserializes the execution plan while executing
        // the InitOperation
        dag.newVertex("faulty", (ProcessorMetaSupplier) addresses -> address -> new NotDeserializableProcessorSupplier());

        // Then
        // we can't assert the exception class. Sometimes the HazelcastSerializationException is wrapped
        // in JetException and sometimes it's not, depending on whether the job managed to write JobResult or not.
        expectedException.expectMessage("java.lang.ClassNotFoundException: fake.Class");

        // When
        executeAndPeel(newJob(instance, dag, null));
    }

    @Test
    public void when_deserializationOnMasterFails_then_jobSubmissionFails_member() {
        // Not applicable for light jobs - light jobs are always submitted to local member, without serializing the DAG
        assumeFalse(useLightJob);

        when_deserializationOnMasterFails_then_jobSubmissionFails(instance());
    }

    @Test
    public void when_deserializationOnMasterFails_then_jobSubmissionFails_client() {
        when_deserializationOnMasterFails_then_jobSubmissionFails(client());
    }

    private void when_deserializationOnMasterFails_then_jobSubmissionFails(HazelcastInstance instance) {
        DAG dag = new DAG();
        // this is designed to fail when the master member deserializes the DAG
        dag.newVertex("faulty", new NotDeserializableProcessorMetaSupplier());

        assertThatThrownBy(() -> newJob(instance, dag, null).join())
                .hasRootCauseInstanceOf(ClassNotFoundException.class)
                .hasMessageContaining("fake.Class");
    }

    @Test
    public void when_serializationOnMasterFails_then_jobFails() {
        DAG dag = new DAG();
        // When
        dag.newVertex("v", new PmsProducingNonSerializablePs());
        Job job = newJob(dag);

        // Then
        assertThatThrownBy(() -> job.join())
                .hasMessageContaining(useLightJob
                        // `checkSerializable` isn't used for light jobs
                        ? "Failed to serialize 'com.hazelcast.jet.impl.execution.init.ExecutionPlan'"
                        : "ProcessorSupplier in vertex 'v'\" must be serializable");
    }

    @Test
    public void when_clientJoinBeforeAndAfterComplete_then_exceptionEquals() {
        // not applicable to light jobs - we can't connect to light jobs after they complete
        assumeFalse(useLightJob);

        DAG dag = new DAG();
        Vertex noop = dag.newVertex("noop", (SupplierEx<Processor>) NoOutputSourceP::new)
                         .localParallelism(1);
        Vertex faulty = dag.newVertex("faulty", () -> new MockP().setCompleteError(MOCK_ERROR))
                           .localParallelism(1);
        dag.edge(between(noop, faulty));

        Job job = newJob(client(), dag, null);
        assertJobStatusEventually(job, RUNNING);
        NoOutputSourceP.proceedLatch.countDown();
        Throwable excBeforeComplete;
        Throwable excAfterComplete;
        try {
            job.join();
            throw new AssertionError("should have failed");
        } catch (Exception e) {
            excBeforeComplete = e;
        }

        // create a new client that will join the job after completion
        HazelcastInstance client2 = factory().newHazelcastClient();
        Job job2 = client2.getJet().getJob(job.getId());
        try {
            job2.join();
            throw new AssertionError("should have failed");
        } catch (Exception e) {
            excAfterComplete = e;
        }

        logger.info("exception before completion", excBeforeComplete);
        logger.info("exception after completion", excAfterComplete);

        // Then
        assertInstanceOf(CompletionException.class, excBeforeComplete);
        assertInstanceOf(CompletionException.class, excAfterComplete);

        Throwable causeBefore = excBeforeComplete.getCause();
        Throwable causeAfter = excAfterComplete.getCause();

        assertEquals(causeBefore.getClass(), causeAfter.getClass());
        assertContains(causeAfter.getMessage(), causeBefore.getMessage());
    }

    @Test
    public void when_job_withNoSnapshots_completed_then_noSnapshotMapsLeft() {
        HazelcastInstance instance = createHazelcastInstance();
        DAG dag = new DAG();
        dag.newVertex("noop", Processors.noopP());
        newJob(instance, dag, null).join();
        Collection<DistributedObject> objects = instance.getDistributedObjects();
        long snapshotMaps = objects.stream()
                                   .filter(obj -> obj instanceof IMap)
                                   .filter(obj -> obj.getName().contains("snapshots.data"))
                                   .count();

        assertEquals(0, snapshotMaps);
    }

    @Test
    public void when_dataSerializable_processorSupplier_notSerializable_then_jobFails() {
        DAG dag = new DAG();
        dag.newVertex("v", ProcessorMetaSupplier.of(
                (FunctionEx<? super Address, ? extends ProcessorSupplier>)
                        address -> new NotSerializable_DataSerializable_ProcessorSupplier()));

        Job job = newJob(dag);
        Exception e = assertThrows(Exception.class, () -> job.join());
        assertContains(e.getMessage(), "Failed to serialize");
    }

    @Test
    public void test_jobStatusCompleting() {
        assumeFalse(useLightJob); // light jobs cannot be restarted

        DAG dag = new DAG();
        dag.newVertex("v", () -> new TestProcessors.MockP().streaming());
        Job job = newJob(dag);

        long endTime = System.nanoTime() + SECONDS.toNanos(2);
        while (System.nanoTime() < endTime) {
            //noinspection StatementWithEmptyBody
            while (job.getStatus() != RUNNING) { }

            // test for https://github.com/hazelcast/hazelcast-jet/pull/2507
            // we try to restart as soon as the job status is RUNNING. If the `RUNNING` status is
            // reported incorrectly, the restart will fail with "Cannot RESTART_GRACEFUL,
            // job status is XXX, should be RUNNING"
            job.restart();
        }
    }

    @Test
    public void when_memberLeft_then_jobFails() {
        // Test for https://github.com/hazelcast/hazelcast/issues/18844

        // This test doesn't use the shared cluster because it shuts down one of the members
        HazelcastInstance inst1 = createHazelcastInstance();
        HazelcastInstance inst2 = createHazelcastInstance();

        DAG dag = new DAG();
        dag.newVertex("v", () -> new MockP().streaming());

        Job job = inst1.getJet().newLightJob(dag);
        assertJobRunningEventually(inst1, job, null);
        inst2.getLifecycleService().terminate();

        assertThatThrownBy(() -> job.join())
                .hasRootCauseInstanceOf(MemberLeftException.class);
    }

    public static class NotSerializable_DataSerializable_ProcessorSupplier implements ProcessorSupplier, DataSerializable {
        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return nCopies(count, Processors.noopP().get());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // Object is not serializable
            out.writeObject(new Object());
        }

        @Override
        public void readData(ObjectDataInput in) {
            fail();
        }
    }

    private Job newJob(DAG dag) {
        return newJob(instance(), dag, null);
    }

    private Job newJob(HazelcastInstance instance, DAG dag, JobConfig config) {
        if (config != null) {
            assumeFalse(useLightJob); // light jobs don't support config
            return instance.getJet().newJob(dag, config);
        } else {
            return useLightJob ? instance.getJet().newLightJob(dag) : instance.getJet().newJob(dag);
        }
    }

    private Job runJobExpectFailure(@Nonnull DAG dag, boolean snapshotting) {
        Job job = null;
        assumeTrue(!snapshotting || !useLightJob); // snapshotting not supported for light jobs
        try {
            JobConfig config = null;
            if (snapshotting) {
                config = new JobConfig()
                        .setProcessingGuarantee(EXACTLY_ONCE)
                        .setSnapshotIntervalMillis(100);
            }
            job = newJob(instance(), dag, config);
            job.join();
            fail("Job execution should have failed");
        } catch (Exception actual) {
            String causeString = peel(actual).toString();
            if (causeString == null
                    || !(causeString.contains(MOCK_ERROR.toString()) || causeString.contains(CancellationException.class.getName()))) {
                throw new AssertionError(format("'%s' didn't contain expected '%s'", causeString, MOCK_ERROR.getMessage()), actual);
            }
        }
        return job;
    }

    private void assertPmsClosedWithoutError() {
        assertTrue("initCalled", MockPMS.initCalled.get());
        assertTrue("closeCalled", MockPMS.closeCalled.get());
        assertNull("receivedCloseError", MockPMS.receivedCloseError.get());
    }

    private void assertPmsClosedWithError() {
        assertTrue("init not called", MockPMS.initCalled.get());
        assertTrue("close not called", MockPMS.closeCalled.get());
        assertOneOfExceptionsInCauses(MockPMS.receivedCloseError.get(),
                MOCK_ERROR,
                new CancellationException(),
                new JobTerminateRequestedException(CANCEL_FORCEFUL));
    }

    private void assertPsClosedWithoutError() {
        assertEquals(MEMBER_COUNT, MockPS.initCount.get());
        assertEquals(MEMBER_COUNT, MockPS.closeCount.get());
        assertEquals(0, MockPS.receivedCloseErrors.size());
    }

    private void assertPsClosedWithError() {
        assertEquals(MEMBER_COUNT, MockPS.initCount.get());
        // with light jobs the init can be called on not all the members - the execution on one member
        // can be cancelled due to the failure on the other member before it was initialized.
        int minCount = useLightJob ? 1 : MEMBER_COUNT;
        assertBetween("close count", MockPS.closeCount.get(), minCount, MEMBER_COUNT);
        assertBetween("received close errors", MockPS.receivedCloseErrors.size(), minCount, MEMBER_COUNT);

        for (int i = 0; i < MockPS.receivedCloseErrors.size(); i++) {
            assertOneOfExceptionsInCauses(MockPS.receivedCloseErrors.get(i),
                    MOCK_ERROR,
                    new CancellationException(),
                    new JobTerminateRequestedException(CANCEL_FORCEFUL));
        }
    }

    private void assertPClosedWithoutError() {
        assertEquals(MEMBER_COUNT * parallelism, MockP.initCount.get());
        assertEquals(MEMBER_COUNT * parallelism, MockP.closeCount.get());
    }

    private void assertPClosedWithError() {
        // with light jobs the init can be called on not all the members - the execution on one member
        // can be cancelled due to the failure on the other member before it was initialized.
        int minCount = useLightJob ? parallelism : MEMBER_COUNT * parallelism;
        assertBetween("close count", MockP.closeCount.get(), minCount, MEMBER_COUNT * parallelism);
    }

    private void assertOneOfExceptionsInCauses(Throwable caught, Throwable... expected) {
        for (Throwable exp : expected) {
            try {
                assertExceptionInCauses(exp, caught);
                return;
            } catch (AssertionError ignored) { }
        }
        throw new AssertionError("None of expected exceptions caught. Expected: " + Arrays.toString(expected), caught);
    }

    private void assertJobSucceeded(Job job) {
        assertTrue(job.getFuture().isDone());
        job.join();
        if (!job.isLightJob()) {
            JobResult jobResult = getJobResult(job);
            assertTrue(jobResult.isSuccessful());
            assertNull(jobResult.getFailureText());
        }
    }

    private void assertJobFailed(Job job, Throwable expected) {
        assertTrue(job.getFuture().isDone());
        try {
            job.join();
            fail("job didn't fail");
        } catch (Throwable caught) {
            assertExceptionInCauses(expected, caught);
        }
        if (!job.isLightJob()) {
            Job normalJob = job;
            JobResult jobResult = getJobResult(normalJob);
            assertFalse("jobResult.isSuccessful", jobResult.isSuccessful());
            assertNotNull(jobResult.getFailureText());
            assertContains(jobResult.getFailureText(), expected.toString());
            assertEquals("jobStatus", JobStatus.FAILED, normalJob.getStatus());
        }
    }

    private JobResult getJobResult(Job job) {
        JetServiceBackend jetServiceBackend = getJetServiceBackend(instance());
        assertNull(jetServiceBackend.getJobRepository().getJobRecord(job.getId()));
        JobResult jobResult = jetServiceBackend.getJobRepository().getJobResult(job.getId());
        assertNotNull(jobResult);
        return jobResult;
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

    private static class PmsProducingNonSerializablePs implements ProcessorMetaSupplier {
        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return count -> new ProcessorSupplier() {

                @Nonnull
                @Override
                public Collection<? extends Processor> get(int count) {
                    throw new UnsupportedOperationException("should not get here");
                }

                private void writeObject(java.io.ObjectOutputStream stream) throws Exception {
                    // simulate serialization failure
                    throw new NotSerializableException(getClass().getName());
                }
            };
        }
    }
}
