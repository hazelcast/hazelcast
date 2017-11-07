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

package com.hazelcast.jet.core;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.FailingOnCompletePMS;
import com.hazelcast.jet.core.TestProcessors.FailingOnCompletePS;
import com.hazelcast.jet.core.TestProcessors.Identity;
import com.hazelcast.jet.core.TestProcessors.MockPMS;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.ProcessorThatFailsInComplete;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.core.TestUtil.assertExceptionInCauses;
import static com.hazelcast.jet.core.TestUtil.getJetService;
import static com.hazelcast.jet.impl.execution.SnapshotContext.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionLifecycleTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 4;

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance;
    private JetTestInstanceFactory factory;


    @Before
    public void setup() {
        MockPMS.initCalled.set(false);
        MockPMS.completeCalled.set(false);
        MockPMS.completeError.set(null);

        MockPS.completeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        instance = factory.newMember(config);
        factory.newMember(config);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void when_jobCompleted_then_completeCalled() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPMS(() -> new MockPS(Identity::new, NODE_COUNT))));

        // When
        Job job = instance.newJob(dag);
        job.join();

        // Then
        assertPsCompletedWithoutError();
        assertPmsCompleted();
        assertJobSucceeded(job);
    }

    @Test
    public void when_pmsInitThrows_then_jobCompletedWithError() throws Throwable {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("test", new MockPMS(
                e, () -> new MockPS(Identity::new, NODE_COUNT)))
        );

        // When
        Job job = null;
        try {
            job = instance.newJob(dag);
            job.join();
            fail("Job execution should fail");
        } catch (Exception expected) {
            Throwable cause = peel(expected);
            assertEquals(e.getMessage(), cause.getMessage());
        }

        // Then
        assertPmsCompletedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_psInitThrows_then_jobCompletedWithError() throws Throwable {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        DAG dag = new DAG().vertex(new Vertex("test", new MockPMS(
                () -> new MockPS(e, Identity::new, NODE_COUNT))
        ));

        // When
        Job job = null;
        try {
            job = instance.newJob(dag);
            job.join();
            fail("Job execution should fail");
        } catch (Exception expected) {
            Throwable cause = peel(expected);
            assertEquals(e.getMessage(), cause.getMessage());
        }

        // Then
        assertPsCompletedWithError(e);
        assertPmsCompletedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_executionFails_then_jobCompletedWithError() throws Throwable {
        // Given
        RuntimeException e = new RuntimeException("mock error");
        String vertexName = "test";
        DAG dag = new DAG().vertex(new Vertex(vertexName, new MockPMS(
                () -> new MockPS(() -> new ProcessorThatFailsInComplete(e), NODE_COUNT)
        )));
        // When
        Job job = null;
        try {
            job = instance.newJob(dag);
            job.join();
            fail("Job execution should fail");
        } catch (Exception expected) {
            assertExceptionInCauses(e, expected);
            String expectedMessage = "vertex=" + vertexName + "";
            assertTrue("Error message does not contain vertex name.\nExpected: " + expectedMessage
                            + "\nActual: " + expected,
                    expected.getMessage() != null && expected.getMessage().contains(expectedMessage));
        }

        // Then
        assertPsCompletedWithError(e);
        assertPmsCompletedWithError(e);
        assertJobFailed(job, e);
    }

    @Test
    public void when_executionCancelled_then_jobCompletedWithCancellationException() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPMS(
                () -> new MockPS(StuckProcessor::new, NODE_COUNT)
        )));

        // When
        Job job = instance.newJob(dag);
        try {
            StuckProcessor.executionStarted.await();
            job.cancel();
            job.join();
            fail("Job execution should fail");
        } catch (CancellationException ignored) {
        }

        assertTrueEventually(() -> {
            assertJobFailed(job, new CancellationException());
            assertPsCompletedWithError(new CancellationException());
            assertPmsCompletedWithError(new CancellationException());
        });
    }

    @Test
    public void when_executionCancelledBeforeStart_then_jobFutureIsCancelledOnExecute() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance.getHazelcastInstance());
        Address localAddress = nodeEngineImpl.getThisAddress();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngineImpl.getClusterService();
        MembersView membersView = clusterService.getMembershipManager().getMembersView();
        int memberListVersion = membersView.getVersion();

        JetService jetService = getJetService(instance);
        final Map<MemberInfo, ExecutionPlan> executionPlans =
                ExecutionPlanBuilder.createExecutionPlans(nodeEngineImpl, membersView, dag, new JobConfig(),
                        NO_SNAPSHOT);
        ExecutionPlan executionPlan = executionPlans.get(membersView.getMember(localAddress));
        long jobId = 0;
        long executionId = 1;

        jetService.initExecution(jobId, executionId, localAddress, memberListVersion,
                new HashSet<>(membersView.getMembers()), executionPlan);

        ExecutionContext executionContext = jetService.getJobExecutionService().getExecutionContext(executionId);
        executionContext.cancel();

        // When
        final AtomicReference<Object> result = new AtomicReference<>();

        executionContext.execute(stage ->
                stage.whenComplete((aVoid, throwable) -> result.compareAndSet(null, throwable)));

        // Then
        assertTrue(result.get() instanceof CancellationException);
    }

    @Test
    public void when_pmsCompleteStepThrowsException_then_jobStillSucceeds() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new FailingOnCompletePMS(
                () -> new MockPS(Identity::new, NODE_COUNT)
        )));

        // When
        Job job = instance.newJob(dag);

        // Then
        job.join();

        assertJobSucceeded(job);
    }

    @Test
    public void when_psCompleteStepThrowsException_then_jobStillSucceeds() throws Throwable {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new FailingOnCompletePS(Identity::new)));

        // When
        Job job = instance.newJob(dag);

        // Then
        job.join();

        assertJobSucceeded(job);
    }


    private void assertPmsCompleted() {
        assertTrue("initCalled", MockPMS.initCalled.get());
        assertTrue("completeCalled", MockPMS.completeCalled.get());
        assertNull("completeError", MockPMS.completeError.get());
    }

    private void assertPmsCompletedWithError(RuntimeException e) {
        assertTrue("initCalled", MockPMS.initCalled.get());
        assertTrue("completeCalled", MockPMS.completeCalled.get());
        assertExceptionInCauses(e, MockPMS.completeError.get());
    }

    private void assertPsCompletedWithoutError() {
        assertEquals(NODE_COUNT, MockPS.initCount.get());
        assertEquals(NODE_COUNT, MockPS.completeCount.get());
        assertEquals(0, MockPS.completeErrors.size());
    }

    private void assertPsCompletedWithError(Throwable e) {
        assertEquals(NODE_COUNT, MockPS.initCount.get());
        assertEquals(NODE_COUNT, MockPS.completeCount.get());
        assertEquals(NODE_COUNT, MockPS.completeErrors.size());

        for (int i = 0; i < NODE_COUNT; i++) {
            assertExceptionInCauses(e, MockPS.completeErrors.get(i));
        }
    }

    private void assertJobSucceeded(Job job) {
        JobResult jobResult = getJobResult(job);
        assertTrue(jobResult.isSuccessful());
        assertNull(jobResult.getFailure());
    }

    private void assertJobFailed(Job job, Throwable e) {
        JobResult jobResult = getJobResult(job);
        assertFalse("jobResult.isSuccessful", jobResult.isSuccessful());
        assertExceptionInCauses(e, jobResult.getFailure());
        JobStatus expectedStatus = e instanceof CancellationException ? JobStatus.COMPLETED : JobStatus.FAILED;
        assertEquals("jobStatus", expectedStatus, job.getJobStatus());
    }

    private JobResult getJobResult(Job job) {
        JetService jetService = getJetService(instance);
        assertNull(jetService.getJobRepository().getJob(job.getJobId()));
        JobResult jobResult = jetService.getJobCoordinationService().getJobResult(job.getJobId());
        assertNotNull(jobResult);
        return jobResult;
    }
}
