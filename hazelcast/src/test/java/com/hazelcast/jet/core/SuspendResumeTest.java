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
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.JobResult;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.SUSPEND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SuspendResumeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int PARALLELISM = 4;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance[] instances;
    private DAG dag;
    private Config config;

    @Before
    public void before() {
        TestProcessors.reset(NODE_COUNT * PARALLELISM);
        instances = new HazelcastInstance[NODE_COUNT];
        config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(PARALLELISM);
        for (int i = 0; i < NODE_COUNT; i++) {
            instances[i] = createHazelcastInstance(config);
        }
        dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
    }

    @Test
    public void when_suspendAndResume_then_jobResumes() throws Exception {
        // When
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        job.resume();
        assertJobStatusEventually(job, RUNNING);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(this::isSuspend));
        }, 5);
    }

    @Test
    public void when_memberAddedWhileSuspended_then_jobResumesOnAllMembers() throws Exception {
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        // When
        createHazelcastInstance(config);

        // Then
        job.resume();
        assertJobStatusEventually(job, RUNNING);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertEquals(2 * NODE_COUNT + 1, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT + 1, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(this::isSuspend));
        }, 5);
    }

    @Test
    public void when_nonCoordinatorDiesWhileSuspended_then_jobResumes() throws Exception {
        // When
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        instances[2].getLifecycleService().terminate();
        job.resume();
        assertJobStatusEventually(job, RUNNING);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT - 1, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT - 1, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(this::isSuspend));
        }, 5);
    }

    @Test
    public void when_coordinatorDiesWhileSuspended_then_jobResumes() throws Exception {
        // When
        Job job = instances[1].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        instances[0].getLifecycleService().terminate();
        for (int i = 0; ; i++) {
            try {
                // resume() can fail with JobNotFoundException if the new master didn't yet scan the jobs
                // and created MasterContext for the JobRecords. It should do so in few seconds.
                job.resume();
                break;
            } catch (JobNotFoundException e) {
                if (i == 20) {
                    throw e;
                }
                sleepSeconds(1);
            }
        }
        assertJobStatusEventually(job, RUNNING);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        assertEquals(2 * NODE_COUNT - 1, MockPS.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(2 * NODE_COUNT - 1, MockPS.closeCount.get());
            assertEquals(NODE_COUNT, MockPS.receivedCloseErrors.size());
            assertTrue(MockPS.receivedCloseErrors.stream().allMatch(this::isSuspend));
        }, 5);
    }

    @Test
    public void when_joinAndThenSuspend_then_joinBlocks() throws Exception {
        Job job = instances[1].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        // When
        Future future = spawn(job::join);
        sleepSeconds(1); // wait for the join to reach member
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        // Then
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 2);
    }

    @Test
    public void when_suspendAndThenJoin_then_joinBlocks() throws Exception {
        Job job = instances[1].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        // When
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        Future future = spawn(job::join);
        // Then
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 2);
    }

    @Test
    public void when_joinSuspendedJob_then_waitsAndReturnsAfterResume() throws Exception {
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        // When
        Future future = spawn(job::join);
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 1);
        job.resume();
        assertJobStatusEventually(job, RUNNING);
        assertTrueAllTheTime(() -> assertFalse(future.isDone()), 1);
        NoOutputSourceP.proceedLatch.countDown();
        assertTrueEventually(() -> assertTrue(future.isDone()), 5);
    }

    @Test
    public void when_cancelSuspendedJob_then_jobCancels() throws Exception {
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        // When-Then
        cancelAndJoin(job);
        assertJobStatusEventually(job, FAILED);

        // check that job resources are deleted
        JobRepository jobRepository = new JobRepository(instances[0]);
        assertTrueEventually(() -> {
            assertNull("JobRecord", jobRepository.getJobRecord(job.getId()));
            JobResult jobResult = jobRepository.getJobResult(job.getId());
            assertContains(jobResult.getFailureText(), CancellationException.class.getName());
            assertFalse("Job result successful", jobResult.isSuccessful());
        });
    }

    @Test
    public void when_restartSuspendedJob_then_fail() throws Exception {
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        // Then
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot RESTART_GRACEFUL, job status is SUSPENDED");
        // When
        job.restart();
    }

    @Test
    public void when_suspendSuspendedJob_then_fail() throws Exception {
        Job job = instances[0].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        // Then
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot SUSPEND_GRACEFUL, job status is SUSPENDED");
        // When
        job.suspend();
    }

    @Test
    public void when_jobSuspendedAndCoordinatorShutDown_then_jobStaysSuspended() throws Exception {
        when_jobSuspendedAndCoordinatorGone_then_jobStaysSuspended(true);
    }

    @Test
    public void when_jobSuspendedAndCoordinatorTerminated_then_jobStaysSuspended() throws Exception {
        when_jobSuspendedAndCoordinatorGone_then_jobStaysSuspended(false);
    }

    private void when_jobSuspendedAndCoordinatorGone_then_jobStaysSuspended(boolean graceful) throws Exception {
        assertTrue(((ClusterService) instances[0].getCluster()).isMaster());
        Job job = instances[1].getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        if (graceful) {
            instances[0].shutdown();
        } else {
            instances[0].getLifecycleService().terminate();
        }
        assertTrueAllTheTime(() -> assertEquals(SUSPENDED, job.getStatus()), 10);
    }

    private boolean isSuspend(Throwable e) {
        return e instanceof JobTerminateRequestedException
                && ((JobTerminateRequestedException) e).mode().actionAfterTerminate() == SUSPEND;
    }
}
