/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.Identity;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestProcessors.streamingDag;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobTest extends SimpleTestInClusterSupport {
    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    @SuppressWarnings("Convert2MethodRef")
    @Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return asList(
                mode("fromNonMaster", () -> instances()[1]),
                mode("fromClient", () -> client()));
    }

    static Object[] mode(String name, Supplier<HazelcastInstance> supplier) {
        return new Object[] {name, supplier};
    }

    @Parameter(0)
    public String mode;

    @Parameter(1)
    public Supplier<HazelcastInstance> instance;

    @BeforeClass
    public static void setup() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        initializeWithClient(NODE_COUNT, config, null);
    }

    @Before
    public void resetProcessors() {
        TestProcessors.reset(TOTAL_PARALLELISM);
    }

    @Test
    public void when_jobSubmitted_then_jobStatusIsStarting() {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(Identity::new, NODE_COUNT).initBlocks()));

        // When
        Job job = instance.get().getJet().newJob(dag);
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_RUNNING || status == STARTING);

        for (int i = 0; i < NODE_COUNT; i++) {
            MockPS.unblock();
        }

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobSubmitted_then_userCancelledCannotBeQueried() {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(Identity::new, NODE_COUNT).initBlocks()));

        // When
        Job job = instance.get().getJet().newJob(dag);

        // Then
        assertJobIsUserCancelledCannotBeQueried(job);

        for (int i = 0; i < NODE_COUNT; i++) {
            MockPS.unblock();
        }
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting() {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(Identity::new, NODE_COUNT).initBlocks()));

        // When
        Job job = instance.get().getJet().newJobIfAbsent(dag, new JobConfig());
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_RUNNING || status == STARTING);

        for (int i = 0; i < NODE_COUNT; i++) {
            MockPS.unblock();
        }

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsFailedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        // When
        Job job = instance.get().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // Then
        cancelAndJoin(job);

        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(job, FAILED);
        assertTrue(job.isUserCancelled());
    }

    @Test
    public void when_jobFailed_then_jobStatusIsFailedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                () -> new MockP().setCompleteError(ExpectedRuntimeException::new)));

        // When
        Job job = instance.get().getJet().newJob(dag);

        // Then
        try {
            job.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, job.getStatus());
            assertFalse(job.isUserCancelled());
        }
    }

    @Test
    public void when_jobSubmitted_then_trackedJobCanQueryJobStatus() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));

        // When
        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);
        assertJobIsUserCancelledCannotBeQueried(trackedJob);

        cancelAndJoin(submittedJob);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_trackedJobCanQueryStatus() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));

        // When
        Job submittedJob = instance().getJet().newJobIfAbsent(dag, new JobConfig());

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);
        assertJobIsUserCancelledCannotBeQueried(trackedJob);

        cancelAndJoin(submittedJob);
    }

    @Test
    public void when_namedJobSubmittedWithNewJobIfAbsent_then_trackedJobCanQueryStatus() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job submittedJob = instance().getJet().newJobIfAbsent(dag, config);

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        cancelAndJoin(submittedJob);
    }

    @Test
    public void when_jobCompleted_then_trackedJobCanQueryResult() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        // When
        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
        assertFalse(trackedJob.isUserCancelled());
    }

    @Test
    public void when_jobCancelled_then_trackedJobCanQueryResult() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        // When
        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        submittedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(trackedJob, FAILED);
        assertTrue(trackedJob.isUserCancelled());
    }

    @Test
    public void when_jobFailed_then_trackedJobCanQueryResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test",
                () -> new MockP().setCompleteError(ExpectedRuntimeException::new)));

        // When
        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        try {
            trackedJob.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, trackedJob.getStatus());
            assertFalse(trackedJob.isUserCancelled());
        }
    }

    @Test
    public void test_trackedJobCanJoin_lightJob() throws Exception {
        test_trackedJobCanJoin(true);
    }

    @Test
    public void test_trackedJobCanJoin_normalJob() throws Exception {
        test_trackedJobCanJoin(false);
    }

    private void test_trackedJobCanJoin(boolean useLightJob) throws Exception {
        DAG dag = new DAG().vertex(new Vertex("v", () -> new NoOutputSourceP()));

        Job submittedJob = useLightJob ? instance().getJet().newLightJob(dag) : instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        Job trackedJob = assertJobVisibleEventually(instance.get(), submittedJob);

        Future<?> joinFuture = spawn(trackedJob::join);
        sleepMillis(200);
        assertFalse(trackedJob.getFuture().isDone());
        assertFalse(joinFuture.isDone());

        NoOutputSourceP.proceedLatch.countDown();
        trackedJob.join();
        joinFuture.get();
    }

    @Test
    public void when_trackedJobCancels_then_jobCompletes() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instance.get().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // When
        trackedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);
        joinAndExpectCancellation(submittedJob);

        NoOutputSourceP.proceedLatch.countDown();

        assertJobStatusEventually(trackedJob, FAILED);
        assertTrue(trackedJob.isUserCancelled());
        assertJobStatusEventually(submittedJob, FAILED);
        assertTrue(submittedJob.isUserCancelled());
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job = instance().getJet().newJob(dag, config);
        assertEquals(jobName, job.getName());

        // Then
        Job trackedJob = instance.get().getJet().getJob(jobName);
        assertNotNull(trackedJob);

        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertJobStatusEventually(trackedJob, RUNNING);
        assertJobIsUserCancelledCannotBeQueried(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedById() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        // When
        Job job = instance().getJet().newJob(dag);

        // Then
        Job trackedJob = instance.get().getJet().getJob(job.getId());
        assertNotNull(trackedJob);

        assertEquals(job.getId(), trackedJob.getId());
        assertJobStatusEventually(trackedJob, RUNNING);
        assertJobIsUserCancelledCannotBeQueried(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance.get().getJet().getJob(jobName);
        assertNotNull(trackedJob);

        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedById() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        // When
        Job job = instance().getJet().newJob(dag);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance.get().getJet().getJob(job.getId());
        assertNotNull(trackedJob);

        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobIsQueriedByInvalidId_then_noJobIsReturned() {
        assertNull(instance.get().getJet().getJob(0));
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedSubmitJoinsExisting() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig()
                .setName(randomName());
        Job job1 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);

        // When
        Job job2 = instance.get().getJet().newJobIfAbsent(dag, config);

        // Then
        assertEquals(job1.getId(), job2.getId());
        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void stressTest_parallelNamedJobSubmission() throws Exception {
        final int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        String randomPrefix = randomName();
        try {
            for (int round = 0; round < 10; round++) {
                DAG dag = new DAG().vertex(new Vertex("test" + round, () -> new MockP().streaming()));
                System.out.println("Starting round " + round);
                JobConfig config = new JobConfig().setName(randomPrefix + round);
                List<Future<Job>> futures = new ArrayList<>();
                for (int i = 0; i < nThreads; i++) {
                    futures.add(executor.submit(() -> instance.get().getJet().newJobIfAbsent(dag, config)));
                }
                for (int i = 1; i < nThreads; i++) {
                    assertEquals(futures.get(0).get().getId(), futures.get(i).get().getId());
                }
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, MINUTES));
        }
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedJobFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job, RUNNING);

        // Then
        assertThatThrownBy(() -> instance.get().getJet().newJob(dag, config))
                .isInstanceOf(JobAlreadyExistsException.class);
    }

    @Test
    public void when_namedJobCompletedAndAnotherSubmitted_then_runningOneIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job2, RUNNING);

        // Then
        Job trackedJob = instance.get().getJet().getJob(jobName);
        assertNotNull(trackedJob);

        assertEquals(jobName, trackedJob.getName());
        assertNotEquals(job1.getId(), trackedJob.getId());
        assertEquals(job2.getId(), trackedJob.getId());
        assertEquals(RUNNING, trackedJob.getStatus());
        assertJobIsUserCancelledCannotBeQueried(trackedJob);
    }

    @Test
    public void when_namedJobCompletedAndAnotherSubmitted_then_bothAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job2, RUNNING);

        // Then
        List<Job> trackedJobs = instance.get().getJet().getJobs(jobName);

        assertEquals(2, trackedJobs.size());

        Job trackedJob1 = trackedJobs.get(1);
        Job trackedJob2 = trackedJobs.get(0);

        assertEquals(job1.getId(), trackedJob1.getId());
        assertEquals(COMPLETED, trackedJob1.getStatus());
        assertFalse(trackedJob1.isUserCancelled());

        assertEquals(job2.getId(), trackedJob2.getId());
        assertEquals(RUNNING, trackedJob2.getStatus());
        assertJobIsUserCancelledCannotBeQueried(trackedJob2);
    }

    @Test
    public void when_jobConfigChanged_then_doesNotAffectSubmittedJob() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));
        JobConfig config = new JobConfig()
                .setName(randomName());

        Job job1 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);

        // When
        config.setName(randomName());

        // Then
        Job job2 = instance.get().getJet().newJob(dag, config);
        assertJobStatusEventually(job2, RUNNING);
    }

    @Test
    public void when_jobsCompleted_then_theyAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job2.join();

        // Then
        assertNotNull(config.getName());
        List<Job> jobs = instance.get().getJet().getJobs(config.getName());
        assertEquals(2, jobs.size());

        Job trackedJob1 = jobs.get(1);
        Job trackedJob2 = jobs.get(0);

        assertEquals(job1.getId(), trackedJob1.getId());
        assertEquals(config.getName(), trackedJob1.getName());
        assertEquals(COMPLETED, trackedJob1.getStatus());
        assertFalse(trackedJob1.isUserCancelled());

        assertEquals(job2.getId(), trackedJob2.getId());
        assertEquals(config.getName(), trackedJob1.getName());
        assertEquals(COMPLETED, trackedJob2.getStatus());
        assertFalse(trackedJob2.isUserCancelled());
    }

    @Test
    public void when_suspendedNamedJob_then_newJobIfAbsentWithEqualNameJoinsIt() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);

        // Then
        Job job2 = instance.get().getJet().newJobIfAbsent(dag, config);
        assertEquals(job1.getId(), job2.getId());
        assertEquals(SUSPENDED, job2.getStatus());
        assertJobIsUserCancelledCannotBeQueried(job2);
    }

    @Test
    public void when_suspendedNamedJob_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job, RUNNING);
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);

        // Then
        assertThatThrownBy(() -> instance.get().getJet().newJob(dag, config))
                .isInstanceOf(JobAlreadyExistsException.class);
    }

    @Test
    public void when_namedJobCancelledAndJoined_then_newJobWithSameNameCanBeSubmitted() {
        DAG dag = new DAG().vertex(new Vertex("v", () -> new MockP().streaming()));
        for (int i = 0; i < 10; i++) {
            JobConfig config = new JobConfig()
                    .setName("job");
            Job job = instance.get().getJet().newJob(dag, config);
            assertJobStatusEventually(job, RUNNING);
            assertJobIsUserCancelledCannotBeQueried(job);
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) { }
        }
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried_normalJob() throws Exception {
        when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(false);
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried_lightJob() throws Exception {
        when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(true);
    }

    private void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(boolean useLightJob) throws Exception {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        // When
        Job job = useLightJob ? instance().getJet().newLightJob(dag) : instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // The light job is submitted in JobCoordinationService.submitLightJob. The order of instructions is:
        // - LightMasterContext.createContext()
        // - thenComposeAsync -> lightMasterContexts.put(jobId, mc)
        // As long as the context is not put in the lightMasterContexts we cannot get the job by id. The
        // tasklets are added to workers in the execution of LightMasterContext.createContext(), so the
        // tasklet may start before the lightMasterContexts is filled.
        Job trackedJob = assertJobVisibleEventually(instance.get(), job);

        // Then
        long now = System.currentTimeMillis();
        long submissionTime = job.getSubmissionTime();
        long trackedJobSubmissionTime = trackedJob.getSubmissionTime();
        assertNotEquals(0, submissionTime);
        assertNotEquals(0, trackedJobSubmissionTime);
        assertEquals(submissionTime, trackedJobSubmissionTime);
        assertBetween("submissionTime", submissionTime, now - MINUTES.toMillis(10), now);
        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_jobSubmissionTimeIsQueried() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        Job trackedJob = instance.get().getJet().getJob(jobName);

        // Then
        assertNotNull(trackedJob);
        assertNotEquals(0, job.getSubmissionTime());
        assertNotEquals(0, trackedJob.getSubmissionTime());
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForTheJob() {
        // Given
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new ListSource(new Value(1), new Value(2)));
        Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLoggerP());
        dag.edge(between(source, sink).distributed().partitioned(FunctionEx.identity()));

        JobConfig config = new JobConfig()
                .registerSerializer(Value.class, ValueSerializer.class);

        // When
        Job job = instance.get().getJet().newJob(dag, config);

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobWithSameNameManyTimes_then_queryResultSortedBySubmission() {
        DAG streamingDag = new DAG().vertex(new Vertex("v", () -> new MockP().streaming()));

        List<Long> jobIds = new ArrayList<>();

        // When
        String jobName = randomName();
        for (int i = 0; i < 10; i++) {
            Job job = instance().getJet().newJob(streamingDag, new JobConfig().setName(jobName));
            jobIds.add(0, job.getId());
            job.cancel();
            joinAndExpectCancellation(job);
        }
        Job activeJob = instance().getJet().newJob(streamingDag, new JobConfig().setName(jobName));
        jobIds.add(0, activeJob.getId());

        // Then
        List<Job> actualJobs = instance.get().getJet().getJobs(jobName);
        assertThat(toList(actualJobs, Job::getId))
                .containsExactlyElementsOf(jobIds);
    }

    @Test
    public void test_manyJobs() {
        // We use a standalone cluster here. This test looks at all the jobs,
        // so it must not see completed jobs from other tests.
        HazelcastInstance[] hz = createHazelcastInstances(2);
        HazelcastInstance inst = mode.equals("fromNonMaster") ? hz[1] : createHazelcastClient();

        JetService jet = inst.getJet();
        DAG streamingDag = new DAG().vertex(new Vertex("v", () -> new MockP().streaming()));

        DAG batchDag = new DAG().vertex(new Vertex("v", MockP::new));

        // normal streaming job
        Job streamingJob = jet.newJob(streamingDag);
        assertJobStatusEventually(streamingJob, RUNNING);

        // two normal batch job
        Job batchJob1 = jet.newJob(batchDag);
        batchJob1.join();
        Job batchJob2 = jet.newJob(batchDag);
        batchJob2.join();

        // named streaming job name1
        Job namedStreamingJob1 = jet.newJob(streamingDag, new JobConfig().setName("name1"));
        assertJobStatusEventually(namedStreamingJob1, RUNNING);

        // named streaming job name2, cancelled
        Job namedStreamingJob2 = jet.newJob(streamingDag, new JobConfig().setName("name2"));
        namedStreamingJob2.cancel();
        joinAndExpectCancellation(namedStreamingJob2);

        // named streaming job name2, again, not cancelled
        Job namedStreamingJob2_1 = jet.newJob(streamingDag, new JobConfig().setName("name2"));
        assertJobStatusEventually(namedStreamingJob2_1, RUNNING);

        // light streaming job
        Job lightStreamingJob = jet.newLightJob(streamingDag);

        // light streaming job, cancelled
        Job lightStreamingJobCancelled = jet.newLightJob(streamingDag);
        assertJobVisibleEventually(inst, lightStreamingJobCancelled);
        lightStreamingJobCancelled.cancel();
        joinAndExpectCancellation(lightStreamingJobCancelled);

        // two light batch jobs
        Job lightBatchJob1 = jet.newLightJob(batchDag);
        Job lightBatchJob2 = jet.newLightJob(batchDag);
        lightBatchJob1.join();
        lightBatchJob2.join();

        List<Job> allJobsExceptCompletedLightJobs = asList(streamingJob, batchJob1, batchJob2,
                namedStreamingJob1, namedStreamingJob2, namedStreamingJob2_1, lightStreamingJob);

        List<Job> allJobs = new ArrayList<>(allJobsExceptCompletedLightJobs);
        allJobs.add(lightStreamingJobCancelled);
        allJobs.add(lightBatchJob1);
        allJobs.add(lightBatchJob2);

        // Then
        // getJobs must include all submitted jobs, except for the light batch jobs that are done
        assertThat(toList(jet.getJobs(), this::jobEqualityString))
                .containsExactlyInAnyOrderElementsOf(toList(allJobsExceptCompletedLightJobs, this::jobEqualityString));

        for (Job job : allJobs) {
            Job trackedJobById = jet.getJob(job.getId());
            Job trackedJobByName = job.getName() != null ? jet.getJob(job.getName()) : null;

            if (allJobsExceptCompletedLightJobs.contains(job)) {
                assertEquals(jobEqualityString(job), jobEqualityString(trackedJobById));
                if (job.getName() != null && job != namedStreamingJob2) {
                    assertEquals(jobEqualityString(job), jobEqualityString(trackedJobByName));
                }
            } else {
                assertNull(trackedJobById);
                assertNull(trackedJobByName);
            }
        }

        assertThat(toList(jet.getJobs("name1"), this::jobEqualityString))
                .containsExactlyElementsOf(toList(singletonList(namedStreamingJob1), this::jobEqualityString));

        assertThat(toList(jet.getJobs("name2"), this::jobEqualityString))
                .containsExactlyElementsOf(toList(asList(namedStreamingJob2_1, namedStreamingJob2), this::jobEqualityString));
    }

    /**
     * Converts {@link Job} to a string. Used for comparing two Job instances.
     */
    private String jobEqualityString(Job job) {
        if (job == null) {
            return null;
        }
        return "id=" + job.getIdString() +
                ", name=" + job.getName() +
                ", light=" + job.isLightJob() +
                ", submissionTime=" + job.getSubmissionTime() +
                ", status=" + job.getStatus();
    }

    @Test
    public void test_updateJobConfig() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(asList(2, 3, 1)))
                .sort()
                .writeTo(AssertionSinks.assertOrdered(asList(1, 2, 3)));

        JobConfig config = new JobConfig().setSuspendOnFailure(true).setMaxProcessorAccumulatedRecords(2);
        Job job = instance.get().getJet().newJob(pipeline, config);
        assertJobSuspendedEventually(job);
        assertThat(job.getSuspensionCause().errorCause())
                .contains("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");

        job.updateConfig(new DeltaJobConfig().setMaxProcessorAccumulatedRecords(3L));
        Stream.of(instances()[0], instances()[1], client()).forEach(hz ->
                assertEquals(3, hz.getJet().getJob(job.getId()).getConfig().getMaxProcessorAccumulatedRecords()));

        job.resume();
        job.join();

        assertThatThrownBy(() -> job.updateConfig(new DeltaJobConfig())).hasMessage("Job not suspended, but COMPLETED");
    }

    @Test
    public void test_tryUpdatingJobConfig_then_fail() {
        DAG dag = new DAG().vertex(new Vertex("test", () -> new MockP().streaming()));

        Job job = instance.get().getJet().newJob(dag);
        assertThatThrownBy(() -> job.updateConfig(new DeltaJobConfig())).hasMessageStartingWith("Job not suspended, but");

        assertJobStatusEventually(job, RUNNING);
        assertThatThrownBy(() -> job.updateConfig(new DeltaJobConfig())).hasMessage("Job not suspended, but RUNNING");

        cancelAndJoin(job);
        assertThatThrownBy(() -> job.updateConfig(new DeltaJobConfig())).hasMessage("Job not suspended, but FAILED");
    }

    @Test
    public void test_tryUpdatingLightJobConfig_then_fail() throws InterruptedException {
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));

        Job job = instance.get().getJet().newLightJob(dag);
        assertThatThrownBy(() -> job.updateConfig(new DeltaJobConfig()))
                .hasMessage("not supported for light jobs: updateConfig");
        NoOutputSourceP.executionStarted.await();
        cancelAndJoin(job);
    }

    @Test
    public void when_lightJob_then_unsupportedMethodsThrow() {
        DAG streamingDag = new DAG().vertex(new Vertex("v", () -> new MockP().streaming()));

        // When
        Job job = instance.get().getJet().newLightJob(streamingDag);

        // Then
        assertThatThrownBy(job::getSuspensionCause).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(job::getMetrics).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(job::restart).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(job::suspend).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(job::resume).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> job.cancelAndExportSnapshot("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> job.exportSnapshot("foo")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void test_nonSmartClient() {
        if (mode.equals("fromNonMaster")) {
            HazelcastInstance client = factory().newHazelcastClient(configForNonSmartClientConnectingTo(instances()[1]));

            Job job = instance().getJet().newLightJob(streamingDag());
            assertTrueEventually(() -> assertJobExecuting(job, instance()));

            Job trackedJob = client.getJet().getJob(job.getId());
            assertNotNull(trackedJob);

            trackedJob.getSubmissionTime();
            assertEquals(RUNNING, trackedJob.getStatus());
            assertJobIsUserCancelledCannotBeQueried(trackedJob);
            assertTrue(trackedJob.isLightJob());
            cancelAndJoin(trackedJob);
        } else {
            HazelcastInstance client = factory().newHazelcastClient(configForNonSmartClientConnectingTo(instance()));

            // Test whether the client will randomly pick a member it's not connected to
            for (int i = 0; i < 10; i++) {
                Job job = client.getJet().newLightJob(streamingDag());
                assertTrueEventually(() -> assertJobExecuting(job, instance()));
                assertNotNull(client.getJet().getJob(job.getId()));
                cancelAndJoin(job);
            }
        }
    }

    private static void joinAndExpectCancellation(Job job) {
        assertThatThrownBy(job::join).isInstanceOf(CancellationException.class);
    }

    private static void assertJobIsUserCancelledCannotBeQueried(Job job) {
        assertThatThrownBy(job::isUserCancelled).isInstanceOf(IllegalStateException.class);
    }

    private static final class Value {
        private final int value;

        private Value(int value) {
            this.value = value;
        }
    }

    private static class ValueSerializer implements StreamSerializer<Value> {
        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void write(ObjectDataOutput output, Value value) throws IOException {
            output.writeInt(value.value);
        }

        @Override @Nonnull
        public Value read(ObjectDataInput input) throws IOException {
            return new Value(input.readInt());
        }
    }
}
