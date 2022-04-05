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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.Identity;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobTest extends SimpleTestInClusterSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        initializeWithClient(NODE_COUNT, config, null);
    }

    @Before
    public void setup() {
        TestProcessors.reset(TOTAL_PARALLELISM);
    }

    @Test
    public void when_jobSubmitted_then_jobStatusIsStarting_member() {
        when_jobSubmitted_then_jobStatusIsStarting(instances()[1]);
    }

    @Test
    public void when_jobSubmitted_then_jobStatusIsStarting_client() {
        when_jobSubmitted_then_jobStatusIsStarting(client());
    }

    private void when_jobSubmitted_then_jobStatusIsStarting(HazelcastInstance submitter) {
        PSThatWaitsOnInit.initLatch = new CountDownLatch(1);
        DAG dag = new DAG().vertex(new Vertex("test", new PSThatWaitsOnInit(Identity::new)));

        // When
        Job job = submitter.getJet().newJob(dag);
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_RUNNING || status == STARTING);

        PSThatWaitsOnInit.initLatch.countDown();

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting_nonMaster() {
        when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting(instances()[1]);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting_client() {
        when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting(client());
    }

    private void when_jobSubmittedWithNewJobIfAbsent_then_jobStatusIsStarting(HazelcastInstance submitter) {
        PSThatWaitsOnInit.initLatch = new CountDownLatch(1);
        DAG dag = new DAG().vertex(new Vertex("test", new PSThatWaitsOnInit(Identity::new)));

        // When
        Job job = submitter.getJet().newJobIfAbsent(dag, new JobConfig());
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_RUNNING || status == STARTING);

        PSThatWaitsOnInit.initLatch.countDown();

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // Then
        cancelAndJoin(job);

        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(job, FAILED);
    }

    @Test
    public void when_jobFailed_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS((SupplierEx<Processor>)
                () -> new MockP().setCompleteError(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        Job job = instance().getJet().newJob(dag);

        // Then
        try {
            job.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, job.getStatus());
        }
    }

    @Test
    public void when_jobSubmitted_then_trackedJobCanQueryJobStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        cancelAndJoin(submittedJob);
    }

    @Test
    public void when_jobSubmittedWithNewJobIfAbsent_then_trackedJobCanQueryStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance().getJet().newJobIfAbsent(dag, new JobConfig());
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        cancelAndJoin(submittedJob);
    }

    @Test
    public void when_namedJobSubmittedWithNewJobIfAbsent_then_trackedJobCanQueryStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job submittedJob = instance().getJet().newJobIfAbsent(dag, config);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        assertJobStatusEventually(trackedJob, RUNNING);

        cancelAndJoin(submittedJob);
    }

    @Test
    public void when_jobCompleted_then_trackedJobCanQueryResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobCancelled_then_trackedJobCanQueryResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        submittedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();
        assertJobStatusEventually(trackedJob, FAILED);
    }

    @Test
    public void when_jobFailed_then_trackedJobCanQueryResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS((SupplierEx<Processor>)
                () -> new MockP().setCompleteError(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // Then
        try {
            trackedJob.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, trackedJob.getStatus());
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
        DAG dag = new DAG();
        dag.newVertex("v", (SupplierEx<Processor>) NoOutputSourceP::new);

        Job submittedJob = useLightJob ? instance().getJet().newLightJob(dag) : instance().getJet().newJob(dag);

        Job[] trackedJob = {null};
        assertTrueEventually(() -> {
            trackedJob[0] = instance().getJet().getJob(submittedJob.getId());
            assertNotNull(trackedJob[0]);
        });

        Future<?> joinFuture = spawn(() -> trackedJob[0].join());
        sleepMillis(200);
        assertFalse(trackedJob[0].getFuture().isDone());
        assertFalse(joinFuture.isDone());

        NoOutputSourceP.proceedLatch.countDown();
        trackedJob[0].join();
        joinFuture.get();
    }

    @Test
    public void when_trackedJobCancels_then_jobCompletes() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        Job submittedJob = instance().getJet().newJob(dag);

        Collection<Job> trackedJobs = instances()[1].getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        // When
        trackedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);
        joinAndExpectCancellation(submittedJob);

        NoOutputSourceP.proceedLatch.countDown();

        assertJobStatusEventually(trackedJob, FAILED);
        assertJobStatusEventually(submittedJob, FAILED);
    }

    @Test
    public void when_jobCompleted_then_trackedJobCanQueryResultFromClient() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job submittedJob = instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        Collection<Job> trackedJobs = client().getJet().getJobs();
        Job trackedJob = trackedJobs.stream().filter(j -> j.getId() == submittedJob.getId()).findFirst().orElse(null);
        assertNotNull(trackedJob);

        NoOutputSourceP.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByName_member() throws InterruptedException {
        when_jobIsRunning_then_itIsQueriedByName(instances()[1]);
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByName_client() throws InterruptedException {
        when_jobIsRunning_then_itIsQueriedByName(client());
    }


    private void when_jobIsRunning_then_itIsQueriedByName(HazelcastInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job = instance().getJet().newJob(dag, config);
        assertEquals(jobName, job.getName());
        NoOutputSourceP.executionStarted.await();

        // Then
        Job trackedJob = instance.getJet().getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertJobStatusEventually(trackedJob, RUNNING);

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedById() throws InterruptedException {
        testGetJobByIdWhenJobIsRunning(instance());
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByIdFromClient() throws InterruptedException {
        testGetJobByIdWhenJobIsRunning(client());
    }

    private void testGetJobByIdWhenJobIsRunning(HazelcastInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // Then
        Job trackedJob = instance.getJet().getJob(job.getId());

        assertNotNull(trackedJob);
        assertEquals(job.getId(), trackedJob.getId());
        assertJobStatusEventually(trackedJob, RUNNING);

        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance().getJet().getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedById() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = instance().getJet().newJob(dag);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance().getJet().getJob(job.getId());

        assertNotNull(trackedJob);
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsQueriedByInvalidId_then_noJobIsReturned_member() {
        assertNull(instance().getJet().getJob(0));
    }

    @Test
    public void when_jobIsQueriedByInvalidId_then_noJobIsReturned_client() {
        assertNull(client().getJet().getJob(0));
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedSubmitJoinsExisting_member() {
        when_namedJobIsRunning_then_newNamedSubmitJoinsExisting(instance());
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedSubmitJoinsExisting_client() {
        when_namedJobIsRunning_then_newNamedSubmitJoinsExisting(client());
    }

    private void when_namedJobIsRunning_then_newNamedSubmitJoinsExisting(HazelcastInstance instance) {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName(randomName());
        Job job1 = instance.getJet().newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job1.getStatus()));

        // When
        Job job2 = instance.getJet().newJobIfAbsent(dag, config);

        // Then
        assertEquals(job1.getId(), job2.getId());
        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void stressTest_parallelNamedJobSubmission_member() throws Exception {
        stressTest_parallelNamedJobSubmission(instance());
    }

    @Test
    public void stressTest_parallelNamedJobSubmission_client() throws Exception {
        stressTest_parallelNamedJobSubmission(client());
    }

    private void stressTest_parallelNamedJobSubmission(HazelcastInstance instance) throws Exception {
        final int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        String randomPrefix = randomName();
        try {
            for (int round = 0; round < 10; round++) {
                DAG dag = new DAG().vertex(new Vertex("test" + round, new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
                System.out.println("Starting round " + round);
                JobConfig config = new JobConfig().setName(randomPrefix + round);
                List<Future<Job>> futures = new ArrayList<>();
                for (int i = 0; i < nThreads; i++) {
                    futures.add(executor.submit(() -> instance.getJet().newJobIfAbsent(dag, config)));
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
    public void when_namedJobIsRunning_then_newNamedJobFails_member() {
        when_namedJobIsRunning_then_newNamedJobFails(instance());
    }

    @Test
    public void when_namedJobIsRunning_then_newNamedJobFails_client() {
        when_namedJobIsRunning_then_newNamedJobFails(client());
    }

    private void when_namedJobIsRunning_then_newNamedJobFails(HazelcastInstance instance) {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job1 = instance.getJet().newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job1.getStatus()));

        // Then
        assertThatThrownBy(() -> instance.getJet().newJob(dag, config))
                .isInstanceOf(JobAlreadyExistsException.class);
    }

    @Test
    public void when_namedJobCompletedAndAnotherSubmitted_then_runningOneIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance().getJet().newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job2.getStatus()));

        // Then
        Job trackedJob = instance().getJet().getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertNotEquals(job1.getId(), trackedJob.getId());
        assertEquals(job2.getId(), trackedJob.getId());
        assertEquals(RUNNING, trackedJob.getStatus());
    }

    @Test
    public void when_namedJobCompletedAndAnotherSubmitted_then_bothAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance().getJet().newJob(dag, config);
        assertTrueEventually(() -> assertEquals(RUNNING, job2.getStatus()));

        // Then
        List<Job> trackedJobs = instance().getJet().getJobs(jobName);

        assertEquals(2, trackedJobs.size());

        Job trackedJob1 = trackedJobs.get(0);
        Job trackedJob2 = trackedJobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals(RUNNING, trackedJob1.getStatus());

        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals(COMPLETED, trackedJob2.getStatus());
    }

    @Test
    public void when_jobConfigChanged_then_doesNotAffectSubmittedJob() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName(randomName());

        Job job1 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);

        // When
        config.setName(randomName());

        // Then
        Job job2 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job2, RUNNING);
    }

    @Test
    public void when_jobsCompleted_then_theyAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job1.join();

        sleepAtLeastMillis(1);

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        Job job2 = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job2.join();

        // Then
        assertNotNull(config.getName());
        List<Job> jobs = instance().getJet().getJobs(config.getName());
        assertEquals(2, jobs.size());

        Job trackedJob1 = jobs.get(0);
        Job trackedJob2 = jobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals(config.getName(), trackedJob1.getName());
        assertEquals(COMPLETED, trackedJob1.getStatus());
        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals(config.getName(), trackedJob2.getName());
        assertEquals(COMPLETED, trackedJob2.getStatus());
    }

    @Test
    public void when_suspendedNamedJob_then_newJobIfAbsentWithEqualNameJoinsIt() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);

        // Then
        Job job2 = instances()[1].getJet().newJobIfAbsent(dag, config);
        assertEquals(job1.getId(), job2.getId());
        assertEquals(job2.getStatus(), SUSPENDED);
    }

    @Test
    public void when_suspendedNamedJob_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName(randomName());

        // When
        Job job1 = instance().getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);

        // Then
        assertThatThrownBy(() -> instances()[1].getJet().newJob(dag, config))
                .isInstanceOf(JobAlreadyExistsException.class);
    }

    @Test
    public void when_namedJobCancelledAndJoined_then_newJobWithSameNameCanBeSubmitted() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new TestProcessors.MockP().streaming());
        for (int i = 0; i < 10; i++) {
            JobConfig config = new JobConfig()
                    .setName("job");
            Job job = instance().getJet().newJob(dag, config);
            assertJobStatusEventually(job, RUNNING);
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) {
            }
        }
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried_member_normalJob() throws Exception {
        when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(instance(), false);
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried_member_lightJob() throws Exception {
        when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(instance(), true);
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried_client_normalJob() throws Exception {
        when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(client(), false);
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried_client_lightJob() throws Exception {
        when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(client(), true);
    }

    private void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried(HazelcastInstance instance, boolean useLightJob)
            throws Exception {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        // When
        Job job = useLightJob ? instances()[1].getJet().newLightJob(dag) : instance().getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();
        Job trackedJob = instance.getJet().getJob(job.getId());

        // Then
        assertNotNull(trackedJob);
        long submissionTime = job.getSubmissionTime();
        long trackedJobSubmissionTime = trackedJob.getSubmissionTime();
        assertNotEquals(0, submissionTime);
        assertNotEquals(0, trackedJobSubmissionTime);
        assertEquals(submissionTime, trackedJobSubmissionTime);
        assertBetween("submissionTime", submissionTime,
                System.currentTimeMillis() - MINUTES.toMillis(10), System.currentTimeMillis() + SECONDS.toMillis(1));
        NoOutputSourceP.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_jobSubmissionTimeIsQueried() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = randomName();
        config.setName(jobName);

        // When
        Job job = instance().getJet().newJob(dag, config);
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        Job trackedJob = instance().getJet().getJob(jobName);

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
        Job job = instance().getJet().newJob(dag, config);

        // Then
        assertJobStatusEventually(job, COMPLETED);
    }

    @Test
    public void when_jobWithSameNameManyTimes_then_queryResultSortedBySubmission() {
        DAG streamingDag = new DAG();
        streamingDag.newVertex("v", () -> new MockP().streaming());

        List<Long> jobIds = new ArrayList<>();

        // When
        for (int i = 0; i < 10; i++) {
            Job job = instance().getJet().newJob(streamingDag, new JobConfig().setName("foo"));
            jobIds.add(0, job.getId());
            job.cancel();
            joinAndExpectCancellation(job);
        }
        Job activeJob = instance().getJet().newJob(streamingDag, new JobConfig().setName("foo"));
        jobIds.add(0, activeJob.getId());

        // Then
        List<Job> actualJobs = instance().getJet().getJobs("foo");
        assertThat(toList(actualJobs, Job::getId))
                .containsExactlyElementsOf(jobIds);
    }

    @Test
    public void test_manyJobs_member() {
        // we use a standalone cluster here - this test looks at all the jobs and it must not see completed jobs from other tests
        HazelcastInstance inst = createHazelcastInstance();
        createHazelcastInstance();

        test_manyJobs(inst);
    }

    @Test
    public void test_manyJobs_client() {
        // we use a standalone cluster here - this test looks at all the jobs and it must not see completed jobs from other tests
        createHazelcastInstance();
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();

        test_manyJobs(client);
    }

    private void test_manyJobs(HazelcastInstance inst) {
        JetService jet = inst.getJet();
        DAG streamingDag = new DAG();
        streamingDag.newVertex("v", () -> new MockP().streaming());

        DAG batchDag = new DAG();
        batchDag.newVertex("v", MockP::new);

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
        Job lightStreamingJob = inst.getJet().newLightJob(streamingDag);

        // light streaming job, cancelled
        Job lightStreamingJobCancelled = jet.newLightJob(streamingDag);
        lightStreamingJobCancelled.cancel();
        joinAndExpectCancellation(lightStreamingJobCancelled);

        // two light batch jobs
        Job lightBatchJob1 = jet.newLightJob(batchDag);
        Job lightBatchJob2 = jet.newLightJob(batchDag);
        lightBatchJob1.join();
        lightBatchJob2.join();

        List<Job> allJobsExceptCompletedLightJobs =
                asList(streamingJob, batchJob1, batchJob2, namedStreamingJob1, namedStreamingJob2, namedStreamingJob2_1, lightStreamingJob);

        List<Job> allJobs = new ArrayList<>();
        allJobs.addAll(allJobsExceptCompletedLightJobs);
        allJobs.add(lightStreamingJobCancelled);
        allJobs.add(lightBatchJob1);
        allJobs.add(lightBatchJob2);

        // Then
        // getJobs must include all submitted all jobs, except for the light batch jobs that are done
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

    // ### Tests for light jobs

    @Test
    public void when_lightJob_then_unsupportedMethodsThrow() {
        DAG streamingDag = new DAG();
        streamingDag.newVertex("v", () -> new MockP().streaming());

        // When
        Job job = instance().getJet().newLightJob(streamingDag);

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
    public void test_smartClientConnectedToNonCoordinator() {
        HazelcastInstance clientConnectedToI1 = factory().newHazelcastClient(configForNonSmartClientConnectingTo(instances()[1]));

        Job job1 = instances()[0].getJet().newLightJob(streamingDag());
        assertTrueEventually(() -> assertJobExecuting(job1, instances()[0]));

        Job job1ThroughClient2 = clientConnectedToI1.getJet().getJob(job1.getId());
        assertNotNull("job1ThroughClient2 not found", job1ThroughClient2);
        job1ThroughClient2.getSubmissionTime();
        assertEquals(RUNNING, job1ThroughClient2.getStatus());
        assertTrue(job1ThroughClient2.isLightJob());
        cancelAndJoin(job1ThroughClient2);
    }

    @Test
    public void test_nonSmartClient() {
        HazelcastInstance client = factory().newHazelcastClient(configForNonSmartClientConnectingTo(instance()));

        // try multiple times - we test the case when the client randomly picks a member it's not connected to.
        // It should not pick such a member.
        for (int i = 0; i < 10; i++) {
            Job job = client.getJet().newLightJob(streamingDag());
            assertTrueEventually(() -> assertJobExecuting(job, instance()));
            cancelAndJoin(job);
        }
    }

    private void joinAndExpectCancellation(Job job) {
        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
        }
    }

    private static final class PSThatWaitsOnInit implements ProcessorSupplier {

        public static volatile CountDownLatch initLatch;
        private final SupplierEx<Processor> supplier;

        PSThatWaitsOnInit(SupplierEx<Processor> supplier) {
            this.supplier = supplier;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            initLatch.await();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());
        }
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
