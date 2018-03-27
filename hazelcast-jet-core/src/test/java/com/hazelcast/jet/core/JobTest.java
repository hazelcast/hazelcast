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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.Identity;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class JobTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;

    @Before
    public void setup() {
        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        instance1 = createJetMember(config);
        instance2 = createJetMember(config);
    }

    @Test
    public void when_jobIsSubmittedFromNonMaster_then_jobStatusShouldBeStarting() {
        testJobStatusDuringStart(instance2);
    }

    @Test
    public void when_jobIsSubmittedFromClient_then_jobStatusShouldBeStarting() {
        testJobStatusDuringStart(createJetClient());
    }

    private void testJobStatusDuringStart(JetInstance submitter) {
        PSThatWaitsOnInit.initLatch = new CountDownLatch(1);
        DAG dag = new DAG().vertex(new Vertex("test", new PSThatWaitsOnInit(Identity::new)));

        // When
        Job job = submitter.newJob(dag);
        JobStatus status = job.getStatus();

        assertTrue(status == NOT_STARTED || status == STARTING);

        PSThatWaitsOnInit.initLatch.countDown();

        // Then
        assertCompletedEventually(job);
    }

    @Test
    public void when_jobIsCancelled_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        // Then
        job.cancel();
        joinAndExpectCancellation(job);

        StuckProcessor.proceedLatch.countDown();
        assertCompletedEventually(job);
    }

    @Test
    public void when_jobIsFailed_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS((DistributedSupplier<Processor>)
                () -> new MockP().setCompleteError(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);

        // Then
        try {
            job.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, job.getStatus());
        }
    }

    @Test
    public void when_jobIsSubmitted_then_trackedJobCanQueryJobStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        assertTrueEventually(() -> assertEquals(RUNNING, trackedJob.getStatus()));

        submittedJob.cancel();
        joinAndExpectCancellation(submittedJob);
    }

    @Test
    public void when_jobIsCompleted_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        StuckProcessor.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsCancelled_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        submittedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);

        StuckProcessor.proceedLatch.countDown();
        assertCompletedEventually(trackedJob);
    }

    @Test
    public void when_jobIsFailed_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS((DistributedSupplier<Processor>)
                () -> new MockP().setCompleteError(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        instance1.newJob(dag);

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        try {
            trackedJob.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, trackedJob.getStatus());
        }
    }

    @Test
    public void when_trackedJobCancels_then_jobCompletes() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        Job submittedJob = instance1.newJob(dag);

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // When
        trackedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);
        joinAndExpectCancellation(submittedJob);

        StuckProcessor.proceedLatch.countDown();

        assertCompletedEventually(trackedJob);
        assertCompletedEventually(submittedJob);
    }

    @Test
    public void when_jobIsCompleted_then_trackedJobCanQueryJobResultFromClient() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        JetInstance client = createJetClient();

        Collection<Job> trackedJobs = client.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        StuckProcessor.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByName() throws InterruptedException {
        testGetJobByNameWhenJobIsRunning(instance2);
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByNameFromClient() throws InterruptedException {
        testGetJobByNameWhenJobIsRunning(createJetClient());
    }

    private void testGetJobByNameWhenJobIsRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        assertEquals(jobName, job.getName());
        StuckProcessor.executionStarted.await();

        // Then
        Job trackedJob = instance.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertTrueEventually(() -> assertEquals(RUNNING, trackedJob.getStatus()));

        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedById() throws InterruptedException {
        testGetJobByIdWhenJobIsRunning(instance1);
    }

    @Test
    public void when_jobIsRunning_then_itIsQueriedByIdFromClient() throws InterruptedException {
        testGetJobByIdWhenJobIsRunning(createJetClient());
    }

    private void testGetJobByIdWhenJobIsRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        // Then
        Job trackedJob = instance.getJob(job.getId());

        assertNotNull(trackedJob);
        assertEquals(job.getId(), trackedJob.getId());
        assertTrueEventually(() -> assertEquals(RUNNING, trackedJob.getStatus()));

        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance1.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsCompleted_then_itIsQueriedById() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        StuckProcessor.proceedLatch.countDown();
        job.join();

        // Then
        Job trackedJob = instance1.getJob(job.getId());

        assertNotNull(trackedJob);
        assertEquals(job.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_jobIsQueriedByInvalidId_then_noJobIsReturned() {
        assertNull(instance1.getJob(0));
    }

    @Test
    public void when_jobIsQueriedByInvalidIdFromClient_then_noJobIsReturned() {
        assertNull(createJetClient().getJob(0));
    }

    @Test
    public void when_jobsAreRunning_then_lastSubmittedJobIsQueriedByName() throws InterruptedException {
        testGetJobByNameWhenMultipleJobsAreRunning(instance1);
    }


    @Test
    public void when_jobsAreRunning_then_lastSubmittedJobIsQueriedByNameFromClient() throws InterruptedException {
        testGetJobByNameWhenMultipleJobsAreRunning(createJetClient());
    }

    private void testGetJobByNameWhenMultipleJobsAreRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag, config);
        sleepAtLeastMillis(1);
        Job job2 = instance1.newJob(dag, config);
        StuckProcessor.executionStarted.await();

        // Then
        Job trackedJob = instance.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertNotEquals(job1.getId(), trackedJob.getId());
        assertEquals(job2.getId(), trackedJob.getId());
        assertTrueEventually(() -> assertEquals(RUNNING, trackedJob.getStatus()));

        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_jobsAreCompleted_then_lastSubmittedJobIsQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag, config);
        sleepAtLeastMillis(1);
        Job job2 = instance1.newJob(dag, config);

        StuckProcessor.proceedLatch.countDown();
        job1.join();
        job2.join();

        // Then
        Job trackedJob = instance1.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertNotEquals(job1.getId(), trackedJob.getId());
        assertEquals(job2.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());
    }

    @Test
    public void when_lastSubmittedJobIsCompletedBeforePreviouslySubmittedRunningJob_then_itIsQueriedByName() {
        // Given
        DAG dag1 = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
        DAG dag2 = new DAG().vertex(new Vertex("test", new MockPS(Identity::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag1, config);
        sleepAtLeastMillis(1);
        Job job2 = instance1.newJob(dag2, config);
        job2.join();

        // Then
        Job trackedJob = instance1.getJob(jobName);

        assertNotNull(trackedJob);
        assertEquals(jobName, trackedJob.getName());
        assertNotEquals(job1.getId(), trackedJob.getId());
        assertEquals(job2.getId(), trackedJob.getId());
        assertEquals(COMPLETED, trackedJob.getStatus());

        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_jobsAreRunning_then_theyAreQueriedByName() throws InterruptedException {
        testGetJobsByNameWhenJobsAreRunning(instance1);
    }

    @Test
    public void when_jobsAreRunning_then_theyAreQueriedByNameFromClient() throws InterruptedException {
        testGetJobsByNameWhenJobsAreRunning(createJetClient());
    }

    private void testGetJobsByNameWhenJobsAreRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag, config);
        sleepAtLeastMillis(1);
        Job job2 = instance1.newJob(dag, config);
        StuckProcessor.executionStarted.await();

        // Then
        List<Job> jobs = instance.getJobs(jobName);
        assertEquals(2, jobs.size());

        Job trackedJob1 = jobs.get(0);
        Job trackedJob2 = jobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals(jobName, trackedJob1.getName());
        assertTrueEventually(() -> assertEquals(RUNNING, trackedJob1.getStatus()));
        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals(jobName, trackedJob2.getName());
        assertTrueEventually(() -> assertEquals(RUNNING, trackedJob2.getStatus()));

        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_jobsAreCompleted_then_theyAreQueriedByName() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag, config);
        sleepAtLeastMillis(1);
        Job job2 = instance1.newJob(dag, config);
        StuckProcessor.proceedLatch.countDown();
        job1.join();
        job2.join();

        // Then
        List<Job> jobs = instance1.getJobs(jobName);
        assertEquals(2, jobs.size());

        Job trackedJob1 = jobs.get(0);
        Job trackedJob2 = jobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals(jobName, trackedJob1.getName());
        assertEquals(COMPLETED, trackedJob1.getStatus());
        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals(jobName, trackedJob2.getName());
        assertEquals(COMPLETED, trackedJob2.getStatus());
    }

    @Test
    public void when_jobsAreCompletedInReverseOrderOfTheirSubmissionOrder_then_theyAreQueriedByName() {
        // Given
        DAG dag1 = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
        DAG dag2 = new DAG().vertex(new Vertex("test", new MockPS(Identity::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job1 = instance1.newJob(dag1, config);
        sleepAtLeastMillis(1);
        Job job2 = instance1.newJob(dag2, config);
        job2.join();
        StuckProcessor.proceedLatch.countDown();
        job1.join();

        // Then
        List<Job> jobs = instance1.getJobs(jobName);
        assertEquals(2, jobs.size());

        Job trackedJob1 = jobs.get(0);
        Job trackedJob2 = jobs.get(1);

        assertEquals(job2.getId(), trackedJob1.getId());
        assertEquals(COMPLETED, trackedJob1.getStatus());
        assertEquals(job1.getId(), trackedJob2.getId());
        assertEquals(COMPLETED, trackedJob2.getStatus());
    }

    @Test
    public void when_jobIsSubmitted_then_jobSubmissionTimeIsQueried() throws InterruptedException {
        testJobSubmissionTimeWhenJobIsRunning(instance1);
    }

    @Test
    public void when_jobIsRunning_then_jobSubmissionTimeIsQueriedFromClient() throws InterruptedException {
        testJobSubmissionTimeWhenJobIsRunning(createJetClient());
    }

    private void testJobSubmissionTimeWhenJobIsRunning(JetInstance instance) throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        StuckProcessor.executionStarted.await();
        Job trackedJob = instance.getJob("job1");

        // Then
        assertNotNull(trackedJob);
        assertNotEquals(0, job.getSubmissionTime());
        assertNotEquals(0, trackedJob.getSubmissionTime());
        StuckProcessor.proceedLatch.countDown();
    }

    @Test
    public void when_jobIsCompleted_then_jobSubmissionTimeIsQueried() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(StuckProcessor::new, NODE_COUNT)));
        JobConfig config = new JobConfig();
        String jobName = "job1";
        config.setName(jobName);

        // When
        Job job = instance1.newJob(dag, config);
        StuckProcessor.proceedLatch.countDown();
        job.join();
        Job trackedJob = instance1.getJob("job1");

        // Then
        assertNotNull(trackedJob);
        assertNotEquals(0, job.getSubmissionTime());
        assertNotEquals(0, trackedJob.getSubmissionTime());
    }

    private void joinAndExpectCancellation(Job job) {
        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
        }
    }

    private void assertCompletedEventually(Job job) {
        assertTrueEventually(() -> assertEquals(COMPLETED, job.getStatus()));
    }

    private static final class PSThatWaitsOnInit implements ProcessorSupplier {

        public static volatile CountDownLatch initLatch;
        private final DistributedSupplier<Processor> supplier;

        PSThatWaitsOnInit(DistributedSupplier<Processor> supplier) {
            this.supplier = supplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            uncheckRun(() -> initLatch.await());
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());

        }
    }
}
