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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.core.TestProcessors.ProcessorThatFailsInComplete;
import com.hazelcast.jet.core.TopologyChangeTest.MockSupplier;
import com.hazelcast.jet.core.TopologyChangeTest.StuckProcessor;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JobTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;
    private JetTestInstanceFactory factory;

    @Before
    public void setup() {
        MockSupplier.completeCount.set(0);
        MockSupplier.initCount.set(0);
        MockSupplier.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        factory = new JetTestInstanceFactory();

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getProperties().put(OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));
        config.getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        instance1 = factory.newMember(config);
        instance2 = factory.newMember(config);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void when_jobIsSubmitted_then_jobStatusIsRunningEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        // Then
        assertEquals(RUNNING, job.getJobStatus());

        job.cancel();
        joinAndExpectCancellation(job);
    }

    @Test
    public void when_jobIsCancelled_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        // Then
        job.cancel();
        joinAndExpectCancellation(job);

        assertEquals(COMPLETED, job.getJobStatus());
    }

    @Test
    public void when_jobIsFailed_then_jobStatusIsCompletedEventually() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier((DistributedSupplier<Processor>)
                () -> new ProcessorThatFailsInComplete(new ExpectedRuntimeException()), NODE_COUNT)));

        // When
        Job job = instance1.newJob(dag);

        // Then
        try {
            job.getFuture().get();
            fail();
        } catch (ExecutionException expected) {
            assertEquals(FAILED, job.getJobStatus());
        }
    }

    @Test
    public void when_jobIsSubmitted_then_trackedJobCanQueryJobStatus() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // Then
        assertEquals(RUNNING, trackedJob.getJobStatus());

        submittedJob.cancel();
        joinAndExpectCancellation(submittedJob);
    }

    @Test
    public void when_jobIsCompleted_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        StuckProcessor.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getJobStatus());
    }

    @Test
    public void when_jobIsCancelled_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        Job submittedJob = instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        submittedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);

        assertEquals(COMPLETED, trackedJob.getJobStatus());
    }

    @Test
    public void when_jobIsFailed_then_trackedJobCanQueryJobResult() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier((DistributedSupplier<Processor>)
                () -> new ProcessorThatFailsInComplete(new ExpectedRuntimeException()), NODE_COUNT)));

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
            assertEquals(FAILED, trackedJob.getJobStatus());
        }
    }

    @Test
    public void when_trackedJobCancels_then_jobCompletes() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        Job submittedJob = instance1.newJob(dag);

        Collection<Job> trackedJobs = instance2.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        // When
        trackedJob.cancel();

        // Then
        joinAndExpectCancellation(trackedJob);
        joinAndExpectCancellation(submittedJob);

        assertEquals(COMPLETED, trackedJob.getJobStatus());
        assertEquals(COMPLETED, submittedJob.getJobStatus());
    }

    @Test
    public void when_jobIsCompleted_then_trackedJobCanQueryJobResultFromClient() throws InterruptedException {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT)));

        // When
        instance1.newJob(dag);
        StuckProcessor.executionStarted.await();

        JetClientInstanceImpl client = factory.newClient();

        Collection<Job> trackedJobs = client.getJobs();
        assertEquals(1, trackedJobs.size());
        Job trackedJob = trackedJobs.iterator().next();

        StuckProcessor.proceedLatch.countDown();

        // Then
        trackedJob.join();

        assertEquals(COMPLETED, trackedJob.getJobStatus());
    }

    private void joinAndExpectCancellation(Job job) {
        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
        }
    }

}
