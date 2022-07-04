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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.TestProcessors.streamingDag;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NonSmartClientTest extends SimpleTestInClusterSupport {

    private static HazelcastInstance masterInstance;
    private static HazelcastInstance nonMasterInstance;
    private static HazelcastInstance masterClient;
    private static HazelcastInstance nonMasterClient;

    @BeforeClass
    public static void setUp() {
        initialize(2, null);
        masterInstance = instances()[0];
        nonMasterInstance = instances()[1];
        masterClient = createClientConnectingTo(masterInstance);
        nonMasterClient = createClientConnectingTo(nonMasterInstance);
    }

    private static HazelcastInstance createClientConnectingTo(HazelcastInstance targetInstance) {
        Address address = targetInstance.getCluster().getLocalMember().getAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().getAddresses().clear();
        clientConfig.getNetworkConfig().getAddresses().add(address.getHost() + ":" + address.getPort());
        return factory().newHazelcastClient(clientConfig);
    }

    @Test
    public void when_jobSubmitted_then_executedSuccessfully() {
        //Given
        String sourceName = "source";
        String sinkName = "sink";
        fillListWithInts(masterInstance.getList(sourceName), 10);

        //When
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(sourceName))
         .writeTo(Sinks.list(sinkName));
        nonMasterClient.getJet().newJob(p).join();

        //Then
        assertEquals(10, masterInstance.getList(sinkName).size());
    }

    @Test
    public void when_jobSubmitted_then_jobCanBeFetchedByIdOrName() {
        //Given
        String jobName = randomName();

        //When
        DAG dag = streamingDag();
        JetService jet = nonMasterClient.getJet();
        Job job = jet.newJob(dag, new JobConfig().setName(jobName));

        long jobId = job.getId();

        //Then
        assertTrueEventually(() -> {
            assertNotNull(jet.getJob(jobId));
            assertNotNull(jet.getJob(jobName));
            assertTrue(jet.getJobs().stream().anyMatch(j -> j.getId() == jobId));
            assertFalse(jet.getJobs(jobName).isEmpty());
            assertNotNull(jet.getJob(jobId).getStatus());
            assertEquals(jet.getJob(jobId).getStatus(), JobStatus.RUNNING);
            Job j = jet.getJob(jobName);
            assertNotNull(j.getConfig());
            assertGreaterOrEquals("submissionTime", j.getSubmissionTime(), 0);
        }, 10);
    }


    @Test
    public void when_jobSuspended_then_jobStatusIsSuspended() {
        //Given
        Job job = startJobAndVerifyItIsRunning();

        //When
        job.suspend();

        //Then
        assertJobStatusEventually(nonMasterClient.getJet().getJob(job.getName()), JobStatus.SUSPENDED);
    }

    @Test
    public void when_jobResumed_then_jobStatusIsRunning() {
        //Given
        Job job = startJobAndVerifyItIsRunning();
        job.suspend();
        String jobName = job.getName();
        assertJobStatusEventually(nonMasterClient.getJet().getJob(jobName), JobStatus.SUSPENDED);

        //When
        job.resume();

        //Then
        assertJobStatusEventually(nonMasterClient.getJet().getJob(jobName), JobStatus.RUNNING);
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsCompleted() {
        //Given
        Job job = startJobAndVerifyItIsRunning();

        //When
        job.cancel();

        //Then
        assertJobStatusEventually(nonMasterClient.getJet().getJob(job.getName()), JobStatus.FAILED);
    }

    @Test
    public void when_jobSummaryListIsAsked_then_jobSummaryListReturned() {
        //Given
        Job job = startJobAndVerifyItIsRunning();

        //When
        List<JobSummary> summaryList = ((JetClientInstanceImpl) nonMasterClient.getJet()).getJobSummaryList();

        //Then
        assertNotNull(summaryList);
        assertTrue(summaryList.stream().anyMatch(s -> s.getJobId() == job.getId()));
    }

    @Test
    public void when_lightJobSubmittedToNonMaster_then_coordinatedByNonMaster() {
        Job job = nonMasterClient.getJet().newLightJob(streamingDag());
        JetServiceBackend service = getNodeEngineImpl(nonMasterInstance).getService(JetServiceBackend.SERVICE_NAME);
        assertTrueEventually(() ->
                assertNotNull(service.getJobCoordinationService().getLightMasterContexts().get(job.getId())));
    }

    @Test
    public void when_lightJobSubmittedToNonMaster_then_accessibleFromAllMembers() {
        Job job = nonMasterClient.getJet().newLightJob(streamingDag());
        assertTrueEventually(() -> {
            Job trackedJob = masterInstance.getJet().getJob(job.getId());
            assertNotNull(trackedJob);
            assertEquals(JobStatus.RUNNING, trackedJob.getStatus());
        });

        Job job1 = masterClient.getJet().getJob(job.getId());
        Job job2 = nonMasterClient.getJet().getJob(job.getId());
        assertNotNull(job1);
        assertNotNull(job2);
        assertNotEquals(0, job1.getSubmissionTime());
        assertEquals(job1.getSubmissionTime(), job2.getSubmissionTime());
        assertFalse(job1.getFuture().isDone());
        assertFalse(job2.getFuture().isDone());
        // cancel requested through client connected to the master, but job is coordinated by the other member
        job1.cancel();
        try {
            job1.join();
            fail("join didn't fail");
        } catch (CancellationException ignored) { }
    }

    private Job startJobAndVerifyItIsRunning() {
        String jobName = randomName();
        DAG dag = streamingDag();
        Job job = nonMasterClient.getJet().newJob(dag, new JobConfig().setName(jobName));
        assertJobStatusEventually(nonMasterClient.getJet().getJob(jobName), JobStatus.RUNNING);
        return job;
    }
}
