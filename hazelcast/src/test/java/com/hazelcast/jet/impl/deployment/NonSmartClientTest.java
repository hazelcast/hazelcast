/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class NonSmartClientTest extends JetTestSupport {

    private JetInstance client;
    private JetInstance instance;

    @Before
    public void setUp() {
        Config config = new Config();
        config.getMapConfig("journal*").getEventJournalConfig().setEnabled(true);
        instance = createJetMember(config);
        JetInstance jetInstance = createJetMember(config);
        Address address = jetInstance.getCluster().getLocalMember().getAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.setClusterName(config.getClusterName());
        clientConfig.getNetworkConfig().getAddresses().clear();
        clientConfig.getNetworkConfig().getAddresses().add(address.getHost() + ":" + address.getPort());
        client = createJetClient(clientConfig);
    }

    @Test
    public void when_jobSubmitted_then_executedSuccessfully() {
        //Given
        String sourceName = "source";
        String sinkName = "sink";
        fillListWithInts(instance.getList(sourceName), 10);

        //When
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(sourceName))
         .writeTo(Sinks.list(sinkName));
        client.newJob(p).join();

        //Then
        assertEquals(10, instance.getList(sinkName).size());
    }

    @Test
    public void when_jobSubmitted_then_jobCanBeFetchedByIdOrName() {
        //Given
        String jobName = randomName();

        //When
        Pipeline p = streamingPipeline();
        Job job = client.newJob(p, new JobConfig().setName(jobName));

        long jobId = job.getId();

        //Then
        assertTrueEventually(() -> {
            assertNotNull(client.getJob(jobId));
            assertNotNull(client.getJob(jobName));
            assertTrue(client.getJobs().stream().anyMatch(j -> j.getId() == jobId));
            assertFalse(client.getJobs(jobName).isEmpty());
            assertNotNull(client.getJob(jobId).getStatus());
            assertEquals(client.getJob(jobId).getStatus(), JobStatus.RUNNING);
            Job j = client.getJob(jobName);
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
        assertJobStatusEventually(client.getJob(job.getName()), JobStatus.SUSPENDED);
    }

    @Test
    public void when_jobResumed_then_jobStatusIsRunning() {
        //Given
        Job job = startJobAndVerifyItIsRunning();
        job.suspend();
        String jobName = job.getName();
        assertJobStatusEventually(client.getJob(jobName), JobStatus.SUSPENDED);

        //When
        job.resume();

        //Then
        assertJobStatusEventually(client.getJob(jobName), JobStatus.RUNNING);
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsCompleted() {
        //Given
        Job job = startJobAndVerifyItIsRunning();

        //When
        job.cancel();

        //Then
        assertJobStatusEventually(client.getJob(job.getName()), JobStatus.FAILED);
    }

    @Test
    public void when_jobSummaryListIsAsked_then_jobSummaryListReturned() {
        //Given
        startJobAndVerifyItIsRunning();

        //When
        List<JobSummary> summaryList = ((JetClientInstanceImpl) client).getJobSummaryList();

        //Then
        assertNotNull(summaryList);
        assertEquals(1, summaryList.size());
    }

    private Job startJobAndVerifyItIsRunning() {
        String jobName = randomName();
        Pipeline p = streamingPipeline();
        Job job = client.newJob(p, new JobConfig().setName(jobName));
        assertJobStatusEventually(client.getJob(jobName), JobStatus.RUNNING);
        return job;
    }

    private Pipeline streamingPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal("journal" + randomMapName(), JournalInitialPosition.START_FROM_OLDEST))
         .withoutTimestamps()
         .writeTo(Sinks.map(randomMapName()));
        return p;
    }
}
