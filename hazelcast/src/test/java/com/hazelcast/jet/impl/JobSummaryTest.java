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

package com.hazelcast.jet.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.Util.idToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobSummaryTest extends JetTestSupport {

    private static final String SOURCE_NAME = "source";
    private JetInstance[] instances;
    private JetInstance instance;
    private JetClientInstanceImpl client;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        MapConfig mapConfig = new MapConfig(SOURCE_NAME);
        mapConfig.getEventJournalConfig().setEnabled(true);
        config.addMapConfig(mapConfig);
        instances = createJetMembers(config, 2);
        instance = instances[0];
        client = (JetClientInstanceImpl) createJetClient();
    }

    @Test
    public void when_noJobsRunning() {
        assertEquals(0, client.getJobSummaryList().size());
    }

    @Test
    public void when_batchJob() {
        Job job = instance.newJob(newBatchPipeline(), new JobConfig().setName("jobA"));
        job.join();

        List<JobSummary> list = client.getJobSummaryList();
        assertEquals(1, list.size());
        JobSummary jobSummary = list.get(0);

        assertEquals("jobA", jobSummary.getNameOrId());
        assertEquals(job.getId(), jobSummary.getJobId());
        assertEquals(JobStatus.COMPLETED, jobSummary.getStatus());
        assertNull(jobSummary.getFailureText());
    }

    @Test
    public void when_streamingJobLifecycle() {
        Job job = instance.newJob(newStreamPipeline(), new JobConfig().setName("jobA"));
        List<JobSummary> list = client.getJobSummaryList();
        assertEquals(1, list.size());
        JobSummary jobSummary = list.get(0);

        assertEquals("jobA", jobSummary.getNameOrId());
        assertEquals(job.getId(), jobSummary.getJobId());

        assertTrueEventually(() -> {
            JobSummary summary = client.getJobSummaryList().get(0);
            assertEquals(JobStatus.RUNNING, summary.getStatus());
        }, 20);

        job.suspend();

        assertTrueEventually(() -> {
            JobSummary summary = client.getJobSummaryList().get(0);
            assertEquals(JobStatus.SUSPENDED, summary.getStatus());
        }, 20);

        job.resume();

        assertTrueEventually(() -> {
            JobSummary summary = client.getJobSummaryList().get(0);
            assertEquals(JobStatus.RUNNING, summary.getStatus());
        }, 20);

        job.cancel();

        assertTrueEventually(() -> {
            JobSummary summary = client.getJobSummaryList().get(0);
            assertEquals(JobStatus.FAILED, summary.getStatus());
            assertEquals(0, summary.getExecutionId());
        }, 20);
    }

    @Test
    public void when_manyJobs_then_sortedBySubmissionTime() {
        int numJobs = 10;

        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < numJobs; i++) {
            // half of the jobs will be light, half normal
            boolean useLightJob = i % 2 == 0;
            Pipeline p = newStreamPipeline();
            Job job;
            if (useLightJob) {
                // i % 4 / 2: submit every other light job to a different instance to have different coordinators
                job = instances[i % 4 / 2].newLightJob(p);
                // We need to assert this for light jobs as newLightJob returns immediately before the
                // SubmitOp is handled
                assertJobRunningEventually(instance, job, null);
            } else {
                job = instance.newJob(p);
            }
            jobs.add(job);
        }

        assertTrueEventually(() -> {
            List<JobSummary> list = new ArrayList<>(client.getJobSummaryList());
            assertEquals(numJobs, list.size());

            Collections.reverse(list);
            // jobs are sorted by submission time in descending order
            for (int i = 0; i < numJobs; i++) {
                JobSummary summary = list.get(i);
                assertEquals(idToString(summary.getJobId()), summary.getNameOrId());
                assertEquals(JobStatus.RUNNING, summary.getStatus());
            }
        }, 20);

        jobs.forEach(Job::cancel);

        assertTrueEventually(() -> {
            List<JobSummary> list = new ArrayList<>(client.getJobSummaryList());
            // numJobs / 2: only the normal jobs
            assertEquals(numJobs / 2, list.size());

            Collections.reverse(list);

            // jobs should still be sorted by submission time in descending order, light jobs are missing
            for (int i = 0; i < numJobs; i++) {
                boolean useLightJob = i % 2 == 0;
                if (useLightJob) {
                    // light jobs aren't included after cancellation
                    continue;
                }
                JobSummary summary = list.get(i / 2);
                assertEquals(idToString(summary.getJobId()), summary.getNameOrId());
                assertEquals(JobStatus.FAILED, summary.getStatus());
                assertNotEquals(0, summary.getCompletionTime());
            }
        }, 20);
    }

    @Test
    public void when_job_failed() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal("invalid", JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                .writeTo(Sinks.noop());
        Job job = instance.newJob(p, new JobConfig().setName("jobA"));
        String msg = "";
        try {
            job.join();
        } catch (Exception e) {
            msg = e.getMessage();
        }
        List<JobSummary> list = client.getJobSummaryList();
        assertEquals(1, list.size());
        JobSummary jobSummary = list.get(0);

        assertContains(new JetException(jobSummary.getFailureText()).toString(), msg);
        assertNotEquals(0, jobSummary.getCompletionTime());
    }

    private Pipeline newStreamPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(SOURCE_NAME, JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                .writeTo(Sinks.noop());
        return p;
    }

    private Pipeline newBatchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(SOURCE_NAME))
                .writeTo(Sinks.noop());
        return p;
    }
}
