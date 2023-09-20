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

package com.hazelcast.jet.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.idToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobSummaryTest extends JetTestSupport {

    private static final String SOURCE_NAME = "source";
    private HazelcastInstance[] instances;
    private HazelcastInstance instance;
    private HazelcastInstance client;

    @Parameterized.Parameter
    public boolean useOldJobSummary;

    @Parameterized.Parameters(name = "useOldJobSummary={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        MapConfig mapConfig = new MapConfig(SOURCE_NAME);
        mapConfig.getEventJournalConfig().setEnabled(true);
        config.addMapConfig(mapConfig);
        instances = createHazelcastInstances(config, 2);
        instance = instances[0];
        client = createHazelcastClient();
    }

    @Test
    public void when_noJobsRunning() {
        assertEquals(0, getJobSummaryList().size());
    }

    @Test
    public void when_batchJob() {
        Job job = instance.getJet().newJob(newBatchPipeline(), new JobConfig().setName("jobA"));
        job.join();

        List<JobAndSqlSummary> list = getJobSummaryList();
        assertEquals(1, list.size());
        JobAndSqlSummary jobSummary = list.get(0);

        assertFalse(jobSummary.isLightJob());
        assertEquals("jobA", jobSummary.getNameOrId());
        assertEquals(job.getId(), jobSummary.getJobId());
        assertEquals(JobStatus.COMPLETED, jobSummary.getStatus());
        assertNull(jobSummary.getFailureText());
        assertTrue(useOldJobSummary || !jobSummary.isUserCancelled());
    }

    @Test
    public void when_batchJob_cancelled() {
        Job job = instance.getJet().newJob(newStreamPipeline(), new JobConfig().setName("jobA"));
        String msg = "";
        job.cancel();
        try {
            job.join();
            fail("Join should throw CancellationByUserException");
        } catch (CancellationByUserException e) {
            msg = e.toString();
        }

        JobAndSqlSummary summary = getJobSummaryList().get(0);
        assertEquals(JobStatus.FAILED, summary.getStatus());
        assertEquals(0, summary.getExecutionId());
        assertTrue(useOldJobSummary || summary.isUserCancelled());
        assertContains(summary.getFailureText(), msg);
    }

    @Test
    public void when_streamingJobLifecycle() {
        Job job = instance.getJet().newJob(newStreamPipeline(), new JobConfig().setName("jobA"));
        List<JobAndSqlSummary> list = getJobSummaryList();
        assertEquals(1, list.size());
        JobAndSqlSummary jobSummary = list.get(0);

        assertFalse(jobSummary.isLightJob());
        assertEquals("jobA", jobSummary.getNameOrId());
        assertEquals(job.getId(), jobSummary.getJobId());

        assertTrueEventually(() -> {
            JobAndSqlSummary summary = getJobSummaryList().get(0);
            assertEquals(JobStatus.RUNNING, summary.getStatus());
            assertTrue(useOldJobSummary || !summary.isUserCancelled());
        }, 20);

        job.suspend();

        assertTrueEventually(() -> {
            JobAndSqlSummary summary = getJobSummaryList().get(0);
            assertEquals(JobStatus.SUSPENDED, summary.getStatus());
            assertTrue(useOldJobSummary || !summary.isUserCancelled());
        }, 20);

        job.resume();

        assertTrueEventually(() -> {
            JobAndSqlSummary summary = getJobSummaryList().get(0);
            assertEquals(JobStatus.RUNNING, summary.getStatus());
            assertTrue(useOldJobSummary || !summary.isUserCancelled());
        }, 20);

        job.cancel();

        assertTrueEventually(() -> {
            JobAndSqlSummary summary = getJobSummaryList().get(0);
            assertEquals(JobStatus.FAILED, summary.getStatus());
            assertEquals(0, summary.getExecutionId());
            assertTrue(useOldJobSummary || summary.isUserCancelled());
            assertContains(summary.getFailureText(), new CancellationByUserException().toString());
        }, 20);
    }

    @Test
    public void when_lightJob() throws InterruptedException {
        Job job = instance.getJet().newLightJob(newStreamPipeline());
        // wait till jobs are scanned
        Thread.sleep(200);

        assertTrueEventually(() -> assertEquals(1, getJobSummaryList().size()));
        JobAndSqlSummary jobSummary = getJobSummaryList().get(0);

        assertTrue(jobSummary.isLightJob());
        assertEquals(idToString(job.getId()), jobSummary.getNameOrId());
        assertEquals(job.getId(), jobSummary.getJobId());
        assertEquals(JobStatus.RUNNING, jobSummary.getStatus());
        assertTrue(useOldJobSummary || !jobSummary.isUserCancelled());
    }

    @Test
    @Ignore("Flaky due to race condition described in JobCoordinationService.getJobAndSqlSummary(LightMasterContext)")
    // To see it failing add some delay (eg. 100ms) in JobCoordinationService.submitLightJob in mc.getCompletionFuture().whenComplete invocation
    public void when_lightJobIsCancelled_then_itIsNotReportedOnList() throws InterruptedException {
        // given
        Job job = instance.getJet().newLightJob(newStreamPipeline());
        // wait till jobs are scanned
        Thread.sleep(200);

        assertTrueEventually(() -> assertEquals(1, getJobSummaryList().size()));

        // when
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(job::cancel, 100, TimeUnit.MILLISECONDS);

        try {
            assertThatThrownBy(() -> getJetClientInstanceImpl(client).getJob(job.getId()).join())
                    .isInstanceOf(CancellationException.class);

            // then
            List<JobAndSqlSummary> list = getJobSummaryList();
            assertThat(list).isEmpty();
        } finally {
            executorService.shutdownNow();
        }
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
                job = instances[i % 4 / 2].getJet().newLightJob(p);
                // We need to assert this for light jobs as newLightJob returns immediately before the
                // SubmitOp is handled
                assertJobRunningEventually(instance, job, null);
            } else {
                job = instance.getJet().newJob(p);
            }
            jobs.add(job);
        }

        assertTrueEventually(() -> {
            List<JobAndSqlSummary> list = new ArrayList<>(getJobSummaryList());
            assertEquals(numJobs, list.size());

            Collections.reverse(list);
            // jobs are sorted by submission time in descending order
            for (int i = 0; i < numJobs; i++) {
                JobAndSqlSummary summary = list.get(i);
                assertEquals(idToString(summary.getJobId()), summary.getNameOrId());
                assertEquals(JobStatus.RUNNING, summary.getStatus());
            }
        }, 20);

        jobs.forEach(Job::cancel);

        assertTrueEventually(() -> {
            List<JobAndSqlSummary> list = new ArrayList<>(getJobSummaryList());
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
                JobAndSqlSummary summary = list.get(i / 2);
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
        Job job = instance.getJet().newJob(p, new JobConfig().setName("jobA"));
        String msg = "";
        try {
            job.join();
        } catch (CancellationException cancelled) {
            fail("Job should not be cancelled in this test");
        } catch (Exception e) {
            msg = e.getMessage();
        }
        List<JobAndSqlSummary> list = getJobSummaryList();
        assertEquals(1, list.size());
        JobAndSqlSummary jobSummary = list.get(0);

        assertContains(new JetException(jobSummary.getFailureText()).toString(), msg);
        assertNotEquals(0, jobSummary.getCompletionTime());
        assertEquals(JobStatus.FAILED, jobSummary.getStatus());
        assertTrue(useOldJobSummary || !jobSummary.isUserCancelled());
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

    private JetClientInstanceImpl getJetClientInstanceImpl(HazelcastInstance client) {
        return (JetClientInstanceImpl) client.getJet();
    }

    @Nonnull
    @SuppressWarnings("deprecation")
    private List<JobAndSqlSummary> getJobSummaryList() {
        if (useOldJobSummary) {
            // adapt old result to new type, so it can be tested in a consistent way
            return getJetClientInstanceImpl(client).getJobSummaryList().stream()
                    .map(jobAndSqlSummary -> new JobAndSqlSummary(
                            jobAndSqlSummary.isLightJob(),
                            jobAndSqlSummary.getJobId(),
                            jobAndSqlSummary.getExecutionId(),
                            jobAndSqlSummary.getNameOrId(),
                            jobAndSqlSummary.getStatus(),
                            jobAndSqlSummary.getSubmissionTime(),
                            jobAndSqlSummary.getCompletionTime(),
                            jobAndSqlSummary.getFailureText(),
                            null,
                            null,
                            false))
                    .collect(Collectors.toList());
        } else {
            return getJetClientInstanceImpl(client).getJobAndSqlSummaryList();
        }
    }
}
