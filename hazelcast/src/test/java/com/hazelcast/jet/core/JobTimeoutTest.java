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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestProcessors.batchDag;
import static com.hazelcast.jet.core.TestProcessors.streamingDag;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobTimeoutTest extends JetTestSupport {

    @Test
    public void when_lightJobIsCompletedAfterTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1L);
        final Job job = hz.getJet().newLightJob(streamingDag(), jobConfig);

        assertThrows(CancellationException.class, job::join);
        assertEquals(FAILED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobIsCompletedAfterTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1L);
        final Job job = hz.getJet().newJob(streamingDag(), jobConfig);

        assertThrows(CancellationException.class, job::join);
        assertEquals(FAILED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_lightJobIsCompletedBeforeTimeout_jobIsNotCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newLightJob(batchDag(), jobConfig);

        job.join();
        assertEquals(COMPLETED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobIsCompletedBeforeTimeout_jobIsNotCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newJob(batchDag(), jobConfig);

        job.join();
        assertEquals(COMPLETED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobIsResumedAndExceedsTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newJob(streamingDag(), jobConfig);

        // If the job times out before or during execution of these operations
        // catch and ignore the error to continue testing. The job should eventually finish with CancellationException
        // due to timeout
        try {
            assertJobStatusEventually(job, RUNNING, 10);
            job.suspend();

            assertJobStatusEventually(job, SUSPENDED, 10);
            job.resume();
        } catch (AssertionError | IllegalStateException ignored) {
        }

        assertThrows(CancellationException.class, job::join);
        assertEquals(FAILED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobIsSuspendedAndExceedsTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newJob(streamingDag(), jobConfig);

        // If the job times out before or during execution of these operations
        // catch and ignore the error to continue testing. The job should eventually finish with CancellationException
        // due to timeout
        try {
            assertJobStatusEventually(job, RUNNING);
            job.suspend();

            assertJobStatusEventually(job, SUSPENDED);
        } catch (AssertionError | IllegalStateException ignored) {
        }
        assertThrows(CancellationException.class, job::join);
        assertEquals(FAILED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }

    @Test
    public void when_jobTimeoutIsSetLater_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final Job job = hz.getJet().newJob(streamingDag());

        assertJobStatusEventually(job, RUNNING);
        job.suspend();

        assertJobStatusEventually(job, SUSPENDED);
        job.updateConfig(new DeltaJobConfig().setTimeoutMillis(1000));
        job.resume();

        assertThrows(CancellationException.class, job::join);
        assertEquals(FAILED, job.getStatus());
        assertFalse(job.isUserCancelled());
    }
}
