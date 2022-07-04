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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.TestProcessors.MockP;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobTimeoutTest extends JetTestSupport {

    @Test
    public void when_lightJobIsCompletedAfterTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final DAG dag = new DAG();
        dag.newVertex("stuck", () -> new MockP().streaming());
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1L);
        final Job job = hz.getJet().newLightJob(dag, jobConfig);

        assertThrows(CancellationException.class, job::join);
        assertEquals(JobStatus.FAILED, job.getStatus());
    }

    @Test
    public void when_jobIsCompletedAfterTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final DAG dag = new DAG();
        dag.newVertex("stuck", () -> new MockP().streaming());
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1L);
        final Job job = hz.getJet().newJob(dag, jobConfig);

        assertThrows(CancellationException.class, job::join);
        assertEquals(JobStatus.FAILED, job.getStatus());
    }

    @Test
    public void when_lightJobIsCompletedBeforeTimeout_jobIsNotCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final DAG dag = new DAG();
        dag.newVertex("normal", MockP::new);
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newLightJob(dag, jobConfig);

        job.join();
        assertEquals(JobStatus.COMPLETED, job.getStatus());
    }

    @Test
    public void when_jobIsCompletedBeforeTimeout_jobIsNotCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final DAG dag = new DAG();
        dag.newVertex("normal", MockP::new);
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newJob(dag, jobConfig);

        job.join();
        assertEquals(JobStatus.COMPLETED, job.getStatus());
    }

    @Test
    public void when_jobIsResumedAndExceedsTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final DAG dag = new DAG();
        dag.newVertex("stuck", () -> new MockP().streaming());
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newJob(dag, jobConfig);

        assertJobStatusEventually(job, JobStatus.RUNNING, 1);
        job.suspend();

        assertJobStatusEventually(job, JobStatus.SUSPENDED, 1);
        job.resume();

        assertThrows(CancellationException.class, job::join);
        assertEquals(JobStatus.FAILED, job.getStatus());
    }

    @Test
    public void when_jobIsSuspendedAndExceedsTimeout_jobIsCancelled() {
        final HazelcastInstance hz = createHazelcastInstance();
        final DAG dag = new DAG();
        dag.newVertex("stuck", () -> new MockP().streaming());
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(1000L);
        final Job job = hz.getJet().newJob(dag, jobConfig);

        assertJobStatusEventually(job, JobStatus.RUNNING, 1);
        job.suspend();

        assertJobStatusEventually(job, JobStatus.SUSPENDED, 1);

        assertThrows(CancellationException.class, job::join);
        assertEquals(JobStatus.FAILED, job.getStatus());
    }
}
