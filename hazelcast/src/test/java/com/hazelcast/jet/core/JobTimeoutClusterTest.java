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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({SlowTest.class})
public class JobTimeoutClusterTest extends JetTestSupport {

    @Test
    public void when_masterFails_timedOutJobIsCancelled() {
        final HazelcastInstance[] instances = createHazelcastInstances(2);
        final HazelcastInstance oldMaster = instances[0];
        final HazelcastInstance newMaster = instances[1];

        assertClusterSizeEventually(2, newMaster);
        assertClusterStateEventually(ClusterState.ACTIVE, newMaster);

        final DAG dag = new DAG();
        dag.newVertex("stuck", () -> new MockP().streaming());
        final JobConfig jobConfig = new JobConfig().setTimeoutMillis(10000L)
                .setSnapshotIntervalMillis(1L)
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        final Job job = oldMaster.getJet().newJob(dag, jobConfig);
        final long jobId = job.getId();

        // start and wait for the job to start running
        assertJobStatusEventually(job, JobStatus.RUNNING);
        final JobRepository oldJobRepository = new JobRepository(oldMaster);
        assertTrueEventually(() -> {
            final JobExecutionRecord record = oldJobRepository.getJobExecutionRecord(jobId);
            assertTrue(record.snapshotId() > 0);
        });

        // kill old master and wait for the cluster to reconfigure
        oldMaster.getLifecycleService().terminate();
        assertClusterStateEventually(ClusterState.ACTIVE, newMaster);
        assertClusterSize(1, newMaster);

        // wait for the job to be restarted and cancelled due to timeout
        final Job restartedJob = newMaster.getJet().getJob(jobId);
        assertNotNull(restartedJob);
        assertJobStatusEventually(restartedJob, JobStatus.FAILED);
    }
}
