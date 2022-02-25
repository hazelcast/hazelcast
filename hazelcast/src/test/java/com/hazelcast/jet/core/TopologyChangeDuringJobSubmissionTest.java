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
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopologyChangeDuringJobSubmissionTest extends JetTestSupport {

    private static final int PARALLELISM = 1;

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        NoOutputSourceP.executionStarted = new CountDownLatch(PARALLELISM);

        instance1 = createHazelcastInstance(smallInstanceConfig().setLiteMember(true));
        instance2 = createHazelcastInstance();

        warmUpPartitions(instance1, instance2);
    }

    @Test
    public void when_coordinatorLeavesDuringSubmission_then_submissionCallReturnsSuccessfully() throws Throwable {
        // Given that the job has submitted
        dropOperationsBetween(instance1, instance2,
                SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.NORMAL_RESPONSE));

        Future<Job> future = spawn(() -> {
            DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, 1)));
            return instance2.getJet().newJob(dag);
        });

        NoOutputSourceP.executionStarted.await();

        // When the coordinator leaves before the submission response is received
        instance1.getLifecycleService().terminate();
        Job job = future.get();

        // Then the job completes successfully
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertEquals(2, MockPS.initCount.get());
    }

    @Test
    public void when_jobIsCompletedBeforeSubmissionCallReturns_then_jobRunsOnlyOnce() throws Throwable {
        // Given that the job is already completed
        String jobName = "job1";
        Future<Job> future = spawn(() -> {
            DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, 1)));
            return instance2.getJet().newJob(dag, new JobConfig().setName(jobName));
        });

        NoOutputSourceP.executionStarted.await();

        dropOperationsBetween(instance1, instance2,
                SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.NORMAL_RESPONSE));

        Job submittedJob = instance1.getJet().getJob(jobName);
        assertNotNull(submittedJob);
        NoOutputSourceP.proceedLatch.countDown();

        submittedJob.join();

        // When the coordinator leaves before the submission response is received
        instance1.getLifecycleService().terminate();
        Job job = future.get();

        // Then the job does not run for the second time
        job.join();
        assertEquals(1, MockPS.initCount.get());
    }

}
