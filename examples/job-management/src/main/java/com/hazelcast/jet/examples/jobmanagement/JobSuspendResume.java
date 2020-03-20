/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.jobmanagement;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

/**
 * We demonstrate how a job can be submitted to Jet and further managed via the
 * {@link Job} interface.
 */
public class JobSuspendResume {

    public static void main(String[] args) throws InterruptedException {
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .withoutTimestamps()
                .writeTo(Sinks.list("sink"));

        JobConfig jobConfig = new JobConfig();
        // job name is optional
        String jobName = "sampleJob";
        jobConfig.setName(jobName);
        Job job = instance1.newJob(p, jobConfig);

        // printing the job name
        System.out.println("Job '" + job.getName() + "' is submitted.");

        // job status can be queried, let's wait until the job starts running
        waitForStatus(job, JobStatus.RUNNING);

        // we can suspend the job
        Thread.sleep(1000);
        System.out.println("Suspending the job...");
        job.suspend();
        waitForStatus(job, JobStatus.SUSPENDED);

        // now, the job is not running and can be resumed later
        Thread.sleep(1000);
        System.out.println("Resuming the job...");
        job.resume();
        waitForStatus(job, JobStatus.RUNNING);

        // we can cancel the job
        Thread.sleep(1000);
        System.out.println("Cancelling the job...");
        job.cancel();

        try {
            // let's wait until execution of the job is completed on the cluster
            // we can also call job.getFuture().get()
            job.join();
            assert false;
        } catch (CancellationException e) {
            System.out.println("Job is cancelled.");
        }

        // Let's query the job status again. Now the status is FAILED.
        // It is expected status for streaming jobs when they are cancelled.
        System.out.println("Status: " + job.getStatus());

        instance1.getCluster().shutdown();
    }

    private static void waitForStatus(Job job, JobStatus expectedStatus) throws InterruptedException {
        for (JobStatus status; (status = job.getStatus()) != expectedStatus;) {
            System.out.println("Job is " + status + "...");
            Thread.sleep(10);
        }
        System.out.println("Job is " + expectedStatus + ".");
    }
}
