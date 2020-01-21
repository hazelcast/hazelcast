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
import com.hazelcast.jet.pipeline.Sources;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.list;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * We demonstrate how a job can be manually scaled out after adding new nodes
 * to the Jet cluster.
 */
public class JobManualRestart {

    public static void main(String[] args) throws InterruptedException {
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .withoutTimestamps()
                .writeTo(list("sink"));

        // disable auto-scaling
        Job job = instance1.newJob(p, new JobConfig().setAutoScaling(false));

        // we wait until the job starts running
        while (job.getStatus() != JobStatus.RUNNING) {
            Thread.sleep(1);
        }

        // we add a new node to the cluster.
        JetInstance instance3 = Jet.newJetInstance();

        // we call the restart() method to scale up the job
        job.restart();

        // from now on, the job is running on 3 nodes
        Thread.sleep(SECONDS.toMillis(10));

        job.cancel();

        instance1.getCluster().shutdown();
    }
}
