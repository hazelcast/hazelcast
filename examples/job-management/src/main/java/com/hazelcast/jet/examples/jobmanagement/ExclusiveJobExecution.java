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
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.list;

/**
 * We demonstrate how just one running instance of a named job can be created
 * in the cluster with multiple submissions. The first submission call creates
 * the job and the other submission calls will return the same job.
 * This behavior is useful for micro-service deployments where Jet is used
 * in the embedded mode. Users deploy the same self-contained package into
 * multiple instances and each instance submits the same job during startup.
 */
public class ExclusiveJobExecution {

    public static void main(String[] args) {
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .withoutTimestamps()
                .writeTo(list("sink"));

        JobConfig jobConfig = new JobConfig().setName("namedJob");

        // Submit the same job from multiple Jet instances
        Job job1 = instance1.newJobIfAbsent(p, jobConfig);
        Job job2 = instance2.newJobIfAbsent(p, jobConfig);

        // Both Job handles refer to the same job
        assert job1.getId() == job2.getId();

        try {
            // If I try to submit the same job with newJob(), I will get an exception
            instance1.newJob(p, jobConfig);
            assert false;
        } catch (JobAlreadyExistsException ignored) {
        }

        job1.cancel();

        try {
            job1.join();
        } catch (CancellationException ignored) {
        }

        // Now I can submit a new job with the same name because there is
        // no active job with that name
        Job job3 = instance1.newJobIfAbsent(p, jobConfig);

        // This is a new job instance
        assert job3.getId() != job1.getId();

        job3.cancel();
        try {
            job3.join();
        } catch (CancellationException ignored) {
        }

        instance1.getCluster().shutdown();
    }

}
