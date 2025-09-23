/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.job;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class JobOperationFaultBounceTest {

    private static final int TEST_DURATION_SECONDS = 30;

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(this::getConfig)
                    .clusterSize(3)
                    .driverCount(1)
                    .driverType(BounceTestConfiguration.DriverType.CLIENT)
                    .avoidOverlappingTerminations(true)
                    .useTerminate(false)
                    .bouncingIntervalSeconds(0)
                    .noSteadyMember()
                    .build();

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Before
    public void before() {
        var client = bounceMemberRule.getNextTestDriver();

        DAG streamingDag = new DAG();
        streamingDag.newVertex("v", () -> new TestProcessors.MockP().streaming());

        var job = client.getJet().newJob(streamingDag);
        assertTrueEventually(() -> assertEquals("Job should be running", JobStatus.RUNNING, job.getStatus()));
    }

    @Test
    public void submitOperation() {
        var client = bounceMemberRule.getNextTestDriver();
        Runnable[] tasks = new Runnable[] {
                () -> startJob(client, false)
        };
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
    }

    @Test
    public void clientGetJobs() {
        var client = bounceMemberRule.getNextTestDriver();
        Runnable[] tasks = new Runnable[] {
            () -> client.getJet().getJob("job")
        };
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
    }

    @Test
    public void clientGetAllJobs() {
        var client = bounceMemberRule.getNextTestDriver();
        Runnable[] tasks = new Runnable[] {
                () -> {
                    var jobs = client.getJet().getJobs();
                    assertEquals("Expected exactly one job", 1, jobs.size());
                }
        };
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
    }

    private Job startJob(HazelcastInstance hz, boolean isLight) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemsDistributed(List.of(IntStream.range(0, 5).boxed().toArray(Integer[]::new))))
                .map(e -> e)
                .writeTo(Sinks.noop());
        var jet = hz.getJet();
        return isLight ? jet.newLightJob(pipeline) : jet.newJob(pipeline);
    }
}
