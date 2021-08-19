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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class CompleteExecutionTest extends JetTestSupport {

    private final int NODE_COUNT = 2;
    private final int JOB_COUNT = 100;

    @Test
    @Repeat(100) // This is slow test
    public void shouldFinishAllCloseCallsBeforeTerminationIsFinished() throws Exception {
        TestProcessors.reset(NODE_COUNT);

        Config config = smallInstanceConfig();
        HazelcastInstance instance1 = createHazelcastInstance(config);
        HazelcastInstance instance2 = createHazelcastInstance(config);

        JetService jet = instance1.getJet();
        DAG dag = new DAG().vertex(new Vertex("test", new TestProcessors.MockPS(TestProcessors.NoOutputSourceP::new, NODE_COUNT)));

        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < JOB_COUNT; i++) {
            jobs.add(jet.newJob(dag));
        }
        for (Job job : jobs) {
            assertJobStatusEventually(job, RUNNING);
        }

        // Terminating instance2 first reproduces the issue more easily
        terminateInstance(instance2);
        terminateInstance(instance1);

        int i = TestProcessors.MockPS.closeCount.get();
        assertThat(i).isEqualTo(NODE_COUNT * JOB_COUNT);
    }
}
