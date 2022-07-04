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
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ParallelStressTest extends JetTestSupport {

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(2);

    @Test
    public void stressTest_parallelJobSubmissionAndCompletion() throws Exception {
        /*
        This test tests the issue #1339 (https://github.com/hazelcast/hazelcast-jet/issues/1339)
        There's no assert in this test. If the problem reproduces, the jobs won't complete and will
        get stuck.
         */
        DAG dag = new DAG();
        dag.newVertex("p", TestProcessors.ListSource.supplier(Arrays.asList(1, 2, 3)));

        HazelcastInstance instance = createHazelcastInstance();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Job>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(executor.submit(() -> instance.getJet().newJob(dag)));
        }
        for (Future<Job> future : futures) {
            future.get().join();
        }
    }

    @Test
    public void stressTest_parallelSnapshots() {
        /*
        This test tests an issue similar to #1339 (https://github.com/hazelcast/hazelcast-jet/issues/1339).
        `onSnapshotCompleted` used to run on an async thread and it called `IMap.clear()`, which deadlocks
        in imdg 3.12.
        There's no assert in this test. If the problem reproduces, the jobs won't get cancelled and will
        get stuck.
         */
        DAG dag = new DAG();
        dag.newVertex("p", TestProcessors.DummyStatefulP::new);
        HazelcastInstance instance = createHazelcastInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(0).setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            jobs.add(instance.getJet().newJob(dag, jobConfig));
        }
        sleepSeconds(3);
        for (Job job : jobs) {
            cancelAndJoin(job);
        }
    }
}
