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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.DummyStatefulP;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.JobRepository;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.function.Function;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

public class JobRestartStressTestBase extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance instance1;

    @Before
    public void setup() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);

        instance1 = createHazelcastInstance(config);
        createHazelcastInstance(config);
    }

    @SuppressWarnings("WeakerAccess") // has sub-classes in jet-enterprise
    protected void stressTest(Function<Tuple3<HazelcastInstance, DAG, Job>, Job> action) throws Exception {
        JobRepository jobRepository = new JobRepository(instance1);
        TestProcessors.reset(2);

        DAG dag = new DAG();
        dag.newVertex("dummy-stateful-p", DummyStatefulP::new)
           .localParallelism(1);

        Job[] job = {instance1.getJet().newJob(dag,
                new JobConfig().setSnapshotIntervalMillis(10)
                               .setProcessingGuarantee(EXACTLY_ONCE))};

        logger.info("waiting for 1st snapshot");
        waitForFirstSnapshot(jobRepository, job[0].getId(), 5, false);
        logger.info("first snapshot found");
        for (int i = 0; i < 10; i++) {
            job[0] = action.apply(tuple3(instance1, dag, job[0]));
            waitForNextSnapshot(jobRepository, job[0].getId(), 5, false);
        }

        cancelAndJoin(job[0]);
    }
}
