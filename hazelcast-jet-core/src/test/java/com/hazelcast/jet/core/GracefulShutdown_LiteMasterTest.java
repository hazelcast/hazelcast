/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.DummyStatefulP;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.DAYS;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class GracefulShutdown_LiteMasterTest extends JetTestSupport {

    private JetInstance instance;
    private JetInstance liteMaster;

    @Before
    public void setup() {
        TestProcessors.reset(0);
        JetConfig liteMemberConfig = new JetConfig();
        liteMemberConfig.getHazelcastConfig().setLiteMember(true);
        liteMaster = createJetMember(liteMemberConfig);
        instance = createJetMember();
    }

    @Test
    public void test() {
        DummyStatefulP.parallelism = 2;
        DAG dag = new DAG();
        dag.newVertex("v", (DistributedSupplier<Processor>) DummyStatefulP::new)
           .localParallelism(DummyStatefulP.parallelism);
        Job job = instance.newJob(dag, new JobConfig()
                .setSnapshotIntervalMillis(DAYS.toMillis(1))
                .setProcessingGuarantee(EXACTLY_ONCE));
        assertJobStatusEventually(job, RUNNING, 10);
        DummyStatefulP.wasRestored = false;
        liteMaster.shutdown();
        assertJobStatusEventually(job, RUNNING, 10);
        assertTrueEventually(() -> assertTrue("snapshot wasn't restored", DummyStatefulP.wasRestored), 10);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 1);
    }
}
