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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.DummyStatefulP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GracefulShutdown_LiteMasterTest extends JetTestSupport {

    private HazelcastInstance instance;
    private HazelcastInstance liteMaster;

    @Before
    public void setup() {
        TestProcessors.reset(0);
        Config liteMemberConfig = smallInstanceConfig();
        liteMemberConfig.setLiteMember(true);
        liteMaster = createHazelcastInstance(liteMemberConfig);
        instance = createHazelcastInstance();
    }

    @Test
    public void test() {
        DummyStatefulP.parallelism = 2;
        DAG dag = new DAG();
        dag.newVertex("v", (SupplierEx<Processor>) DummyStatefulP::new)
           .localParallelism(DummyStatefulP.parallelism);
        Job job = instance.getJet().newJob(dag, new JobConfig()
                .setSnapshotIntervalMillis(DAYS.toMillis(1))
                .setProcessingGuarantee(EXACTLY_ONCE));
        var timeout = Duration.ofSeconds(10);
        assertThat(job).eventuallyHasStatus(RUNNING, timeout);
        DummyStatefulP.wasRestored = false;
        liteMaster.shutdown();
        assertThat(job).eventuallyHasStatus(RUNNING, timeout);
        assertTrueEventually(() -> assertTrue("snapshot wasn't restored", DummyStatefulP.wasRestored), 10);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 1);
    }
}
