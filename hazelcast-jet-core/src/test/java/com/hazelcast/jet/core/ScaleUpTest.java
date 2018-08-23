/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class ScaleUpTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private JetInstance[] instances;
    private DAG dag;
    private JetConfig config;

    private void setup(long scaleUpDelay) {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckForeverSourceP::new, NODE_COUNT)));
        config = new JetConfig();
        config.getInstanceConfig().setScaleUpDelayMillis(scaleUpDelay);
        instances = createJetMembers(config, NODE_COUNT);
    }

    @Test
    public void when_memberAdded_then_jobScaledUp() {
        setup(1000);
        instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createJetMember(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT * 2 + 1, MockPS.initCount.get()));
    }

    @Test
    public void when_memberAddedAndAutoScalingDisabled_then_jobNotRestarted() {
        setup(1000);
        instances[0].newJob(dag, new JobConfig().setAutoScaling(false));
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createJetMember(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));
    }

    @Test
    public void when_liteMemberAdded_then_jobNotRestarted() {
        setup(1000);
        instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().setLiteMember(true);
        createJetMember(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));
    }

    @Test
    public void when_memberAddedAndAnotherAddedBeforeDelay_then_jobRestartedOnce() {
        setup(10_000);
        instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createJetMember(config);
        sleepSeconds(1);
        createJetMember(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT * 2 + 2, MockPS.initCount.get()));
    }

    @Test
    public void when_memberAddedAndRemovedBeforeDelay_then_jobNotRestarted() {
        setup(12_000);
        instances[0].newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        JetInstance addedMember = createJetMember(config);
        sleepSeconds(1);
        addedMember.shutdown();
        assertTrueAllTheTime(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 15);
    }
}
