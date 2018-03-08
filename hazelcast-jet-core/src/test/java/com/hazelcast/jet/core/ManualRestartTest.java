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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.core.TestProcessors.StuckProcessor;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastSerialClassRunner.class)
public class ManualRestartTest extends JetTestSupport {
    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private DAG dag;
    private JetInstance[] instances;

    @Before
    public void setup() {
        MockPS.completeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);

        dag = new DAG().vertex(new Vertex("test", new MockPS(StuckForeverSourceP::new, NODE_COUNT)));
        instances = createJetMembers(new JetConfig(), NODE_COUNT);
    }

    @Test
    public void when_jobIsRunning_then_itRestarts() {
        testJobRestartWhenJobIsRunning(true);
    }

    @Test
    public void when_autoRestartOnMemberFailureDisabled_then_jobRestarts() {
        testJobRestartWhenJobIsRunning(false);
    }

    private void testJobRestartWhenJobIsRunning(boolean autoRestartOnMemberFailureEnabled) {
        // Given that the job is running
        JetInstance client = createJetClient();
        Job job = client.newJob(dag, new JobConfig().setAutoRestartOnMemberFailure(autoRestartOnMemberFailureEnabled));

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT, MockPS.initCount.get());
        });

        // When the job is restarted after new members join to the cluster
        int newMemberCount = 2;
        for (int i = 0; i < newMemberCount; i++) {
            createJetMember();
        }

        job.restart();

        // Then, the job restarts
        int initCount = NODE_COUNT * 2 + newMemberCount;
        assertTrueEventually(() -> {
            assertEquals(initCount, MockPS.initCount.get());
        });
    }

    @Test
    public void when_jobIsNotBeingExecuted_then_itCannotBeRestarted() {
        // Given that the job execution has not started
        rejectOperationsBetween(instances[0].getHazelcastInstance(), instances[1].getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(JetInitDataSerializerHook.INIT_EXECUTION_OP));

        JetInstance client = createJetClient();
        Job job = client.newJob(dag);

        assertTrueEventually(() -> assertTrue(job.getStatus() == JobStatus.STARTING));

        // Then, the job cannot restart
        assertFalse(job.restart());

        resetPacketFiltersFrom(instances[0].getHazelcastInstance());
    }

    @Test(expected = IllegalStateException.class)
    public void when_jobIsCompleted_then_isCannotBeRestarted() {
        // Given that the job is completed
        JetInstance client = createJetClient();
        Job job = client.newJob(dag);

        job.cancel();

        try {
            job.join();
            fail();
        } catch (CancellationException ignored) {
        }

        // Then, the job cannot restart
        job.restart();
    }
}
