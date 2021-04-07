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

package com.hazelcast.jet.core;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Job} with a separate cluster for each test.
 */
public class Job_SeparateClusterTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetInstance instance1;
    private JetInstance instance2;

    @Before
    public void setup() {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        Config config = new Config();
        config.getJetConfig().getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        config.getJetConfig().getInstanceConfig().setScaleUpDelayMillis(10);
        instance1 = createJetMember(config);
        instance2 = createJetMember(config);
    }

    @Test
    public void when_suspendedJobScannedOnNewMaster_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);
        // gracefully shutdown the master
        instance1.shutdown();

        // Then
        expectedException.expect(JobAlreadyExistsException.class);
        instance2.newJob(dag, config);
    }

    @Test
    public void when_joinFromClientTimesOut_then_futureShouldNotBeCompletedEarly() throws InterruptedException {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        int timeoutSecs = 1;
        ClientConfig config = new ClientConfig()
                .setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), Integer.toString(timeoutSecs));
        JetInstance client = createJetClient(config);

        // join request is sent along with job submission
        Job job = client.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // wait for join invocation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeoutSecs));

        // When
        NoOutputSourceP.initCount.set(0);
        instance1.getHazelcastInstance().getLifecycleService().terminate();
        // wait for job to be restarted on remaining node
        assertTrueEventually(() -> assertEquals(LOCAL_PARALLELISM, NoOutputSourceP.initCount.get()));

        RuntimeException ex = new RuntimeException("Faulty job");
        NoOutputSourceP.failure.set(ex);

        // Then
        expectedException.expectMessage(Matchers.containsString(ex.getMessage()));
        job.join();
    }

    @Test
    public void when_joinFromClientSentToNonMaster_then_futureShouldNotBeCompletedEarly() throws InterruptedException {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        int timeoutSecs = 1;
        Address address = getAddress(instance2);
        ClientConfig config = new ClientConfig()
                .setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), Integer.toString(timeoutSecs))
                .setNetworkConfig(new ClientNetworkConfig()
                        .setSmartRouting(false)
                        .addAddress(address.getHost() + ":" + address.getPort())
                );
        JetInstance client = createJetClient(config);

        // join request is sent along with job submission
        Job job = client.newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // wait for join invocation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeoutSecs));

        // When
        NoOutputSourceP.initCount.set(0);
        instance1.getHazelcastInstance().getLifecycleService().terminate();
        // wait for job to be restarted on remaining node
        assertTrueEventually(() -> assertEquals(LOCAL_PARALLELISM, NoOutputSourceP.initCount.get()));

        RuntimeException ex = new RuntimeException("Faulty job");
        NoOutputSourceP.failure.set(ex);

        // Then
        expectedException.expectMessage(Matchers.containsString(ex.getMessage()));
        job.join();
    }

    @Test
    public void stressTest_getJobStatus_client() throws Exception {
        JetInstance client = createJetClient();
        stressTest_getJobStatus(() -> client);
    }

    @Test
    @Ignore("fails currently")
    public void stressTest_getJobStatus_member() throws Exception {
        stressTest_getJobStatus(() -> instance1);
    }

    private void stressTest_getJobStatus(Supplier<JetInstance> submitterSupplier) throws Exception {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        AtomicReference<Job> job = new AtomicReference<>(submitterSupplier.get().newJob(dag));

        AtomicBoolean done = new AtomicBoolean();
        List<Runnable> actions = asList(
                () -> job.get().getStatus(),
                () -> job.get().getMetrics(),
                () -> job.get().getConfig()
        );
        List<Future> checkerFutures = new ArrayList<>();
        for (Runnable action : actions) {
            checkerFutures.add(spawn(() -> {
                while (!done.get()) {
                    action.run();
                }
            }));
        }

        for (int i = 0; i < 5; i++) {
            instance1.shutdown();
            instance1 = createJetMember();
            job.set(submitterSupplier.get().getJob(job.get().getId()));
            assertJobStatusEventually(job.get(), RUNNING);

            instance2.shutdown();
            instance2 = createJetMember();
            job.set(submitterSupplier.get().getJob(job.get().getId()));
            assertJobStatusEventually(job.get(), RUNNING);

            sleepSeconds(1);
            if (checkerFutures.stream().anyMatch(Future::isDone)) {
                break;
            }
        }

        done.set(true);
        for (Future future : checkerFutures) {
            future.get();
        }
    }
}
