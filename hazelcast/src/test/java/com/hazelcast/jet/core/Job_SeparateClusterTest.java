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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Job} with a separate cluster for each test.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class Job_SeparateClusterTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private Config config;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        config = smallInstanceConfig();
        config.getJetConfig().getInstanceConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        config.getJetConfig().getInstanceConfig().setScaleUpDelayMillis(10);
        instance1 = createHazelcastInstance(config);
        instance2 = createHazelcastInstance(config);
    }

    @Test
    public void when_suspendedJobScannedOnNewMaster_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.getJet().newJob(dag, config);
        assertJobStatusEventually(job1, RUNNING);
        job1.suspend();
        assertJobStatusEventually(job1, SUSPENDED);
        // gracefully shutdown the master
        instance1.shutdown();

        // Then
        assertThatThrownBy(() -> instance2.getJet().newJob(dag, config))
                .isInstanceOf(JobAlreadyExistsException. class);
    }

    @Test
    public void when_joinFromClientTimesOut_then_futureShouldNotBeCompletedEarly() throws InterruptedException {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        int timeoutSecs = 1;
        ClientConfig config = new ClientConfig()
                .setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), Integer.toString(timeoutSecs));
        HazelcastInstance client = createHazelcastClient(config);

        // join request is sent along with job submission
        Job job = client.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // wait for join invocation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeoutSecs));

        // When
        NoOutputSourceP.initCount.set(0);
        instance1.getLifecycleService().terminate();
        // wait for job to be restarted on remaining node
        assertTrueEventually(() -> assertEquals(LOCAL_PARALLELISM, NoOutputSourceP.initCount.get()));

        RuntimeException ex = new RuntimeException("Faulty job");
        NoOutputSourceP.failure.set(ex);

        // Then
        assertThatThrownBy(job::join)
                .hasMessageContaining(ex.getMessage());
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
        HazelcastInstance client = createHazelcastClient(config);

        // join request is sent along with job submission
        Job job = client.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // wait for join invocation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeoutSecs));

        // When
        NoOutputSourceP.initCount.set(0);
        instance1.getLifecycleService().terminate();
        // wait for job to be restarted on remaining node
        assertTrueEventually(() -> assertEquals(LOCAL_PARALLELISM, NoOutputSourceP.initCount.get()));

        RuntimeException ex = new RuntimeException("Faulty job");
        NoOutputSourceP.failure.set(ex);

        // Then
        assertThatThrownBy(job::join)
                .hasMessageContaining(ex.getMessage());
    }

    @Test
    public void stressTest_getJobStatus_client() throws Exception {
        HazelcastInstance client = createHazelcastClient();
        stressTest_getJobStatus(() -> client);
    }

    @Test
    @Ignore("fails currently")
    public void stressTest_getJobStatus_member() throws Exception {
        stressTest_getJobStatus(() -> instance1);
    }

    private void stressTest_getJobStatus(Supplier<HazelcastInstance> submitterSupplier) throws Exception {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        AtomicReference<Job> job = new AtomicReference<>(submitterSupplier.get().getJet().newJob(dag));

        AtomicBoolean done = new AtomicBoolean();
        List<Runnable> actions = asList(
                () -> job.get().getStatus(),
                () -> job.get().getMetrics(),
                () -> job.get().getConfig()
        );
        List<Future<?>> checkerFutures = new ArrayList<>();
        for (Runnable action : actions) {
            checkerFutures.add(spawn(() -> {
                while (!done.get()) {
                    action.run();
                }
            }));
        }

        for (int i = 0; i < 5; i++) {
            instance1.shutdown();
            instance1 = createHazelcastInstance();
            job.set(submitterSupplier.get().getJet().getJob(job.get().getId()));
            assertJobStatusEventually(job.get(), RUNNING);

            instance2.shutdown();
            instance2 = createHazelcastInstance();
            job.set(submitterSupplier.get().getJet().getJob(job.get().getId()));
            assertJobStatusEventually(job.get(), RUNNING);

            sleepSeconds(1);
            if (checkerFutures.stream().anyMatch(Future::isDone)) {
                break;
            }
        }

        done.set(true);
        for (Future<?> future : checkerFutures) {
            future.get();
        }
    }

    @Test
    public void stressTest_clientSubmission() throws Throwable {
        /*
        This test submits jobs from the master while concurrently starting and removing members.
        Jobs have the prevent-shutdown flag set.
         */
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean terminated = new AtomicBoolean();
        AtomicInteger jobsExecuted = new AtomicInteger();
        AtomicInteger membersAddedRemoved = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();

        // add threads submitting jobs
        DAG dag = new DAG();
        dag.edge(between(dag.newVertex("v1", MockP::new), dag.newVertex("v2", MockP::new)).distributed());
        for (int i = 0; i < 2; i++) {
            threads.add(new Thread(() -> {
                try {
                    while (!terminated.get()) {
                        logger.info("submitting and joining job");
                        instance1.getJet().newLightJob(dag, new JobConfig().setPreventShutdown(true))
                                .join();
                        logger.info("job completed");
                        jobsExecuted.incrementAndGet();
                    }
                } catch (Throwable e) {
                    logger.info("", e);
                    error.compareAndSet(null, e);
                }
            }, "jobTread-" + i));
        }

        for (int i = 0; i < 2; i++) {
            threads.add(new Thread(() -> {
                try {
                    while (!terminated.get()) {
                        logger.info("creating instance");
                        HazelcastInstance inst = createHazelcastInstance(config);
                        logger.info("instance created, shutting it down");
                        inst.shutdown();
                        logger.info("instance shut down");
                        membersAddedRemoved.incrementAndGet();
                    }
                } catch (Throwable e) {
                    logger.info("", e);
                    error.compareAndSet(null, e);
                }
            }, "memberAddRemoveThread-" + i));
        }

        for (Thread t : threads) {
            t.start();
        }

        for (int i = 0; i < 60; i++) {
            checkError(error);
            sleepSeconds(1);
        }
        terminated.set(true);

        for (Thread t : threads) {
            t.join();
        }

        checkError(error);
        logger.info(jobsExecuted.get() + " jobs executed, " + membersAddedRemoved.get() + " members added/removed");
        assertThat(jobsExecuted.get()).as("jobs executed").isGreaterThan(10);
        assertThat(membersAddedRemoved.get()).as("members added/removed").isGreaterThan(10);
    }

    private void checkError(AtomicReference<Throwable> error) {
        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
    }
}
