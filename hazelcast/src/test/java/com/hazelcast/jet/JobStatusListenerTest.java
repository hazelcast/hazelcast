/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.JobEventService;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.spi.impl.eventservice.impl.EventServiceTest.getEventService;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class JobStatusListenerTest extends SimpleTestInClusterSupport {
    private static final Function<String, String> SIMPLIFY = log -> log.replaceAll("(?<=\\().*: ", "");

    @SuppressWarnings("Convert2MethodRef")
    @Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return asList(
                mode("onMaster", () -> instances()[0]),
                mode("onNonMaster", () -> instances()[1]),
                mode("onClient", () -> client()));
    }

    static Object[] mode(String name, Supplier<HazelcastInstance> supplier) {
        return new Object[] {name, supplier};
    }

    @Parameter(0)
    public String mode;

    @Parameter(1)
    public Supplier<HazelcastInstance> instance;

    protected String jobIdString;
    protected UUID registrationId;

    @BeforeClass
    public static void setup() {
        initializeWithClient(2, null, null);
    }

    @Before
    public void reset() {
        TestProcessors.reset(1);
    }

    @Test
    public void testListener_waitForCompletion() {
        testListener(batchSource(),
                Job::join,
                "Jet: NOT_RUNNING -> STARTING",
                "Jet: STARTING -> RUNNING",
                "Jet: RUNNING -> COMPLETED");
    }

    @Test
    public void testListener_suspend_resume_restart_cancelJob() {
        NoOutputSourceP.executionStarted = new CountDownLatch(3);
        testListener(streamSource(),
                (job, listener) -> {
                    assertJobStatusEventually(job, RUNNING);
                    job.suspend();
                    assertJobStatusEventually(job, SUSPENDED);
                    job.resume();
                    MockPS.unblock();
                    assertJobStatusEventually(job, RUNNING);
                    job.restart();
                    MockPS.unblock();
                    NoOutputSourceP.executionStarted.await();
                    cancelAndJoin(job);
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> SUSPENDED (Suspend)",
                            "User: SUSPENDED -> NOT_RUNNING (Resume)",
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> NOT_RUNNING (Restart)",
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> FAILED (Cancel)");
                });
    }

    @Test
    public void testListener_jobFails() {
        testListener(batchSource(new JetException("mock error")),
                (job, listener) -> {
                    Throwable failure = null;
                    try {
                        job.join();
                    } catch (CompletionException e) {
                        failure = e.getCause();
                    }
                    assertNotNull(failure);
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "Jet: RUNNING -> FAILED (" + failure + ")");
                });
    }

    @Test
    public void testListener_restartOnException() {
        NoOutputSourceP.executionStarted = new CountDownLatch(2);
        testListener(new JobConfig().setAutoScaling(true),
                streamSource(new RestartableException()),
                (job, listener) -> {
                    MockPS.unblock();
                    MockPS.unblock();
                    NoOutputSourceP.executionStarted.await();
                    cancelAndJoin(job);
                    assertTailEqualsEventually(listener.log.stream().map(SIMPLIFY).collect(toList()),
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "Jet: RUNNING -> NOT_RUNNING (com.hazelcast.jet.RestartableException)",
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> FAILED (Cancel)");
                });
    }

    @Test
    public void testListener_suspendOnFailure() {
        testListener(new JobConfig().setSuspendOnFailure(true),
                batchSource(new JetException("mock error")),
                (job, listener) -> {
                    assertJobSuspendedEventually(job);
                    String failure = job.getSuspensionCause().errorCause().split("\n", 3)[1];
                    cancelAndJoin(job);
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "Jet: RUNNING -> SUSPENDED (" + failure + ")",
                            "User: SUSPENDED -> FAILED (Cancel)");
                });
    }

    @Test
    public void testListenerDeregistration() {
        testListener(streamSource(),
                (job, listener) -> {
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING");
                    listener.deregister();
                    cancelAndJoin(job);
                    assertHasNoListenerEventually(job.getIdString(), listener.registrationId);
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING");
                });
    }

    @Test
    public void testListenerLateRegistration() {
        testListenerLateRegistration(JetService::newJob);
    }

    @Test
    public void testLightListener_waitForCompletion() {
        testLightListener(batchSource(),
                Job::join,
                "Jet: RUNNING -> COMPLETED");
    }

    @Test
    public void testLightListener_cancelJob() {
        testLightListener(streamSource(),
                Job::cancel,
                "User: RUNNING -> FAILED (Cancel)");
    }

    @Test
    public void testLightListener_jobFails() {
        testLightListener(batchSource(new JetException("mock error")),
                (job, listener) -> {
                    Throwable failure = null;
                    try {
                        job.getFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        failure = e.getCause();
                    }
                    assertNotNull(failure);
                    assertEqualsEventually(listener.log,
                            "Jet: RUNNING -> FAILED (" + failure + ")");
                });
    }

    @Test
    public void testLightListenerDeregistration() {
        testLightListener(streamSource(),
                (job, listener) -> {
                    listener.deregister();
                    job.cancel();
                    assertHasNoListenerEventually(job.getIdString(), listener.registrationId);
                    assertTrue(listener.log.isEmpty());
                });
    }

    @Test
    public void testLightListenerLateRegistration() {
        testListenerLateRegistration(JetService::newLightJob);
    }

    private void testListenerLateRegistration(BiFunction<JetService, DAG, Job> submit) {
        Job job = submit.apply(instance.get().getJet(), batchSource());
        MockPS.unblock();
        jobIdString = job.getIdString();
        job.join();
        assertThatThrownBy(() -> job.addStatusListener(e -> { }))
                .hasMessage("Cannot add status listener to a COMPLETED job");
    }

    @After
    public void testListenerDeregistration_onCompletion() {
        assertHasNoListenerEventually(jobIdString, registrationId);
    }

    protected static void assertHasNoListenerEventually(String jobIdString, UUID registrationId) {
        assertTrueEventually(() -> {
            assertTrue(Arrays.stream(instances()).allMatch(hz ->
                    getEventService(hz).getRegistrations(JobEventService.SERVICE_NAME, jobIdString).isEmpty()));
            assertTrue(registrationId == null
                    || !((ClientListenerServiceImpl) getHazelcastClientInstanceImpl(client()).getListenerService())
                        .getRegistrations().containsKey(registrationId));
        });
    }

    protected void testListener(JobConfig config, DAG dag, BiConsumerEx<Job, JobStatusLogger> test) {
        testListener(dag, (jet, p) -> jet.newJob(p, config), test);
    }

    protected void testListener(DAG dag, BiConsumerEx<Job, JobStatusLogger> test) {
        testListener(new JobConfig(), dag, test);
    }

    protected void testListener(DAG dag, ConsumerEx<Job> test, String... log) {
        testListener(dag, (job, listener) -> {
            test.accept(job);
            assertTailEqualsEventually(listener.log, log);
        });
    }

    protected void testLightListener(DAG dag, BiConsumerEx<Job, JobStatusLogger> test) {
        testListener(dag, (jet, p) -> {
            Job job = jet.newLightJob(p);
            assertJobVisible(instance.get(), job, job.getIdString());
            return job;
        }, test);
    }

    protected void testLightListener(DAG dag, ConsumerEx<Job> test, String log) {
        testLightListener(dag, (job, listener) -> {
            test.accept(job);
            assertEqualsEventually(listener.log, log);
        });
    }

    private void testListener(DAG dag, BiFunction<JetService, DAG, Job> submit,
                              BiConsumerEx<Job, JobStatusLogger> test) {
        Job job = submit.apply(instance.get().getJet(), dag);
        JobStatusLogger listener = new JobStatusLogger(job);
        jobIdString = job.getIdString();
        registrationId = listener.registrationId;
        test.accept(job, listener);
    }

    @SafeVarargs
    protected static <T> void assertEqualsEventually(List<T> actual, T... expected) {
        assertTrueEventually(() -> {
            List<T> actualCopy = new ArrayList<>(actual);
            assertEquals("length", actualCopy.size(), expected.length);
            assertEquals(actualCopy, asList(expected));
        });
    }

    @SafeVarargs
    protected static <T> void assertTailEqualsEventually(List<T> actual, T... expected) {
        assertTrueEventually(() -> {
            List<T> actualCopy = new ArrayList<>(actual);
            assertBetween("length", actualCopy.size(), expected.length - 1, expected.length);
            List<T> tail = asList(expected).subList(expected.length - actualCopy.size(), expected.length);
            assertEquals(tail, actualCopy);
        });
    }

    protected static DAG batchSource() {
        return batchSource(null);
    }

    protected static DAG batchSource(RuntimeException failure) {
        NoOutputSourceP.proceedLatch.countDown();
        NoOutputSourceP.failure.set(failure);
        return new DAG().vertex(new Vertex("batchSource",
                forceTotalParallelismOne(new MockPS(NoOutputSourceP::new, 1).initBlocks())));
    }

    protected static DAG streamSource() {
        return streamSource(null);
    }

    protected static DAG streamSource(RuntimeException failure) {
        NoOutputSourceP.failure.set(failure);
        return new DAG().vertex(new Vertex("streamSource",
                forceTotalParallelismOne(new MockPS(NoOutputSourceP::new, 1).initBlocks())));
    }

    protected static class JobStatusLogger implements JobStatusListener {
        public final List<String> log = Collections.synchronizedList(new ArrayList<>());
        public final UUID registrationId;
        final Job job;
        Thread originalThread;

        public JobStatusLogger(Job job) {
            this.job = job;
            registrationId = job.addStatusListener(this);
            MockPS.unblock();
        }

        @Override
        public void jobStatusChanged(JobStatusEvent e) {
            String threadInfo = "";
            if (originalThread == null) {
                originalThread = Thread.currentThread();
            } else if (originalThread != Thread.currentThread()) {
                // All event handler invocations should be on the same thread to guarantee order.
                // This is especially important on client side.
                threadInfo = String.format(" (invoked from different thread: expected %s but invoked on %s)",
                        originalThread.getName(), Thread.currentThread().getName());
            }

            log.add(String.format("%s: %s -> %s%s%s",
                    e.isUserRequested() ? "User" : "Jet", e.getPreviousStatus(), e.getNewStatus(),
                    e.getDescription() == null ? "" : " (" + e.getDescription() + ")",
                    threadInfo));
        }

        public void deregister() {
            job.removeStatusListener(registrationId);
        }
    }
}
