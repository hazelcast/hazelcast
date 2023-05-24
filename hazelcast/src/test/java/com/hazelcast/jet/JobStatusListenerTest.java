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
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.JobEventService;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
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
    private static final String ADVANCE_MAP_NAME = "advanceCount";
    private static final String RUN_MAP_NAME = "runCount";

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
        testListener(streamSource(),
                (job, listener) -> {
                    listener.advance(3);
                    assertEqualsEventually(listener::runCount, 1);
                    job.suspend();
                    assertJobStatusEventually(job, SUSPENDED);
                    job.resume();
                    assertEqualsEventually(listener::runCount, 2);
                    job.restart();
                    assertEqualsEventually(listener::runCount, 3);
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
        testListener(batchSource(r -> {
                    throw new JetException("mock error");
                }),
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
        testListener(new JobConfig().setAutoScaling(true),
                streamSource(run -> {
                    if (run == 1) {
                        throw new RestartableException();
                    }
                }),
                (job, listener) -> {
                    listener.advance(2);
                    assertEqualsEventually(listener::runCount, 2);
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
                batchSource(r -> {
                    throw new JetException("mock error");
                }),
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
        testLightListener(batchSource(r -> {
                    throw new JetException("mock error");
                }),
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
        advance(job.getId(), 1);
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

    protected void testListener(JobConfig config, DAG dag, BiConsumer<Job, JobStatusLogger> test) {
        testListener(dag, (jet, p) -> jet.newJob(p, config), test);
    }

    protected void testListener(DAG dag, BiConsumer<Job, JobStatusLogger> test) {
        testListener(new JobConfig(), dag, test);
    }

    protected void testListener(DAG dag, Consumer<Job> test, String... log) {
        testListener(dag, (job, listener) -> {
            test.accept(job);
            assertTailEqualsEventually(listener.log, log);
        });
    }

    protected void testLightListener(DAG dag, BiConsumer<Job, JobStatusLogger> test) {
        testListener(dag, (jet, p) -> {
            Job job = jet.newLightJob(p);
            assertJobVisible(instance.get(), job, job.getIdString());
            return job;
        }, test);
    }

    protected void testLightListener(DAG dag, Consumer<Job> test, String log) {
        testLightListener(dag, (job, listener) -> {
            test.accept(job);
            assertEqualsEventually(listener.log, log);
        });
    }

    private void testListener(DAG dag, BiFunction<JetService, DAG, Job> submit,
                              BiConsumer<Job, JobStatusLogger> test) {
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

    protected class JobStatusLogger implements JobStatusListener {
        public final List<String> log = Collections.synchronizedList(new ArrayList<>());
        public final UUID registrationId;
        final Job job;
        Thread originalThread;

        public JobStatusLogger(Job job) {
            this.job = job;
            registrationId = job.addStatusListener(this);
            advance(1);
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

        /**
         * {@link JetTestSupport#assertJobStatusEventually assertJobStatusEventually(job, RUNNING)}
         * is not reliable to wait for RESTART since the initial status is RUNNING. For such cases,
         * {@link HazelcastTestSupport#assertEqualsEventually assertEqualsEventually(listener::runCount,
         * lastRunCount + 1)} can be used.
         */
        public int runCount() {
            return (int) instance.get().getMap(RUN_MAP_NAME).getOrDefault(job.getId(), 0);
        }

        public void advance(int count) {
            JobStatusListenerTest.this.advance(job.getId(), count);
        }

        public void deregister() {
            job.removeStatusListener(registrationId);
        }
    }

    protected void advance(long jobId, int count) {
        instance.get().getMap(ADVANCE_MAP_NAME).put(jobId, count);
    }

    protected static DAG batchSource() {
        return batchSource(r -> { });
    }

    protected static DAG batchSource(ConsumerEx<Integer> action) {
        return new DAG().vertex(new Vertex("batchSource",
                forceTotalParallelismOne(new TestProcessorSupplier(action, false))));
    }

    protected static DAG streamSource() {
        return streamSource(r -> { });
    }

    protected static DAG streamSource(ConsumerEx<Integer> action) {
        return new DAG().vertex(new Vertex("streamSource",
                forceTotalParallelismOne(new TestProcessorSupplier(action, true))));
    }

    static class TestProcessorSupplier implements ProcessorSupplier {
        final ConsumerEx<Integer> action;
        final boolean streaming;
        Runnable incrementRunCount;
        int currentRun;
        boolean firstRun = true;

        TestProcessorSupplier(ConsumerEx<Integer> action, boolean streaming) {
            this.action = action;
            this.streaming = streaming;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            IMap<Long, Integer> runCount = context.hazelcastInstance().getMap(RUN_MAP_NAME);
            IMap<Long, Integer> allowedRunCount = context.hazelcastInstance().getMap(ADVANCE_MAP_NAME);
            long jobId = context.jobId();
            currentRun = runCount.getOrDefault(jobId, 0) + 1;
            incrementRunCount = () -> runCount.compute(jobId, (id, count) -> count == null ? 1 : count + 1);
            //noinspection StatementWithEmptyBody
            while (allowedRunCount.getOrDefault(jobId, 0) < currentRun) {
                // Busy-wait until it is allowed to advance
            }
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Collections.singletonList(new AbstractProcessor() {
                @Override
                public boolean complete() {
                    if (firstRun) {
                        incrementRunCount.run();
                        firstRun = false;
                    }
                    action.accept(currentRun);
                    //noinspection ResultOfMethodCallIgnored
                    tryEmit(currentRun);
                    return !streaming;
                }
            });
        }
    }
}
