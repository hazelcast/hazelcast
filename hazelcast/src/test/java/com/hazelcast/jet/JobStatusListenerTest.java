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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JobEventService;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.spi.impl.eventservice.impl.EventServiceTest.getEventService;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class JobStatusListenerTest extends SimpleTestInClusterSupport {
    private static final Function<String, String> SIMPLIFY = log -> log.replaceAll("(?<=\\().*: ", "");
    private static final String MAP_NAME = "runCount";

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

    /**
     * Used to generate unique job ids to be used inside jobs, e.g. sinks,
     * since it is not easy to access the context.
     */
    private static int nextJobId;

    @Parameter(0)
    public String mode;

    @Parameter(1)
    public Supplier<HazelcastInstance> instance;

    private String jobIdString;
    private UUID registrationId;

    @BeforeClass
    public static void setup() {
        initializeWithClient(2, null, null);
    }

    @Test
    public void testListener_waitForCompletion() {
        testListener(finiteStream(1, SECONDS, 2),
                Job::join,
                "Jet: NOT_RUNNING -> STARTING",
                "Jet: STARTING -> RUNNING",
                "Jet: RUNNING -> COMPLETED");
    }

    @Test
    public void testListener_suspend_resume_restart_cancelJob() {
        testListener(TestSources.itemStream(1, (t, s) -> s),
                (job, listener) -> {
                    assertJobStatusEventually(job, RUNNING);
                    job.suspend();
                    assertJobStatusEventually(job, SUSPENDED);
                    job.resume();
                    assertJobStatusEventually(job, RUNNING);
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
        testListener(TestSources.itemStream(1, (t, s) -> 1 / (2 - s)),
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
                TestSources.itemStream(1,
                        (t, s) -> {
                            if (s == 2) {
                                throw new RestartableException();
                            }
                            return s;
                        }),
                (job, listener) -> {
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
                TestSources.itemStream(1, (t, s) -> 1 / (2 - s)),
                (job, listener) -> {
                    assertJobStatusEventually(job, SUSPENDED);
                    String[] failure = new String[1];
                    assertTrueEventually(() ->
                            failure[0] = job.getSuspensionCause().errorCause().split("\n", 3)[1]);
                    cancelAndJoin(job);
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "Jet: RUNNING -> SUSPENDED (" + failure[0] + ")",
                            "User: SUSPENDED -> FAILED (Cancel)");
                });
    }

    @Test
    public void testListenerDeregistration() {
        testListener(TestSources.itemStream(1, (t, s) -> s),
                (job, listener) -> {
                    assertJobStatusEventually(job, RUNNING);
                    listener.deregister();
                    cancelAndJoin(job);
                    assertHasNoListenerEventually(job.getIdString(), listener.registrationId);
                    assertTailEqualsEventually(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING");
                });
    }

    @Test
    public void testLightListener_waitForCompletion() {
        testLightListener(finiteStream(1, SECONDS, 2),
                Job::join,
                "Jet: RUNNING -> COMPLETED");
    }

    @Test
    public void testLightListener_cancelJob() {
        testLightListener(TestSources.itemStream(1, (t, s) -> s),
                Job::cancel,
                "User: RUNNING -> FAILED (Cancel)");
    }

    @Test
    public void testLightListener_jobFails() {
        testLightListener(TestSources.itemStream(1, (t, s) -> 1 / (2 - s)),
                (job, listener) -> {
                    Throwable failure = null;
                    try {
                        job.getFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        failure = e.getCause();
                    }
                    assertNotNull(failure);
                    assertIterableEqualsEventually(listener.log,
                            "Jet: RUNNING -> FAILED (" + failure + ")");
                });
    }

    @Test
    public void testLightListenerDeregistration() {
        testLightListener(TestSources.itemStream(1, (t, s) -> s),
                (job, listener) -> {
                    listener.deregister();
                    job.cancel();
                    assertHasNoListenerEventually(job.getIdString(), listener.registrationId);
                    assertTrue(listener.log.isEmpty());
                });
    }

    @After
    public void testListenerDeregistration_onCompletion() {
        assertHasNoListenerEventually(jobIdString, registrationId);
    }

    protected static void assertHasNoListenerEventually(String jobIdString, UUID registrationId) {
        assertTrueEventually(() -> {
            assertTrue(Arrays.stream(instances()).allMatch(hz ->
                    getEventService(hz).getRegistrations(JobEventService.SERVICE_NAME, jobIdString).isEmpty()));
            assertFalse(((ClientListenerServiceImpl) getHazelcastClientInstanceImpl(client()).getListenerService())
                    .getRegistrations().containsKey(registrationId));
        });
    }

    /**
     * @param source A {@link BatchSource BatchSource&lt;Long&gt;} or {@link StreamSource
     *        StreamSource&lt;Long&gt;} where the first element is {@code 0L}, which is used
     *        to increment the {@linkplain JobStatusLogger#runCount run count} of the job.
     */
    protected void testListener(JobConfig config, Object source, BiConsumer<Job, JobStatusLogger> test) {
        Pipeline p = Pipeline.create();
        int jobId = nextJobId++;
        (source instanceof BatchSource
                    ? p.readFrom((BatchSource<Long>) source)
                    : p.readFrom((StreamSource<Long>) source).withoutTimestamps())
                .writeTo(Sinks.<Long, Integer, Integer>mapWithUpdating(
                        MAP_NAME, s -> jobId, (i, s) -> s == 0 ? (i == null ? 0 : i) + 1 : i));

        Job job = instance.get().getJet().newJob(p, config);
        JobStatusLogger listener = new JobStatusLogger(job, jobId);
        jobIdString = job.getIdString();
        registrationId = listener.registrationId;
        test.accept(job, listener);
    }

    protected void testListener(Object source, BiConsumer<Job, JobStatusLogger> test) {
        testListener(new JobConfig(), source, test);
    }

    protected void testListener(Object source, Consumer<Job> test, String... log) {
        testListener(source, (job, listener) -> {
            test.accept(job);
            assertTailEqualsEventually(listener.log, log);
        });
    }

    protected void testLightListener(Object source, BiConsumer<Job, JobStatusLogger> test) {
        Pipeline p = Pipeline.create();
        (source instanceof BatchSource
                    ? p.readFrom((BatchSource<?>) source)
                    : p.readFrom((StreamSource<?>) source).withoutTimestamps())
                .writeTo(Sinks.noop());

        Job job = instance.get().getJet().newLightJob(p);
        JobStatusLogger listener = new JobStatusLogger(job, -1);
        jobIdString = job.getIdString();
        registrationId = listener.registrationId;
        test.accept(job, listener);
    }

    protected void testLightListener(Object source, Consumer<Job> test, String log) {
        testLightListener(source, (job, listener) -> {
            test.accept(job);
            assertIterableEqualsEventually(listener.log, log);
        });
    }

    protected static void assertIterableEqualsEventually(Iterable<?> actual, Object... expected) {
        assertTrueEventually(() -> assertIterableEquals(actual, expected));
    }

    @SafeVarargs
    protected static <T> void assertTailEqualsEventually(List<T> actual, T... expected) {
        assertTrueEventually(() -> {
            List<T> actualCopy = new ArrayList<>(actual);
            assertBetween("length", actualCopy.size(), expected.length - 2, expected.length);
            List<T> tail = asList(expected).subList(expected.length - actualCopy.size(), expected.length);
            assertEquals(tail, actualCopy);
        });
    }

    protected class JobStatusLogger implements JobStatusListener {
        public final List<String> log = Collections.synchronizedList(new ArrayList<>());
        public final UUID registrationId;
        final Job job;
        final int jobId;
        Thread originalThread;

        JobStatusLogger(Job job, int jobId) {
            this.job = job;
            this.jobId = jobId;
            registrationId = job.addStatusListener(this);
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
        int runCount() {
            Integer count = (Integer) instance.get().getMap(MAP_NAME).get(jobId);
            return count == null ? 0 : count;
        }

        void deregister() {
            job.removeStatusListener(registrationId);
        }
    }

    protected static BatchSource<Long> finiteStream(long period, TimeUnit unit, long maxSequence) {
        return SourceBuilder.batch("finiteStream",
                        ctx -> new FiniteStreamSource(period, unit, maxSequence))
                .fillBufferFn(FiniteStreamSource::fillBuffer)
                .build();
    }

    static class FiniteStreamSource {
        final long periodNanos;
        final long maxSequence;
        long emitSchedule = Long.MIN_VALUE;
        long sequence;

        FiniteStreamSource(long period, TimeUnit unit, long maxSequence) {
            periodNanos = unit.toNanos(period);
            this.maxSequence = maxSequence;
        }

        void fillBuffer(SourceBuffer<Long> buf) {
            long nowNs = System.nanoTime();
            if (nowNs >= emitSchedule) {
                buf.add(sequence++);
                emitSchedule += periodNanos;
                if (sequence > maxSequence) {
                    buf.close();
                }
            }
        }
    }
}
