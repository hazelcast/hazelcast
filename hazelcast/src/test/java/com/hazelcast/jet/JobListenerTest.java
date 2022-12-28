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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.AbstractJetInstance;
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestUtil.set;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobListenerTest extends SimpleTestInClusterSupport {
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

    /**
     * Used to test listener deregistration on job completion/failure.
     */
    private static Field jobListeners;

    @Parameter(0)
    public String mode;

    @Parameter(1)
    public Supplier<HazelcastInstance> instance;

    @BeforeClass
    public static void setUp() throws NoSuchFieldException {
        initializeWithClient(2, null, null);
        jobListeners = AbstractJetInstance.class.getDeclaredField("jobListeners");
        jobListeners.setAccessible(true);
    }

    @Test
    public void testListener_waitForCompletion() {
        testListener(finiteStream(1000, 2),
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
                    job.cancel();
                    assertThrows(CancellationException.class, job::join);
                    assertTailEquals(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> SUSPENDED (SUSPEND)",
                            "User: SUSPENDED -> NOT_RUNNING (RESUME)",
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> NOT_RUNNING (RESTART)",
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> FAILED (CANCEL)");
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
                    assertTailEquals(listener.log,
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
                    job.cancel();
                    assertThrows(CancellationException.class, job::join);
                    assertTailEquals(listener.log.stream().map(SIMPLIFY).collect(toList()),
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "Jet: RUNNING -> NOT_RUNNING (com.hazelcast.jet.RestartableException)",
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "User: RUNNING -> FAILED (CANCEL)");
                });
    }

    @Test
    public void testListener_suspendOnFailure() {
        testListener(new JobConfig().setSuspendOnFailure(true),
                TestSources.itemStream(1, (t, s) -> 1 / (2 - s)),
                (job, listener) -> {
                    assertJobStatusEventually(job, SUSPENDED);
                    String failure = job.getSuspensionCause().errorCause().split("\n", 3)[1];
                    job.cancel();
                    assertThrows(CancellationException.class, job::join);
                    assertTailEquals(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING",
                            "Jet: RUNNING -> SUSPENDED (" + failure + ")",
                            "User: SUSPENDED -> FAILED (CANCEL)");
                });
    }

    @Test
    public void testListenerDeregistration() {
        testListener(TestSources.itemStream(1, (t, s) -> s),
                (job, listener) -> {
                    assertJobStatusEventually(job, RUNNING);
                    instance.get().getJet().removeJobStatusListener(listener);
                    job.cancel();
                    assertThrows(CancellationException.class, job::join);
                    assertTailEquals(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING");
                });
    }

    @Test
    public void testLightListener_waitForCompletion() {
        testLightListener(finiteStream(1000, 2),
                job -> {
                    job.join();
                    sleepSeconds(1);
                },
                "Jet: RUNNING -> COMPLETED");
    }

    @Test
    public void testLightListener_cancelJob() {
        testLightListener(TestSources.itemStream(1, (t, s) -> s),
                job -> {
                    job.cancel();
                    sleepSeconds(1);
                },
                "User: RUNNING -> FAILED (CANCEL)");
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
                    sleepSeconds(1);
                    assertIterableEquals(listener.log,
                            "Jet: RUNNING -> FAILED (" + failure + ")");
                });
    }

    @Test
    public void testLightListenerDeregistration() {
        testLightListener(TestSources.itemStream(1, (t, s) -> s),
                (job, listener) -> {
                    instance.get().getJet().removeJobStatusListener(listener);
                    job.cancel();
                    sleepSeconds(1);
                    assertTrue(listener.log.isEmpty());
                });
    }

    @After
    public void testListenerDeregistration_onCompletion() throws IllegalAccessException {
        assertTrue(((Map<?, ?>) jobListeners.get(instance.get().getJet())).isEmpty());
    }

    /**
     * @param source A {@link BatchSource BatchSource&lt;Long&gt;} or {@link StreamSource
     *        StreamSource&lt;Long&gt;} where the first element is {@code 0L}, which is used
     *        to increment the {@linkplain JobStatusLogger#runCount run count} of the job.
     */
    void testListener(JobConfig config, Object source, BiConsumer<Job, JobStatusLogger> test) {
        Pipeline p = Pipeline.create();
        int jobId = nextJobId++;
        (source instanceof BatchSource
                    ? p.readFrom((BatchSource<Long>) source)
                    : p.readFrom((StreamSource<Long>) source).withoutTimestamps())
                .writeTo(Sinks.<Long, Integer, Integer>mapWithUpdating(
                        MAP_NAME, s -> jobId, (i, s) -> s == 0 ? (i == null ? 0 : i) + 1 : i));

        JetService jet = instance.get().getJet();
        JobStatusLogger listener = new JobStatusLogger(jobId);
        Job job = jet.newJob(p, config);
        jet.addJobStatusListener(set(job.getId()), listener);
        test.accept(job, listener);
    }

    void testListener(Object source, BiConsumer<Job, JobStatusLogger> test) {
        testListener(new JobConfig(), source, test);
    }

    void testListener(Object source, Consumer<Job> test, String... log) {
        testListener(source, (job, listener) -> {
            test.accept(job);
            assertTailEquals(listener.log, log);
        });
    }

    void testLightListener(Object source, BiConsumer<Job, JobStatusLogger> test) {
        Pipeline p = Pipeline.create();
        (source instanceof BatchSource
                    ? p.readFrom((BatchSource<?>) source)
                    : p.readFrom((StreamSource<?>) source).withoutTimestamps())
                .writeTo(Sinks.noop());

        JetService jet = instance.get().getJet();
        JobStatusLogger listener = new JobStatusLogger(-1);
        Job job = jet.newLightJob(p);
        sleepSeconds(1);
        jet.addJobStatusListener(set(job.getId()), listener);
        test.accept(job, listener);
    }

    void testLightListener(Object source, Consumer<Job> test, String log) {
        testLightListener(source, (job, listener) -> {
            test.accept(job);
            assertIterableEquals(listener.log, log);
        });
    }

    @SafeVarargs
    static <T> void assertTailEquals(List<T> actual, T... expected) {
        assertBetween("length", actual.size(), expected.length - 2, expected.length);
        List<T> tail = asList(expected).subList(expected.length - actual.size(), expected.length);
        assertEquals(tail, actual);
    }

    class JobStatusLogger implements JobListener {
        final List<String> log = new ArrayList<>();
        final int jobId;

        JobStatusLogger(int jobId) {
            this.jobId = jobId;
        }

        @Override
        public void jobStatusChanged(JobEvent e) {
            log.add(String.format("%s: %s -> %s%s",
                    e.isUserRequested() ? "User" : "Jet", e.getOldStatus(), e.getNewStatus(),
                    e.getDescription() == null ? "" : " (" + e.getDescription() + ")"));
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
    }

    static BatchSource<Long> finiteStream(int delayMillis, long maxSequence) {
        return SourceBuilder.batch("finiteStream",
                        ctx -> new FiniteStreamSource(delayMillis, maxSequence))
                .fillBufferFn(FiniteStreamSource::fillBuffer)
                .build();
    }

    static class FiniteStreamSource {
        final long delayMillis;
        final long maxSequence;
        long emitSchedule;
        long sequence;

        FiniteStreamSource(int delayMillis, long maxSequence) {
            this.delayMillis = delayMillis;
            this.maxSequence = maxSequence;
        }

        void fillBuffer(SourceBuffer<Long> buf) {
            long nowMillis = System.currentTimeMillis();
            if (nowMillis >= emitSchedule) {
                buf.add(sequence++);
                emitSchedule = nowMillis + delayMillis;
                if (sequence > maxSequence) {
                    buf.close();
                }
            }
        }
    }
}
