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
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.core.TestUtil.set;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobListenerTest extends HazelcastTestSupport {
    private static final Function<String, String> SIMPLIFY = log -> log.replaceAll("(?<=\\().*: ", "");

    @Test
    public void testListener_waitForCompletion() {
        testListener(2, TestSources.items(1), Job::join,
                "Jet: NOT_RUNNING -> STARTING",
                "Jet: STARTING -> RUNNING",
                "Jet: RUNNING -> COMPLETED");
    }

    @Test
    public void testListener_suspend_resume_restart_cancelJob() {
        testListener(2, TestSources.itemStream(1),
                job -> {
                    sleepSeconds(2);
                    job.suspend();
                    sleepSeconds(2);
                    job.resume();
                    sleepSeconds(2);
                    job.restart();
                    sleepSeconds(2);
                    job.cancel();
                    assertThrows(CancellationException.class, job::join);
                },
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
    }

    @Test
    public void testListener_jobFails() {
        testListener(2, TestSources.itemStream(1, (t, s) -> 1 / (2 - s)),
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
        AtomicBoolean restarted = new AtomicBoolean();
        testListener(2, new JobConfig().setAutoScaling(true),
                TestSources.itemStream(1,
                        (t, s) -> {
                            if (s == 2 && !restarted.get()) {
                                restarted.set(true);
                                throw new RestartableException();
                            }
                            return s;
                        }),
                (job, listener) -> {
                    sleepSeconds(6);
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
        testListener(2, new JobConfig().setSuspendOnFailure(true),
                TestSources.itemStream(1, (t, s) -> 1 / (2 - s)),
                (job, listener) -> {
                    sleepSeconds(4);
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
        testListener(2, TestSources.itemStream(1),
                (job, listener) -> {
                    sleepSeconds(2);
                    listener.jet.removeJobStatusListener(listener);
                    job.cancel();
                    assertThrows(CancellationException.class, job::join);
                    assertTailEquals(listener.log,
                            "Jet: NOT_RUNNING -> STARTING",
                            "Jet: STARTING -> RUNNING");
                });
    }

    void testListener(int nodeCount, JobConfig config, Object source, BiConsumer<Job, JobStatusLogger> test) {
        HazelcastInstance hz = createHazelcastInstances(nodeCount)[0];

        Pipeline p = Pipeline.create();
        (source instanceof BatchSource
                    ? p.readFrom((BatchSource<?>) source)
                    : p.readFrom((StreamSource<?>) source).withoutTimestamps())
                .writeTo(Sinks.noop());

        JetService jet = hz.getJet();
        JobStatusLogger listener = new JobStatusLogger(jet);
        Job job = jet.newJob(p, config);
        jet.addJobStatusListener(set(job.getId()), listener);
        test.accept(job, listener);
    }

    void testListener(int nodeCount, Object source, BiConsumer<Job, JobStatusLogger> test) {
        testListener(nodeCount, new JobConfig(), source, test);
    }

    void testListener(int nodeCount, Object source, Consumer<Job> test, String... log) {
        testListener(nodeCount, source, (job, listener) -> {
            test.accept(job);
            assertTailEquals(listener.log, log);
        });
    }

    @SafeVarargs
    static <T> void assertTailEquals(List<T> actual, T... expected) {
        assertGreaterOrEquals("length", expected.length, actual.size());
        List<T> tail = asList(expected).subList(expected.length - actual.size(), expected.length);
        assertEquals(tail, actual);
    }

    static class JobStatusLogger implements JobListener {
        final List<String> log = new ArrayList<>();
        final JetService jet;

        JobStatusLogger(JetService jet) {
            this.jet = jet;
        }

        @Override
        public void jobStatusChanged(JobEvent e) {
            log.add(String.format("%s: %s -> %s%s",
                    e.isUserRequested() ? "User" : "Jet", e.getOldStatus(), e.getNewStatus(),
                    e.getDescription() == null ? "" : " (" + e.getDescription() + ")"));
        }
    }
}
