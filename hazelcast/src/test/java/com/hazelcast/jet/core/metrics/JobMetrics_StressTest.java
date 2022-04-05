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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.hazelcast.jet.core.metrics.JobMetrics_BatchTest.JOB_CONFIG_WITH_METRICS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class JobMetrics_StressTest extends JetTestSupport {

    private static final int RESTART_COUNT = 10;
    private static final int TOTAL_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final int WAIT_FOR_METRICS_COLLECTION_TIME = 1200;

    private static volatile Throwable restartThreadException;
    private static volatile Throwable obtainMetricsThreadException;

    private HazelcastInstance instance;

    @Before
    public void setup() {
        restartThreadException = null;
        obtainMetricsThreadException = null;
        IncrementingProcessor.initCount.set(0);
        IncrementingProcessor.completeCount.set(0);

        Config config = defaultInstanceConfigWithJetEnabled();
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        instance = createHazelcastInstance(config);
    }

    @Test
    public void restart_stressTest() throws Throwable {
        stressTest(JobRestartThread::new);
    }

    @Test
    public void suspend_resume_stressTest() throws Throwable {
        stressTest(JobSuspendResumeThread::new);
    }

    private void stressTest(Function<Job, Runnable> restart) throws Throwable {
        DAG dag = buildDag();
        Job job = instance.getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);
        try {
            assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));

            ObtainMetricsThread obtainMetrics = new ObtainMetricsThread(job);
            Thread restartThread = new Thread(restart.apply(job));
            Thread obtainThread = new Thread(obtainMetrics);

            restartThread.start();
            obtainThread.start();

            restartThread.join();
            obtainMetrics.stop = true;
            obtainThread.join();

            if (restartThreadException != null) {
                throw restartThreadException;
            }
            if (obtainMetricsThreadException != null) {
                throw obtainMetricsThreadException;
            }
        } finally {
            ditchJob(job, instance);
        }
    }

    private DAG buildDag() {
        DAG dag = new DAG();
        dag.newVertex("p", (SupplierEx<Processor>) IncrementingProcessor::new);
        return dag;
    }

    private static final class IncrementingProcessor extends AbstractProcessor {

        @Probe(name = "initCount")
        static final AtomicInteger initCount = new AtomicInteger();

        @Probe(name = "completeCount")
        static final AtomicInteger completeCount = new AtomicInteger();

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            initCount.incrementAndGet();
        }

        @Override
        public boolean complete() {
            completeCount.incrementAndGet();
            return false;
        }
    }

    private static class JobRestartThread implements Runnable {

        private final Job job;
        private int restartCount;

        JobRestartThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (restartCount < RESTART_COUNT) {
                    job.restart();
                    sleepMillis(WAIT_FOR_METRICS_COLLECTION_TIME);
                    restartCount++;
                    assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
                }
            } catch (Throwable ex) {
                restartThreadException = ex;
                throw ex;
            }
        }
    }

    private static class JobSuspendResumeThread implements Runnable {

        private final Job job;
        private int restartCount;

        JobSuspendResumeThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (restartCount < RESTART_COUNT) {
                    job.suspend();
                    assertTrueEventually(() -> assertEquals(JobStatus.SUSPENDED, job.getStatus()));
                    sleepMillis(WAIT_FOR_METRICS_COLLECTION_TIME);
                    job.resume();
                    restartCount++;
                    assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
                }
            } catch (Throwable ex) {
                restartThreadException = ex;
                throw ex;
            }
        }
    }

    private static class ObtainMetricsThread implements Runnable {

        volatile boolean stop;
        private final Job job;

        ObtainMetricsThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                long previousInitCountSum = 0;
                long previousCompleteCountSum = 0;
                while (!stop) {
                    assertNotNull(job.getMetrics());

                    Collection<Measurement> initCountMeasurements = job.getMetrics().get("initCount");
                    if (initCountMeasurements.size() != TOTAL_PROCESSORS) {
                        continue;
                    }
                    long initCountSum = initCountMeasurements.stream().mapToLong(Measurement::value).sum();
                    assertTrue("Metrics value should be increasing, current: " + initCountSum
                            + ", previous: " + previousInitCountSum,
                            initCountSum >= previousInitCountSum);
                    previousInitCountSum = initCountSum;

                    Collection<Measurement> completeCountMeasurements = job.getMetrics().get("completeCount");
                    if (completeCountMeasurements.size() != TOTAL_PROCESSORS) {
                        continue;
                    }
                    long completeCountSum = completeCountMeasurements.stream().mapToLong(Measurement::value).sum();
                    assertTrue("Metrics value should be increasing, current: " + completeCountSum
                            + ", previous: " + previousCompleteCountSum,
                            completeCountSum >= previousCompleteCountSum);
                    previousCompleteCountSum = completeCountSum;
                }
                assertTrueEventually(() -> {
                    assertNotNull(job.getMetrics());
                    Collection<Measurement> initCountMeasurements = job.getMetrics().get("initCount");
                    assertEquals(TOTAL_PROCESSORS, initCountMeasurements.size());
                    long sum = initCountMeasurements.stream().mapToLong(Measurement::value).sum();
                    assertEquals((RESTART_COUNT + 1) * TOTAL_PROCESSORS * TOTAL_PROCESSORS, sum);
                }, 3);
            } catch (Throwable ex) {
                obtainMetricsThreadException = ex;
                throw ex;
            }
        }
    }
}
