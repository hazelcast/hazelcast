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

package com.hazelcast.jet.job;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.test.IgnoredForCoverage;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

/**
 * This test is ignored due to being flaky - it tries to submit as many jobs as
 * possible and checks if the rate doesn't go down.
 * Any larger GC pause or noisy neighbor on the build machine can cause the test to fail.
 * We keep the test so we can run it on demand when required.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, IgnoredForCoverage.class})
@Ignore
public final class JobSubmissionSlownessRegressionTest extends JetTestSupport {

    private static final int DURATION_SECS = 10;
    private static final int THREADS_COUNT = 5;

    private static final int HEAT_UP_CYCLE_COUNT = 3;
    private static final int MEASUREMENT_A_CYCLE_COUNT = 3;
    private static final int WAIT_BEFORE_MEASUREMENT_B_COUNT = 3;
    private static final int MEASUREMENT_B_CYCLE_COUNT = 3;

    private static final int HEAT_UP_CYCLE_SECTION = HEAT_UP_CYCLE_COUNT;
    private static final int MEASUREMENT_A_CYCLE_SECTION = HEAT_UP_CYCLE_SECTION + MEASUREMENT_A_CYCLE_COUNT;
    private static final int WAIT_BEFORE_MEASUREMENT_B_SECTION
            = MEASUREMENT_A_CYCLE_SECTION + WAIT_BEFORE_MEASUREMENT_B_COUNT;
    private static final int MEASUREMENT_B_CYCLE_SECTION = WAIT_BEFORE_MEASUREMENT_B_SECTION + MEASUREMENT_B_CYCLE_COUNT;

    private static int measurementCount;

    @Before
    public void setup() {
        Config config = defaultInstanceConfigWithJetEnabled();
        config.setProperty("hazelcast.logging.type", "none");
        createHazelcastInstance(config);
    }

    @Test
    public void regressionTestForPR1488() {
        logger.info(String.format("Starting test with %d threads", THREADS_COUNT));

        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_COUNT);

        double measurementARateSum = 0;
        double measurementBRateSum = 0;

        DAG dag = twoVertex();
        HazelcastInstance client = createHazelcastClient();
        while (measurementCount < MEASUREMENT_B_CYCLE_SECTION) {
            AtomicInteger completedRoundTrips = new AtomicInteger();
            long start = System.nanoTime();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < THREADS_COUNT; i++) {
                Future<?> f = executorService.submit(() -> {
                    bench(() -> {
                        client.getJet().newJob(dag, new JobConfig()).join();
                    }, completedRoundTrips);
                });
                futures.add(f);
            }
            futures.forEach(f -> uncheckRun(f::get));

            long elapsed = System.nanoTime() - start;
            double rate = (double) completedRoundTrips.get() / (double) elapsed * SECONDS.toNanos(1);
            System.out.println("Rate was " + rate + " req/s");
            measurementCount++;

            if (measurementCount > HEAT_UP_CYCLE_SECTION) { //3
                if (measurementCount <= MEASUREMENT_A_CYCLE_SECTION) { //6
                    measurementARateSum += rate;
                } else if (measurementCount > WAIT_BEFORE_MEASUREMENT_B_SECTION) { //9
                    measurementBRateSum += rate;
                }
            }
        }

        double measurementARate = measurementARateSum / MEASUREMENT_A_CYCLE_COUNT;
        double measurementBRate = measurementBRateSum / MEASUREMENT_B_CYCLE_COUNT;

        assertTrue("Job submission rate should not decrease. First rate: " + measurementARate
                + ", second rate: " + measurementBRate, measurementARate * 0.8 < measurementBRate);
    }

    private static DAG twoVertex() {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v", noopP());
        Vertex v2 = dag.newVertex("v2", noopP());
        dag.edge(Edge.between(v1, v2).setConfig(new EdgeConfig().setQueueSize(1)));
        return dag;
    }

    private static void bench(Runnable r, AtomicInteger completedRoundTrips) {
        long start = System.nanoTime();
        long end = start + SECONDS.toNanos(DURATION_SECS);
        while (System.nanoTime() < end) {
            r.run();
            completedRoundTrips.incrementAndGet();
        }
    }
}
