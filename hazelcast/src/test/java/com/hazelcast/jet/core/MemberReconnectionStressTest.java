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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class MemberReconnectionStressTest extends JetTestSupport {

    private final AtomicBoolean terminated = new AtomicBoolean();

    @After
    public void after() {
        terminated.set(true);
    }

    @Test
    public void test() {
        /*
        The test will start 2 thread:
        - one will submit short batch jobs, serially, after joining the previous job
        - the other will keep dropping the member-to-member connection.

        The small jobs will often fail due to the reconnection, we check
        that the job completes in a limited time. We drop the connection
        at a rate lower than the normal job duration so there's
        typically at most 1 restart per job. We assert that the jobs
        eventually successfully complete.
         */
        Config config = defaultInstanceConfigWithJetEnabled();
        // The connection drop often causes regular IMap operations to fail - shorten the timeout so that
        // it recovers more quickly
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "2000");
        config.setClusterName(randomName());

        HazelcastInstance inst1 = createHazelcastInstance(config);
        HazelcastInstance inst2 = createHazelcastInstance(config);
        logger.info("Instances started");

        new Thread(() -> {
            while (!terminated.get()) {
                Connection connection = null;
                while (connection == null) {
                    connection = ImdgUtil.getMemberConnection(getNodeEngineImpl(inst1),
                            getNodeEngineImpl(inst2).getThisAddress());
                }
                connection.close("test", new Exception("test failure"));
                logger.info("connection closed");
                sleepMillis(300);
            }
        }).start();

        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", () -> new MockP()).localParallelism(2);
        Vertex v2 = dag.newVertex("v2", () -> new MockP()).localParallelism(2);
        dag.edge(between(v1, v2).distributed());

        AtomicInteger jobCount = new AtomicInteger();
        new Thread(() -> {
            while (!terminated.get()) {
                try {
                    inst1.getJet().newJob(dag).getFuture().join();
                    logger.info("job completed");
                    jobCount.incrementAndGet();
                } catch (Exception e) {
                    logger.info("Job failed, ignoring it", e);
                }
            }
        }).start();

        // in a loop check that the `jobCount` is incremented at least every N seconds
        long lastIncrement = System.nanoTime();
        long lastJobCount = 0;
        long testEndTime = System.nanoTime() + MINUTES.toNanos(3);
        for (long now; (now = System.nanoTime()) < testEndTime; ) {
            if (jobCount.get() > lastJobCount) {
                lastIncrement = now;
                lastJobCount++;
            }
            if (NANOSECONDS.toSeconds(now - lastIncrement) > 30) {
                fail("jobCount didn't increment for 30 seconds");
            }
            sleepMillis(100);
        }
    }
}
