/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.core.TestProcessors.streamingDag;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlGracefulShutdownTest extends JetTestSupport {

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        instances = createHazelcastInstances(2);
    }

    @Test
    public void when_shuttingDown_then_clientRetriesWithTheGoodMember() throws Exception {
        Job jobPreventingShutdown = instances[0].getJet().newLightJob(streamingDag(),
                new JobConfig().setPreventShutdown(true));
        assertTrueEventually(() -> assertJobExecuting(jobPreventingShutdown, instances[1]));

        Future<?> shutdownFuture = spawn(() -> instances[1].shutdown());

        // the client chooses a random member. So let's try multiple times - if it randomly chooses the
        // shutting-down member, it should still work by retrying with the other member.
        for (int i = 0; i < 10; i++) {
            HazelcastInstance client = createHazelcastClient();
            for (SqlRow r : client.getSql().execute("select * from table(generate_series(1, 1))")) {
                System.out.println(r);
            }
        }

        jobPreventingShutdown.cancel();
        shutdownFuture.get();
    }

    @Test
    public void stressTest_smartClient() throws Throwable {
        stressTest(true);
    }

    @Test
    @Ignore // https://github.com/hazelcast/hazelcast/issues/19171
    public void stressTest_nonSmartClient() throws Throwable {
        stressTest(false);
    }

    private void stressTest(boolean useSmartRouting) throws Throwable {
        /*
        This test will start wih 1 member. Then it will continually add/remove members, while
        in parallel it will submit queries. The queries must not fail. All queries are batch queries
        so that they prevent a graceful shutdown. We also set the page size to 1 to ensure that
        the query is executed using multiple iterations.
         */
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(useSmartRouting);
        HazelcastInstance client = createHazelcastClient(clientConfig);

        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger queriesExecuted = new AtomicInteger();
        AtomicInteger membersAddedRemoved = new AtomicInteger();
        AtomicBoolean terminated = new AtomicBoolean();

        List<Thread> threads = new ArrayList<>();
        try {
            // TODO [viliam] the test should include also master shutdown

            // add threads executing queries
            for (int i = 0; i < 2; i++) {
                threads.add(new Thread(() -> {
                    try {
                        SqlStatement stmt = new SqlStatement("select * from table(generate_series(1, 2))")
                                .setCursorBufferSize(1);
                        // TODO [viliam] check that 2 client calls are made: execute & one fetch
                        while (!terminated.get()) {
                            Iterator<SqlRow> iterator = client.getSql().execute(stmt).iterator();
                            assertEquals(1, (int) iterator.next().getObject(0));
                            assertEquals(2, (int) iterator.next().getObject(0));
                            assertFalse(iterator.hasNext());

                            queriesExecuted.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        logger.info("", e);
                        error.compareAndSet(null, e);
                    }
                }, "queryTread-" + i));
            }

            // add a thread starting/stopping members
            for (int i = 0; i < 2; i++) {
                threads.add(new Thread(() -> {
                    try {
                        while (!terminated.get()) {
                            logger.info("creating instance");
                            HazelcastInstance inst = createHazelcastInstance();
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

        } finally {
            terminated.set(true);

            for (Thread t : threads) {
                t.join();
            }

            logger.info(queriesExecuted.get() + " queries executed, " + membersAddedRemoved.get() + " members added/removed");
        }
        checkError(error);
        assertThat(queriesExecuted.get()).as("queries executed").isGreaterThan(10);
        assertThat(membersAddedRemoved.get()).as("members added/removed").isGreaterThan(10);
    }

    private void checkError(AtomicReference<Throwable> error) {
        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
    }
}
