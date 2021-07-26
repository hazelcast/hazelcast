/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class GracefulShutdownStressTest extends JetTestSupport {

    @Test
    public void test_smartClient() throws Throwable {
        test(true);
    }

    @Test
    @Ignore // https://github.com/hazelcast/hazelcast/issues/19171
    public void test_nonSmartClient() throws Throwable {
        test(false);
    }

    private void test(boolean useSmartRouting) throws Throwable {
        /*
        This test will start wih 1 member. Then it will continually add/remove members, while
        in parallel it will submit queries. The queries must not fail. All queries are batch queries
        so that they prevent a graceful shutdown. We also set the page size to 1 to ensure that
        the query is executed using multiple iterations.
         */
        createHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(useSmartRouting);
        HazelcastInstance client = createHazelcastClient(clientConfig);

        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger queriesExecuted = new AtomicInteger();
        AtomicInteger membersAddedRemoved = new AtomicInteger();
        AtomicBoolean terminated = new AtomicBoolean();

        // TODO [viliam] the test should include also master shutdown

        List<Thread> threads = new ArrayList<>();

        // add threads executing queries
        for (int i = 0; i < 10; i++) {
            threads.add(new Thread(() -> {
                try {
                    SqlStatement stmt = new SqlStatement("select * from table(generate_series(1, 2))")
                            .setCursorBufferSize(1);
                    // TODO [viliam] ensure that 2 client calls are made: execute & one fetch
                    while (!terminated.get()) {
                        Iterator<SqlRow> iterator = client.getSql().execute(stmt).iterator();
                        assertEquals(1, (int) iterator.next().getObject(0));
                        assertEquals(2, (int) iterator.next().getObject(0));
                        assertFalse(iterator.hasNext());

                        queriesExecuted.incrementAndGet();
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                }
            }));
        }

        // add a thread starting/stopping members
        for (int i = 0; i < 2; i++) {
            threads.add(new Thread(() -> {
                try {
                    while (!terminated.get()) {
                        HazelcastInstance inst = createHazelcastInstance();
                        inst.shutdown();
                        membersAddedRemoved.incrementAndGet();
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                }
            }));
        }


        for (Thread t : threads) {
            t.start();
        }

        sleepSeconds(10);
        terminated.set(true);

        for (Thread t : threads) {
            t.join();
        }

        if (error.get() != null) {
            throw error.get();
        }
        assertThat(queriesExecuted.get()).as("queries executed").isGreaterThan(10);
        assertThat(membersAddedRemoved.get()).as("members added/removed").isGreaterThan(10);
    }
}
