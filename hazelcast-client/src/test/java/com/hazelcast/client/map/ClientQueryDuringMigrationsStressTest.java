/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.TimeConstants.MINUTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test querying a cluster while members are shutting down and joining.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientQueryDuringMigrationsStressTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(ClientQueryDuringMigrationsStressTest.class);

    private static final String TEST_MAP_NAME = "employees";
    private static final int CONCURRENT_QUERYING_CLIENTS = 10;
    private static final int CLUSTER_SIZE = 6;

    private TestHazelcastFactory factory;
    // members[0] stays up all the time during the test, the rest are shutdown/started during the test
    private HazelcastInstance[] members;
    private HazelcastInstance[] clients;
    private ExecutorService queriesExecutor;

    final AtomicBoolean testRunning = new AtomicBoolean();
    final AtomicBoolean testFailed = new AtomicBoolean();
    final StringBuilder failureMessageBuilder = new StringBuilder();

    @Before
    public void setup() {
        Config config = getConfig();
        factory = new TestHazelcastFactory();
        members = new HazelcastInstance[CLUSTER_SIZE];
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            members[i] = factory.newHazelcastInstance(config);
        }

        clients = new HazelcastInstance[CONCURRENT_QUERYING_CLIENTS];
        for (int i = 0; i < CONCURRENT_QUERYING_CLIENTS; i++) {
            clients[i] = factory.newHazelcastClient(getClientConfig(members[0]));
        }

        testRunning.set(true);
        testFailed.set(false);
        queriesExecutor = Executors.newFixedThreadPool(CONCURRENT_QUERYING_CLIENTS);
    }

    @After
    public void teardown() {
        queriesExecutor.shutdown();
        factory.shutdownAll();
    }

    // Test on a cluster where members shutdown & startup, map without indexes
    @Test(timeout = 4 * MINUTE)
    public void testQueryMapWithoutIndexes_whileShutdownStartup() throws InterruptedException {
        IMap<String, SampleObjects.Employee> map = getMapWithoutIndexes();
        populateMap(map, 100000);
        queryDuringMigrations(map);
    }

    // Test on a cluster where members shutdown & startup, map with indexes
    // Currently ignored, as indexed queries during migrations are not executed
    // safely.
    // see also https://github.com/hazelcast/hazelcast/issues/8931, https://github.com/hazelcast/hazelcast/issues/8046
    // and https://github.com/hazelcast/hazelcast/issues/9043
    @Ignore
    @Test(timeout = 4 * MINUTE)
    public void testQueryMapWithIndexes_whileShutdownStartup() throws InterruptedException {
        IMap<String, SampleObjects.Employee> map = getMapWithIndexes();
        populateMap(map, 100000);
        queryDuringMigrations(map);
    }

    private void queryDuringMigrations(IMap<String, SampleObjects.Employee> map)
            throws InterruptedException {

        Future[] queryingFutures = queryContinuously(clients, queriesExecutor, CONCURRENT_QUERYING_CLIENTS);

        shuffleMembers(factory, members);

        // let the test run for 3 minutes or until failed, whichever comes first
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(failureMessageBuilder.toString(), testFailed.get());
            }
        }, MINUTES.toSeconds(3));

        // stop querying threads
        testRunning.set(false);

        for (Future f : queryingFutures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                fail("A querying thread failed with exception " + e.getMessage());
            }
        }
    }

    private void shuffleMembers(TestHazelcastFactory nodeFactory, HazelcastInstance[] instances) {
        Thread t = new Thread(new MemberUpDownMonkey(nodeFactory, instances));
        t.start();
    }

    private void populateMap(IMap<String, SampleObjects.Employee> map, int numberOfEntries) {
        for (int i = 0; i < numberOfEntries; i++) {
            SampleObjects.Employee e = new SampleObjects.Employee(i, "name" + i, i, true, i);
            map.put("name" + i, e);
        }
        LOGGER.info("Done populating map with " + numberOfEntries + " entries.");
    }

    private Future[] queryContinuously(HazelcastInstance[] instances, ExecutorService executor, int concurrency) {
        Future[] futures = new Future[concurrency];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = executor.submit(new QueryRunnable(instances[i]));
        }
        return futures;
    }

    public class QueryRunnable implements Runnable {

        // query age min-max range, min is randomized, max = min+1000
        private final Random random = new Random();
        private final IMap map;

        public QueryRunnable(HazelcastInstance hz) {
            this.map = hz.getMap(TEST_MAP_NAME);
        }

        @Override
        public void run() {
            int min, max, correctResultsCount = 0;
            while (testRunning.get()) {
                try {
                    min = random.nextInt(99000);
                    max = min + 1000;
                    Collection<SampleObjects.Employee> employees = map.values(new SqlPredicate("age >= " + min +
                            " AND age < " + max));
                    if (employees.size() != 1000) {
                        // error
                        failureMessageBuilder.append("Obtained " + employees.size() + " results for query \"age >= " + min +
                                " AND age < " + max + "\"");
                        testFailed.set(true);
                        testRunning.set(false);
                    } else {
                        correctResultsCount++;
                        if (correctResultsCount % 20 == 0) {
                            LOGGER.info("Obtained " + correctResultsCount + " correct results");
                        }
                    }
                } catch (RuntimeException e) {
                    // runtime exception caught, fail the test and rethrow
                    testFailed.set(true);
                    testRunning.set(false);
                    failureMessageBuilder.append("A query thread failed with: " + e.getMessage());
                    LOGGER.severe("Query thread failed with exception", e);
                    throw e;
                }
            }
        }
    }

    public class MemberUpDownMonkey
            implements Runnable {
        private final TestHazelcastFactory nodeFactory;
        private final HazelcastInstance[] instances;

        public MemberUpDownMonkey(TestHazelcastFactory nodeFactory, HazelcastInstance[] instances) {
            this.nodeFactory = nodeFactory;
            this.instances = new HazelcastInstance[instances.length - 1];
            // exclude 0-index instance
            System.arraycopy(instances, 1, this.instances, 0, instances.length - 1);
        }

        @Override
        public void run() {
            int i = 0;
            int nextInstance = 1;
            while (testRunning.get()) {
                instances[i].shutdown();
                nextInstance = (i + 1) % instances.length;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // ignore
                }
                instances[i] = nodeFactory.newHazelcastInstance();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // ignore
                }
                // move to next member
                i = nextInstance;
            }
        }
    }

    // get client configuration that guarantees our client will connect to the specific member
    private ClientConfig getClientConfig(HazelcastInstance member) {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig().setSmartRouting(false);

        InetSocketAddress socketAddress = member.getCluster().getLocalMember().getSocketAddress();

        config.getNetworkConfig().
                addAddress(socketAddress.getHostName() + ":" + socketAddress.getPort());

        return config;
    }

    // obtain a reference to test map from 0-th member with indexes created for Employee attributes
    private IMap<String, SampleObjects.Employee> getMapWithIndexes() {
        IMap<String, SampleObjects.Employee> map = members[0].getMap(TEST_MAP_NAME);
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);
        return map;

    }

    // obtain a reference to test map from 0-th member without indexes
    private IMap<String, SampleObjects.Employee> getMapWithoutIndexes() {
        IMap<String, SampleObjects.Employee> map = members[0].getMap(TEST_MAP_NAME);
        return map;

    }
}
