/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map;

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
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class QueryDuringMigrationsStressTest extends HazelcastTestSupport {

    protected static final int CONCURRENT_QUERYING_CLIENTS = 10;
    protected static final ILogger LOGGER = Logger.getLogger(QueryDuringMigrationsStressTest.class);

    private static final long TEST_DURATION_SECONDS = MINUTES.toSeconds(3);
    private static final String TEST_MAP_NAME = "employees";
    private static final int CLUSTER_SIZE = 6;

    protected TestHazelcastInstanceFactory factory = createFactory();

    // members[0] stays up all the time during the test, the rest are shutdown/started during the test
    private HazelcastInstance[] members;
    private ExecutorService queriesExecutor;

    private final AtomicBoolean testRunning = new AtomicBoolean();
    private final AtomicBoolean testFailed = new AtomicBoolean();
    private final Collection<String> failureMessages = Collections.synchronizedCollection(new ArrayList<String>());

    private final int numberOfEntries = 100000;

    @Before
    public void setup() {
        Config config = getConfig();
        members = new HazelcastInstance[CLUSTER_SIZE];
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            members[i] = factory.newHazelcastInstance(config);
        }

        setupInternal();

        testRunning.set(true);
        testFailed.set(false);
        queriesExecutor = Executors.newFixedThreadPool(CONCURRENT_QUERYING_CLIENTS);
    }

    protected void setupInternal() {
    }

    @After
    public void teardown() {
        queriesExecutor.shutdown();
        factory.terminateAll();
    }

    // Test on a cluster where members shutdown & startup, map without indexes
    @Test(timeout = 4 * MINUTE)
    public void testQueryMapWithoutIndexes_whileShutdownStartup() throws InterruptedException {
        IMap<String, SampleObjects.Employee> map = getMapWithoutIndexes();
        populateMap(map);
        queryDuringMigrations();
    }

    // Test on a cluster where members shutdown & startup, map with indexes
    // see also https://github.com/hazelcast/hazelcast/issues/8931, https://github.com/hazelcast/hazelcast/issues/8046
    // and https://github.com/hazelcast/hazelcast/issues/9043
    @Test(timeout = 4 * MINUTE)
    public void testQueryMapWithIndexes_whileShutdownStartup() throws InterruptedException {
        IMap<String, SampleObjects.Employee> map = getMapWithIndexes();
        populateMap(map);
        queryDuringMigrations();
    }

    private void queryDuringMigrations() throws InterruptedException {
        Future[] queryingFutures = queryContinuously();

        Future shuffleMembersFuture = shuffleMembers();

        // let the test run for 3 minutes or until failed, whichever comes first
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(failureMessages.toString(), testFailed.get());
            }
        }, TEST_DURATION_SECONDS);

        // stop querying threads
        testRunning.set(false);

        for (Future f : queryingFutures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                fail("A querying thread failed with exception " + e.getMessage());
            }
        }

        try {
            // await member shuffling thread to stop
            shuffleMembersFuture.get();
        } catch (ExecutionException e) {
            // ignore the failure, not related to the test itself
            e.printStackTrace();
        }
    }

    private Future shuffleMembers() {
        return spawn(new MemberUpDownMonkey(members));
    }

    private void populateMap(IMap<String, SampleObjects.Employee> map) {
        for (int i = 0; i < numberOfEntries; i++) {
            SampleObjects.Employee e = new SampleObjects.Employee(i, "name" + i, i, true, i);
            map.put("name" + i, e);
        }
        LOGGER.info("Done populating map with " + numberOfEntries + " entries.");
    }

    private Future[] queryContinuously() {
        Future[] futures = new Future[CONCURRENT_QUERYING_CLIENTS];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = queriesExecutor.submit(new QueryRunnable(getQueryingInstance(i)));
        }
        return futures;
    }

    protected HazelcastInstance getQueryingInstance(int ix) {
        return getFirstMember();
    }

    protected final HazelcastInstance getFirstMember() {
        return members[0];
    }

    private class QueryRunnable implements Runnable {

        private final IMap map;
        // query age min-max range, min is randomized, max = min+1000
        private final Random random = new Random();
        private final int numberOfResults = 1000;

        QueryRunnable(HazelcastInstance hz) {
            this.map = hz.getMap(TEST_MAP_NAME);
        }

        @Override
        public void run() {
            int min, max, correctResultsCount = 0;
            while (testRunning.get()) {
                try {
                    min = random.nextInt(numberOfEntries - numberOfResults);
                    max = min + numberOfResults;
                    String sql = (min % 2 == 0)
                            ? "age >= " + min + " AND age < " + max // sorted
                            : "id >= " + min + " AND id < " + max;  //unsorted
                    Collection<SampleObjects.Employee> employees = map.values(new SqlPredicate(sql));
                    if (employees.size() != numberOfResults) {
                        String message = "Obtained " + employees.size() + " results for query '" + sql + "'";
                        System.err.println(message);
                        failureMessages.add(message);
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
                    failureMessages.add("A query thread failed with: " + e.getMessage());
                    testFailed.set(true);
                    testRunning.set(false);
                    LOGGER.severe("Query thread failed with exception", e);
                    throw e;
                }
            }
        }
    }

    private class MemberUpDownMonkey implements Runnable {
        private final HazelcastInstance[] instances;

        MemberUpDownMonkey(HazelcastInstance[] allInstances) {
            this.instances = new HazelcastInstance[allInstances.length - 1];
            // exclude 0-index instance
            System.arraycopy(allInstances, 1, instances, 0, allInstances.length - 1);
        }

        @Override
        public void run() {
            int i = 0;
            int nextInstance;
            while (testRunning.get()) {
                instances[i].shutdown();
                nextInstance = (i + 1) % instances.length;
                sleepSeconds(2);

                instances[i] = factory.newHazelcastInstance();
                sleepSeconds(2);
                // move to next member
                i = nextInstance;
            }
        }
    }

    // obtain a reference to test map from 0-th member with indexes created for Employee attributes
    private IMap<String, SampleObjects.Employee> getMapWithIndexes() {
        IMap<String, SampleObjects.Employee> map = getFirstMember().getMap(TEST_MAP_NAME);
        map.addIndex("id", false);
        map.addIndex("age", true);
        return map;
    }

    // obtain a reference to test map from 0-th member without indexes
    private IMap<String, SampleObjects.Employee> getMapWithoutIndexes() {
        return getFirstMember().getMap(TEST_MAP_NAME);
    }

    protected TestHazelcastInstanceFactory createFactory() {
        return createHazelcastInstanceFactory();
    }
}
