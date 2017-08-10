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
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

/**
 * Query map while members of the cluster are being shutdown and started
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class QueryBounceTest {

    private static final String TEST_MAP_NAME = "employees";
    private static final int COUNT_ENTRIES = 100000;
    private static final int CONCURRENCY = 10;

    private IMap<String, SampleTestObjects.Employee> map;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
                                                               .clusterSize(4)
                                                               .driverCount(4).build();

    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() {
        if (testName.getMethodName().contains("Indexes")) {
            map = getMapWithIndexes();
        } else {
            map = getMap();
        }
        populateMap(map);
    }

    @Test
    public void testQuery() {
        prepareAndRunQueryTasks();
    }

    @Test
    public void testQueryWithIndexes() {
        prepareAndRunQueryTasks();
    }

    protected Config getConfig() {
        return new Config();
    }

    private void prepareAndRunQueryTasks() {
        QueryRunnable[] testTasks = new QueryRunnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver());
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
    }

    private IMap<String, SampleTestObjects.Employee> getMap() {
        return bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME);
    }

    // obtain a reference to test map from 0-th member with indexes created for Employee attributes
    private IMap<String, SampleTestObjects.Employee> getMapWithIndexes() {
        IMap<String, SampleTestObjects.Employee> map = bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME);
        map.addIndex("id", false);
        map.addIndex("age", true);
        return map;
    }

    private void populateMap(IMap<String, SampleTestObjects.Employee> map) {
        for (int i = 0; i < COUNT_ENTRIES; i++) {
            SampleTestObjects.Employee e = new SampleTestObjects.Employee(i, "name" + i, i, true, i);
            map.put("name" + i, e);
        }
    }

    public static class QueryRunnable implements Runnable {

        private final HazelcastInstance hazelcastInstance;
        // query age min-max range, min is randomized, max = min+1000
        private final Random random = new Random();
        private final int numberOfResults = 1000;
        private IMap map;

        public QueryRunnable(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void run() {
            if (map == null) {
                map = hazelcastInstance.getMap(TEST_MAP_NAME);
            }
            int min, max;
            min = random.nextInt(COUNT_ENTRIES - numberOfResults);
            max = min + numberOfResults;
            String sql = (min % 2 == 0)
                    ? "age >= " + min + " AND age < " + max // may use sorted index
                    : "id >= " + min + " AND id < " + max;  // may use unsorted index
            Collection<SampleTestObjects.Employee> employees = map.values(new SqlPredicate(sql));
            assertEquals("There is data loss", COUNT_ENTRIES, map.size());
            assertEquals("Obtained " + employees.size() + " results for query '" + sql + "'",
                    numberOfResults, employees.size());
        }
    }

}
