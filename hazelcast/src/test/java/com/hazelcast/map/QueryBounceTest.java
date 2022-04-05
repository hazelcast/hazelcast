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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

/**
 * Query map while members of the cluster are being shutdown and started
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class QueryBounceTest {

    private static final String TEST_MAP_NAME = "employees";
    private static final int COUNT_ENTRIES = 10000;
    private static final int CONCURRENCY = 10;

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(getConfig())
                    .clusterSize(4)
                    .driverCount(4)
                    .driverType(BounceTestConfiguration.DriverType.MEMBER)
                    .useTerminate(useTerminate())
                    .build();

    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Test
    public void testQuery() {
        prepareAndRunQueryTasks(false);
    }

    @Test
    public void testQueryWithIndexes() {
        prepareAndRunQueryTasks(true);
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    // XXX: It's better to use parametrized test here, but parameters don't play
    // nice with rules: parameters are injected after rules instantiation.
    protected boolean useTerminate() {
        return false;
    }

    private void prepareAndRunQueryTasks(boolean withIndexes) {
        IMap<String, SampleTestObjects.Employee> map = bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME);
        if (withIndexes) {
            map.addIndex(IndexType.HASH, "id");
            map.addIndex(IndexType.SORTED, "age");
        }
        populateMap(map);

        QueryRunnable[] testTasks = new QueryRunnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver(), withIndexes);
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
    }

    private void populateMap(IMap<String, SampleTestObjects.Employee> map) {
        for (int i = 0; i < COUNT_ENTRIES; i++) {
            SampleTestObjects.Employee e = new SampleTestObjects.Employee(i, "name" + i, i, true, i);
            map.put("name" + i, e);
        }
    }

    protected Predicate makePredicate(String attribute, int min, int max, boolean withIndexes) {
        return Predicates.sql(attribute + " >= " + min + " AND " + attribute + " < " + max);
    }

    public class QueryRunnable implements Runnable {

        private final HazelcastInstance hazelcastInstance;
        private final boolean withIndexes;
        // query age min-max range, min is randomized, max = min+1000
        private final Random random = new Random();
        private final int numberOfResults = 1000;
        private IMap<String, SampleTestObjects.Employee> map;

        public QueryRunnable(HazelcastInstance hazelcastInstance, boolean withIndexes) {
            this.hazelcastInstance = hazelcastInstance;
            this.withIndexes = withIndexes;
        }

        @Override
        public void run() {
            if (map == null) {
                map = hazelcastInstance.getMap(TEST_MAP_NAME);
            }
            int min = random.nextInt(COUNT_ENTRIES - numberOfResults);
            int max = min + numberOfResults;
            String attribute = min % 2 == 0 ? "age" : "id";
            Predicate predicate = makePredicate(attribute, min, max, withIndexes);
            Collection<SampleTestObjects.Employee> employees = map.values(predicate);
            assertEquals("There is data loss", COUNT_ENTRIES, map.size());
            assertEquals("Obtained " + employees.size() + " results for query '" + predicate + "'",
                    numberOfResults, employees.size());
        }

    }

}
