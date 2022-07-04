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

package com.hazelcast.client.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

/**
 * Test removing entries by query from Hazelcast clients while members are
 * shutting down and joining.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientPutAllRemoveBounceTest extends HazelcastTestSupport {
    private static final String TEST_MAP_NAME = "employees";
    private static final int CONCURRENCY = 1;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig()).clusterSize(4).driverCount(4)
            .driverType(BounceTestConfiguration.DriverType.CLIENT).build();

    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Before
    public void setup() {
        bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME).addIndex(IndexType.SORTED, "id");
    }

    @Test
    public void testQuery() {
        QueryRunnable[] testTasks = new QueryRunnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver());
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(1));
    }

    @After
    public void assertMapEmpty() {
        IMap<Integer, Employee> map = bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME);
        assertEquals("Map is not empty ", 0, map.size());
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    public static class QueryRunnable implements Runnable {
        private final HazelcastInstance hazelcastInstance;
        private final Random random = new Random();
        private IMap<Integer, Employee> map;
        int range = 10;
        int keyDomain = Integer.MAX_VALUE;

        QueryRunnable(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void run() {
            if (map == null) {
                map = hazelcastInstance.getMap(TEST_MAP_NAME);
            }
            int min = random.nextInt(keyDomain - range);
            int max = min + range;

            Map<Integer, Employee> m = new HashMap<Integer, Employee>();
            for (int i = min; i < max; i++) {
                m.put(i, new Employee(i, "name" + i, i, true, i));
            }

            map.putAll(m);
            map.removeAll(Predicates.sql("id >= " + min + " and id < " + max));
        }
    }
}
