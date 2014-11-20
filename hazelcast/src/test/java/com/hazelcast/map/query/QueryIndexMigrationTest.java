/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.SampleObjects.Value;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import com.hazelcast.util.IterableUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static com.hazelcast.util.IterableUtil.getFirst;
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueryIndexMigrationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(6);
    private ExecutorService executor;
    private Random rand = new Random();

    @After
    public void shutdown() throws Exception {
        shutdownNodeFactory();
        if( executor != null ) {
            executor.shutdownNow();
            executor.awaitTermination(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        }
    }

    @Test(timeout = MINUTE)
    public void testQueryDuringAndAfterMigration() throws Exception {
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        int count = 500;
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < count; i++) {
            imap.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), (double) i));
        }

        nodeFactory.newInstances(new Config(), 3);

        final IMap employees = h1.getMap("employees");
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<Employee> values = employees.values(new SqlPredicate("active and name LIKE 'joe15%'"));
                for (Employee employee : values) {
                    assertTrue(employee.isActive());
                }
                assertEquals(6, values.size());
            }
        }, 3);
    }

    @Test
    public void testQueryDuringAndAfterMigrationWithIndex() throws Exception {
        Config cfg = new Config();
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);

        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("active", false);

        int size = 500;
        for (int i = 0; i < size; i++) {
            imap.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), (double) i));
        }

        nodeFactory.newInstances(cfg, 3);

        final IMap employees = h1.getMap("employees");
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<Employee> values = employees.values(new SqlPredicate("active and name LIKE 'joe15%'"));
                for (Employee employee : values) {
                    assertTrue(employee.isActive() && employee.getName().startsWith("joe15"));
                }
                assertEquals(6, values.size());
            }
        }, 3);

    }

    @Test
    public void testQueryWithIndexesWhileMigrating() throws Exception {

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("age", true);
        imap.addIndex("active", false);

        for (int i = 0; i < 500; i++) {
            imap.put("e" + i, new Employee("name" + i, i % 50, ((i & 1) == 1), (double) i));
        }
        assertEquals(500, imap.size());
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active=true and age>44"));
        assertEquals(30, entries.size());

        nodeFactory.newInstances(new Config(), 3);

        long startNow = Clock.currentTimeMillis();
        while ((Clock.currentTimeMillis() - startNow) < 10000) {
            entries = imap.entrySet(new SqlPredicate("active=true and age>44"));
            assertEquals(30, entries.size());
        }
    }

    /**
     * test for issue #359
     */
    @Test(timeout = MINUTE)
    public void testIndexCleanupOnMigration() throws Exception {
        final int n = 6;
        final int runCount = 500;
        final Config config = newConfigWithIndex("testMap", "name");
        executor = Executors.newFixedThreadPool(n);
        List<Future<?>> futures = new ArrayList<Future<?>>();

        for (int i = 0; i < n; i++) {
            sleepMillis(rand.nextInt((i + 1) * 100) + 10);
            futures.add( executor.submit(new Runnable() {
                public void run() {
                    HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
                    IMap<Object, Value> map = hz.getMap("testMap");
                    updateMapAndRunQuery(map, runCount);
                }
            }));
        }

        for(Future<?> f: futures) {
            f.get();
        }
    }

    private Config newConfigWithIndex(final String mapName, String attribute) {
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config.getMapConfig(mapName).addMapIndexConfig(new MapIndexConfig(attribute, false));
        return config;
    }

    /**
     * see zendesk ticket #82
     */
    @Test(timeout = MINUTE)
    public void testQueryWithIndexDuringJoin() throws InterruptedException {
        final String name = "test";
        final String FIND_ME = "find-me";

        final int nodes = 5;
        final int entryPerNode = 1000;
        final int modulo = 10;
        final CountDownLatch latch = new CountDownLatch(nodes);
        final Config config = newConfigWithIndex(name, "name");

        for (int n = 0; n < nodes; n++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
                    IMap<Object, Object> map = hz.getMap(name);
                    fillMap(map, FIND_ME, entryPerNode, modulo);
                    latch.countDown();
                }
            }).start();
        }

        assertTrue(latch.await(5, MINUTES));
        Collection<HazelcastInstance> instances = nodeFactory.getAllHazelcastInstances();
        assertEquals(nodes, instances.size());
        waitClusterForSafeState( getFirst(instances, null) );

        final int expected = entryPerNode / modulo * nodes;

        for (HazelcastInstance hz : instances) {
            IMap<Object, Object> map = hz.getMap(name);
            Predicate predicate = equal("name", FIND_ME);
            for (int i = 0; i < 10; i++) {
                int size = map.values(predicate).size();
                assertEquals(expected, size);
            }
        }
    }

    private void updateMapAndRunQuery(final IMap<Object, Value> map, final int runCount) {
        final String name = randomString();
        Predicate<?, ?> predicate = equal("name", name);
        map.put(name, new Value(name, 0));
        map.size();  // helper call on nodes to sync partitions.. see issue github.com/hazelcast/hazelcast/issues/1282

        for (int j = 1; j <= runCount && !interrupted(); j++) {
            Value v = map.get(name);
            v.setIndex(j);
            map.put(name, v);

            sleepMillis(rand.nextInt(100) + 1);

            Collection<Value> values = map.values(predicate);
            assertEquals(1, values.size());
            Value v2 = IterableUtil.getFirst(values, null);
            assertEquals(v, v2);
        }
    }

    private static void fillMap(IMap<Object, Object> map, final String value, final int count, final int modulo) {
        for (int i = 0; i < count; i++) {
            String name = randomString();
            if (i % modulo == 0) {
                name = value;
            }

            map.put(randomString(), new Value(name, i));
        }
    }

}
