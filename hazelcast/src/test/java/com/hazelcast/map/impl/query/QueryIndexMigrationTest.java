/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.Value;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.Clock;
import com.hazelcast.util.IterableUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
import static java.lang.Thread.interrupted;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelTest.class})
public class QueryIndexMigrationTest extends HazelcastTestSupport {

    private Random random = new Random();

    private TestHazelcastInstanceFactory nodeFactory;
    private ExecutorService executor;

    @Before
    public void createFactory() {
        nodeFactory = createHazelcastInstanceFactory(6);
    }

    @After
    public void shutdown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        }
        shutdownNodeFactory();
    }

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.<Object[]>asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    private Config getTestConfig() {
        Config config = getConfig();
        config.setProperty(GroupProperty.INDEX_COPY_BEHAVIOR.getName(), copyBehavior.name());
        return config;
    }

    @Test(timeout = MINUTE)
    public void testQueryDuringAndAfterMigration() throws Exception {
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(getTestConfig());
        int count = 500;
        IMap<String, Employee> map = instance.getMap("employees");
        for (int i = 0; i < count; i++) {
            map.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), (double) i));
        }

        nodeFactory.newInstances(getTestConfig(), 3);

        final IMap<String, Employee> employees = instance.getMap("employees");
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
        Config config = getTestConfig();
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("active", false);

        int size = 500;
        for (int i = 0; i < size; i++) {
            map.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), (double) i));
        }

        nodeFactory.newInstances(config, 3);

        final IMap<String, Employee> employees = instance.getMap("employees");
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
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(getTestConfig());
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex("age", true);
        map.addIndex("active", false);

        for (int i = 0; i < 500; i++) {
            map.put("e" + i, new Employee("name" + i, i % 50, ((i & 1) == 1), (double) i));
        }
        assertEquals(500, map.size());
        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("active=true and age>44"));
        assertEquals(30, entries.size());

        nodeFactory.newInstances(getTestConfig(), 3);

        long startNow = Clock.currentTimeMillis();
        while ((Clock.currentTimeMillis() - startNow) < 10000) {
            entries = map.entrySet(new SqlPredicate("active=true and age>44"));
            assertEquals(30, entries.size());
        }
    }

    /**
     * test for issue #359
     */
    @Test(timeout = 4 * MINUTE)
    public void testIndexCleanupOnMigration() throws Exception {
        int nodeCount = 6;
        final int runCount = 500;
        final Config config = newConfigWithIndex("testMap", "name");
        executor = Executors.newFixedThreadPool(nodeCount);
        List<Future<?>> futures = new ArrayList<Future<?>>();

        for (int i = 0; i < nodeCount; i++) {
            sleepMillis(random.nextInt((i + 1) * 100) + 10);
            futures.add(executor.submit(new Runnable() {
                public void run() {
                    HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
                    IMap<Object, Value> map = hz.getMap("testMap");
                    updateMapAndRunQuery(map, runCount);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
    }

    private Config newConfigWithIndex(String mapName, String attribute) {
        Config config = getTestConfig();
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.getMapConfig(mapName).addMapIndexConfig(new MapIndexConfig(attribute, false));
        return config;
    }

    /**
     * see Zendesk ticket #82
     */
    @Test(timeout = MINUTE)
    public void testQueryWithIndexDuringJoin() throws InterruptedException {
        final String name = "test";
        final String findMe = "find-me";

        int nodeCount = 5;
        final int entryPerNode = 1000;
        final int modulo = 10;
        final CountDownLatch latch = new CountDownLatch(nodeCount);
        final Config config = newConfigWithIndex(name, "name");

        for (int i = 0; i < nodeCount; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
                    IMap<Object, Object> map = hz.getMap(name);
                    fillMap(map, findMe, entryPerNode, modulo);
                    latch.countDown();
                }
            }).start();
        }

        assertTrue(latch.await(1, MINUTES));
        Collection<HazelcastInstance> instances = nodeFactory.getAllHazelcastInstances();
        assertEquals(nodeCount, instances.size());
        waitAllForSafeState(instances);

        int expected = entryPerNode / modulo * nodeCount;
        for (HazelcastInstance hz : instances) {
            IMap<Object, Object> map = hz.getMap(name);
            Predicate predicate = equal("name", findMe);
            for (int i = 0; i < 10; i++) {
                int size = map.values(predicate).size();
                assertEquals(expected, size);
            }
        }
    }

    private void updateMapAndRunQuery(final IMap<Object, Value> map, final int runCount) {
        String name = randomString();
        Predicate<?, ?> predicate = equal("name", name);
        map.put(name, new Value(name, 0));
        // helper call on nodes to sync partitions (see issue github.com/hazelcast/hazelcast/issues/1282)
        map.size();

        for (int i = 1; i <= runCount && !interrupted(); i++) {
            Value value = map.get(name);
            value.setIndex(i);
            map.put(name, value);

            sleepMillis(random.nextInt(100) + 1);

            Collection<Value> values = map.values(predicate);
            assertEquals(1, values.size());
            Value firstValue = IterableUtil.getFirst(values, null);
            assertEquals(value, firstValue);
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
