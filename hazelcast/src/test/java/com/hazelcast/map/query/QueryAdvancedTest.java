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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.SampleObjects.Value;
import com.hazelcast.query.SampleObjects.ValueType;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import com.hazelcast.util.UuidUtil;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.query.QueryBasicTest.doFunctionalQueryTest;
import static com.hazelcast.map.query.QueryBasicTest.doFunctionalSQLQueryTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueryAdvancedTest extends HazelcastTestSupport {

    @Test(timeout = 1000 * 60)
    public void testQueryWithTTL() throws Exception {

        Config cfg = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTimeToLiveSeconds(5);
        mapConfig.setName("employees");
        cfg.addMapConfig(mapConfig);

        HazelcastInstance h1 = createHazelcastInstance(cfg);

        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", false);
        imap.addIndex("active", true);

        int passiveEmployees = 5;
        int activeEmployees = 5;
        int allEmployees = passiveEmployees + activeEmployees;

        final CountDownLatch latch = new CountDownLatch(allEmployees);
        imap.addEntryListener(new EntryAdapter() {
            @Override
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);

        for (int i = 0; i < activeEmployees; i++) {
            Employee employee = new Employee("activeEmployee" + i, 60, true, Double.valueOf(i));
            imap.put("activeEmployee" + i, employee);
        }

        for (int i = 0; i < passiveEmployees; i++) {
            Employee employee = new Employee("passiveEmployee" + i, 60, false, Double.valueOf(i));
            imap.put("passiveEmployee" + i, employee);
        }

        //check the query result before eviction
        Collection values = imap.values(new SqlPredicate("active"));
        assertEquals(activeEmployees, values.size());

        //wait until eviction is completed
        assertOpenEventually(latch);

        //check the query result after eviction
        values = imap.values(new SqlPredicate("active"));
        assertEquals(0, values.size());
    }

    @Test(timeout = 1000 * 60)
    public void testQueryDuringAndAfterMigration() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        int count = 500;
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < count; i++) {
            imap.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), (double) i));
        }

        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h4 = nodeFactory.newHazelcastInstance();

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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        int size = 500;
        for (int i = 0; i < size; i++) {
            imap.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), (double) i));
        }

        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h4 = nodeFactory.newHazelcastInstance(cfg);

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
    public void testQueryWithIndexesWhileMigrating() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 50; i++) {
            Map temp = new HashMap(10);
            for (int j = 0; j < 10; j++) {
                String key = String.valueOf((i * 100000) + j);
                temp.put(key, new Employee("name" + key, i % 60, ((i & 1) == 1), (double) i));
            }
            imap.putAll(temp);
        }
        assertEquals(500, imap.size());
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active=true and age>44"));
        assertEquals(30, entries.size());

        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h4 = nodeFactory.newHazelcastInstance();
        long startNow = Clock.currentTimeMillis();
        while ((Clock.currentTimeMillis() - startNow) < 10000) {
            entries = imap.entrySet(new SqlPredicate("active=true and age>44"));
            assertEquals(30, entries.size());
        }
    }

    @Test(timeout = 1000 * 60)
    public void testTwoNodesWithPartialIndexes() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 500; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), (double) i);
            imap.put(String.valueOf(i), employee);
        }
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        imap = h2.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        Collection<Employee> entries = imap.values(new SqlPredicate("name='name3' and city='city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee e : entries) {
            assertEquals("name3", e.getName());
            assertEquals("city3", e.getCity());
        }
        entries = imap.values(new SqlPredicate("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee e : entries) {
            assertEquals("name3", e.getName());
            assertEquals("city3", e.getCity());
            assertTrue(e.getAge() > 2);
        }
        entries = imap.values(new SqlPredicate("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(5, entries.size());
        for (Employee e : entries) {
            assertTrue(e.getName().startsWith("name3"));
            assertTrue(e.getCity().startsWith("city3"));
        }
    }

    @Test(timeout = 1000 * 60)
    public void testTwoNodesWithIndexes() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("city", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 5000; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), (double) i);
            imap.put(String.valueOf(i), employee);
        }
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        imap = h2.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("city", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        Collection<Employee> entries = imap.values(new SqlPredicate("name='name3' and city='city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee e : entries) {
            assertEquals("name3", e.getName());
            assertEquals("city3", e.getCity());
        }
        entries = imap.values(new SqlPredicate("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee e : entries) {
            assertEquals("name3", e.getName());
            assertEquals("city3", e.getCity());
            assertTrue(e.getAge() > 2);
        }
        entries = imap.values(new SqlPredicate("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(50, entries.size());
        for (Employee e : entries) {
            assertTrue(e.getName().startsWith("name3"));
            assertTrue(e.getCity().startsWith("city3"));
        }
    }


    @Test(timeout = 1000 * 60)
    public void testOneMemberWithoutIndex() {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalQueryTest(imap);
    }

    @Test(timeout = 1000 * 60)
    public void testOneMemberWithIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap imap = instance.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }

    @Test(timeout = 1000 * 60)
    public void testOneMemberSQLWithoutIndex() {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalSQLQueryTest(imap);
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and age>23"));
        assertEquals(27, entries.size());
    }

    @Test(timeout = 1000 * 60)
    public void testOneMemberSQLWithIndex() {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalSQLQueryTest(imap);
    }

    @Test(timeout = 1000 * 60)
    public void testTwoMembers() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalQueryTest(imap);
    }

    @Test(timeout = 1000 * 60)
    public void testTwoMembersWithIndexes() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }

    @Test(timeout = 1000 * 60)
    public void testTwoMembersWithIndexesAndShutdown() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
        assertEquals(101, imap.size());
        h2.getLifecycleService().shutdown();
        assertEquals(101, imap.size());
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }

    @Test(timeout = 1000 * 60)
    public void testTwoMembersWithIndexesAndShutdown2() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
        assertEquals(101, imap.size());
        h1.getLifecycleService().shutdown();
        imap = h2.getMap("employees");
        assertEquals(101, imap.size());
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }

    @Test(timeout = 1000 * 60)
    public void testTwoMembersWithIndexesAndShutdown3() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
        assertEquals(101, imap.size());
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        assertEquals(101, imap.size());
        h1.getLifecycleService().shutdown();
        imap = h2.getMap("employees");
        assertEquals(101, imap.size());
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }

    @Test(timeout = 1000 * 60)
    public void testSecondMemberAfterAddingIndexes() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testMapWithIndexAfterShutDown() {
        Config cfg = new Config();
        cfg.getMapConfig("testMapWithIndexAfterShutDown").addMapIndexConfig(new MapIndexConfig("typeName", false));

        HazelcastInstance[] instances = createHazelcastInstanceFactory(3).newInstances(cfg);

        final IMap map = instances[0].getMap("testMapWithIndexAfterShutDown");
        final int SAMPLE_SIZE_1 = 100;
        final int SAMPLE_SIZE_2 = 30;
        int TOTAL_SIZE = SAMPLE_SIZE_1 + SAMPLE_SIZE_2;

        for (int i = 0; i < SAMPLE_SIZE_1; i++) {
            map.put(i, new ValueType("type" + i));
        }

        for (int i = SAMPLE_SIZE_1; i < TOTAL_SIZE; i++) {
            map.put(i, new ValueType("typex"));
        }

        Collection typexValues = map.values(new SqlPredicate("typeName = typex"));
        assertEquals(SAMPLE_SIZE_2, typexValues.size());

        instances[1].shutdown();

        assertEquals(TOTAL_SIZE, map.size());
        assertTrueEventually(new AssertTask() {
            public void run() {
                final Collection values = map.values(new SqlPredicate("typeName = typex"));
                assertEquals(SAMPLE_SIZE_2, values.size());
            }
        });

        instances[2].shutdown();

        assertEquals(TOTAL_SIZE, map.size());
        assertTrueEventually(new AssertTask() {
            public void run() {
                final Collection values = map.values(new SqlPredicate("typeName = typex"));
                assertEquals(SAMPLE_SIZE_2, values.size());
            }
        });
    }


    /**
     * test for issue #359
     */
    @Test(timeout = 1000 * 60 * 10)
    public void testIndexCleanupOnMigration() throws InterruptedException {
        final int n = 6;
        final int runCount = 500;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(n);
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        final String mapName = "testIndexCleanupOnMigration";
        config.getMapConfig(mapName).addMapIndexConfig(new MapIndexConfig("name", false));
        ExecutorService ex = Executors.newFixedThreadPool(n);
        final CountDownLatch latch = new CountDownLatch(n);
        final AtomicInteger countdown = new AtomicInteger(n * runCount);
        final Random rand = new Random();
        for (int i = 0; i < n; i++) {
            Thread.sleep(rand.nextInt((i + 1) * 100) + 10);
            ex.execute(new Runnable() {
                public void run() {
                    final HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
                    final String name = UuidUtil.buildRandomUuidString();
                    final IMap<Object, Value> map = hz.getMap(mapName);
                    map.put(name, new Value(name, 0));
                    map.size();  // helper call on nodes to sync partitions.. see issue github.com/hazelcast/hazelcast/issues/1282
                    try {
                        for (int j = 1; j <= runCount; j++) {
                            Value v = map.get(name);
                            v.setIndex(j);
                            map.put(name, v);

                            try {
                                Thread.sleep(rand.nextInt(100) + 1);
                            } catch (InterruptedException e) {
                                break;
                            }
                            Value v1 = map.get(name);
                            assertEquals(v, v1);
                            EntryObject e = new PredicateBuilder().getEntryObject();
                            Predicate<?, ?> predicate = e.get("name").equal(name);
                            Collection<Value> values = map.values(predicate);
                            assertEquals(1, values.size());
                            Value v2 = values.iterator().next();
                            assertEquals(v1, v2);
                            countdown.decrementAndGet();
                        }
                    } catch (AssertionError e) {
                        e.printStackTrace();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            assertTrue(latch.await(10, TimeUnit.MINUTES));
            assertEquals(0, countdown.get());
        } finally {
            ex.shutdownNow();
        }
    }

    // issue 1404 "to be fixed by issue 1404"
    @Test(timeout = 1000 * 60)
    public void testQueryAfterInitialLoad() {
        String name = "testQueryAfterInitialLoad";
        Config cfg = new Config();
        final int size = 100;
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter() {
            @Override
            public Map loadAll(Collection keys) {
                Map map = new HashMap();
                for (Object key : keys) {
                    Employee emp = new Employee();
                    emp.setActive(true);
                    map.put(key, emp);
                }
                return map;
            }

            @Override
            public Set loadAllKeys() {
                Set set = new HashSet();
                for (int i = 0; i < size; i++) {
                    set.add(i);
                }
                return set;
            }
        });
        cfg.getMapConfig(name).setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance = createHazelcastInstance(cfg);
        final IMap map = instance.getMap(name);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection values = map.values(new SqlPredicate("active = true"));
                assertEquals(size, values.size());
            }
        });
    }

    /**
     * see zendesk ticket #82
     */
    @Test(timeout = 1000 * 60)
    public void testQueryWithIndexDuringJoin() throws InterruptedException {
        final String name = "test";
        final String FIND_ME = "find-me";
        final String DONT_FIND_ME = "dont-find-me";

        final int nodes = 5;
        final int entryPerNode = 1000;
        final int modulo = 10;
        final CountDownLatch latch = new CountDownLatch(nodes);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodes);

        final Config config = new Config();
        config.getMapConfig(name).addMapIndexConfig(new MapIndexConfig("name", false));

        for (int n = 0; n < nodes; n++) {
            new Thread() {
                public void run() {
                    HazelcastInstance hz = factory.newHazelcastInstance(config);
                    IMap<Object, Object> map = hz.getMap(name);

                    for (int i = 0; i < entryPerNode; i++) {
                        String id = UuidUtil.buildRandomUuidString();
                        String name;
                        if (i % modulo == 0) {
                            name = FIND_ME;
                        } else {
                            name = DONT_FIND_ME;
                        }
                        QueryValue d = new QueryValue(name, id);
                        map.put(id, d);
                    }
                    latch.countDown();
                }
            }.start();
        }

        Assert.assertTrue(latch.await(5, TimeUnit.MINUTES));
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        Assert.assertEquals(nodes, instances.size());

        final int expected = entryPerNode / modulo * nodes;

        for (HazelcastInstance hz : instances) {
            IMap<Object, Object> map = hz.getMap(name);
            EntryObject e = new PredicateBuilder().getEntryObject();
            Predicate p = e.get("name").equal(FIND_ME);
            for (int i = 0; i < 10; i++) {
                int size = map.values(p).size();
                Assert.assertEquals(expected, size);
                Thread.sleep(10);
            }
        }
    }

    private static class QueryValue implements Serializable {
        private String name;
        private String uuid;

        public QueryValue(String name, String uuid) {
            this.name = name;
            this.uuid = uuid;
        }

        public String getName() {
            return name;
        }

        public String getUuid() {
            return uuid;
        }
    }
}
