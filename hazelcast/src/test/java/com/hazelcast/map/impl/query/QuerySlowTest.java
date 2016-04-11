package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class QuerySlowTest extends HazelcastTestSupport {

    @Test(timeout = MINUTE)
    public void testIndexPerformanceUsingPredicate() {
        HazelcastInstance instance = createHazelcastInstance(newConfig());
        IMap<String, SampleObjects.Employee> map = instance.getMap("employees");
        fillMap(map);

        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.is("active").and(entryObject.get("age").equal(23));
        long tookWithout = runQuery(map, predicate);
        map.clear();

        map = instance.getMap("employees2");
        map.addIndex("name", false);
        map.addIndex("active", false);
        map.addIndex("age", true);

        fillMap(map);
        waitAllForSafeState(instance);
        long tookWithIndex = runQuery(map, predicate);

        assertTrue("withIndex: " + tookWithIndex + ", without: " + tookWithout, tookWithIndex < (tookWithout / 2));
    }

    private static Config newConfig() {
        Config conf = new Config();
        conf.getMapConfig("default").setBackupCount(0);
        // disable replication so that indexes are used for queries
        conf.setProperty(PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        return conf;
    }

    private long runQuery(IMap<String, SampleObjects.Employee> imap, Predicate predicate) {
        long start = Clock.currentTimeMillis();
        Set<Map.Entry<String, SampleObjects.Employee>> entries = imap.entrySet(predicate);
        long tookWithout = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        return tookWithout;
    }

    private static void assertEntriesMatch(Set<Map.Entry<String, SampleObjects.Employee>> entries) {
        assertEquals(83, entries.size());
        for (Map.Entry<String, SampleObjects.Employee> entry : entries) {
            SampleObjects.Employee employee = entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
    }

    private static void fillMap(IMap<String, SampleObjects.Employee> map) {
        for (int i = 0; i < 5000; i++) {
            map.put(String.valueOf(i), new SampleObjects.Employee("name" + i, i % 60, ((i & 1) == 1), i));
        }
    }

    @Test(timeout = 1000 * 60)
    public void testIndexSQLPerformance() {
        Config cfg = new Config();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, SampleObjects.Employee> map = instance.getMap("employees");
        fillMap(map);

        long start = Clock.currentTimeMillis();
        Set<Map.Entry<String, SampleObjects.Employee>> entries = map.entrySet(new SqlPredicate("active=true and age=23"));
        long tookWithout = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        map.clear();
        map = instance.getMap("employees2");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);
        fillMap(map);
        start = Clock.currentTimeMillis();
        entries = map.entrySet(new SqlPredicate("active and age=23"));
        long tookWithIndex = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        assertTrue(tookWithIndex < (tookWithout / 2));
    }

    @Test(timeout = 1000 * 60 * 2)
    public void testRangeIndexSQLPerformance() {
        Config cfg = new Config();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, SampleObjects.Employee> map = instance.getMap("employees");
        fillMap(map);

        long start = Clock.currentTimeMillis();
        Set<Map.Entry<String, SampleObjects.Employee>> entries = map.entrySet(
                new SqlPredicate("active and salary between 4010.99 and 4032.01"));
        long tookWithout = (Clock.currentTimeMillis() - start);
        assertEquals(11, entries.size());
        for (Map.Entry entry : entries) {
            SampleObjects.Employee c = (SampleObjects.Employee) entry.getValue();
            assertTrue(c.getAge() < 4033);
            assertTrue(c.isActive());
        }
        map.clear();
        map = instance.getMap("employees2");
        map.addIndex("name", false);
        map.addIndex("salary", false);
        map.addIndex("active", false);
        fillMap(map);
        map.put(String.valueOf(10), new SampleObjects.Employee("name" + 10, 10, true, 44010.99D));
        map.put(String.valueOf(11), new SampleObjects.Employee("name" + 11, 11, true, 44032.01D));
        start = Clock.currentTimeMillis();
        entries = map.entrySet(new SqlPredicate("active and salary between 44010.99 and 44032.01"));
        long tookWithIndex = (Clock.currentTimeMillis() - start);
        assertEquals(2, entries.size());
        boolean foundFirst = false;
        boolean foundLast = false;
        for (Map.Entry entry : entries) {
            SampleObjects.Employee c = (SampleObjects.Employee) entry.getValue();
            assertTrue(c.getAge() < 44033);
            assertTrue(c.isActive());
            if (c.getSalary() == 44010.99D) {
                foundFirst = true;
            } else if (c.getSalary() == 44032.01D) {
                foundLast = true;
            }
        }
        assertTrue(foundFirst);
        assertTrue(foundLast);
        assertTrue(tookWithIndex < (tookWithout / 2));
        for (int i = 0; i < 50000; i++) {
            map.put(String.valueOf(i), new SampleObjects.Employee("name" + i, i % 60, ((i & 1) == 1), 100.25D));
        }
        entries = map.entrySet(new SqlPredicate("salary between 99.99 and 100.25"));
        assertEquals(50000, entries.size());
        for (Map.Entry entry : entries) {
            SampleObjects.Employee c = (SampleObjects.Employee) entry.getValue();
            assertTrue(c.getSalary() == 100.25D);
        }
    }

    @Test(timeout = 1000 * 60)
    public void testIndexPerformance() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig("employees2");
        mapConfig.addMapIndexConfig(new MapIndexConfig("name", false))
                .addMapIndexConfig(new MapIndexConfig("age", true))
                .addMapIndexConfig(new MapIndexConfig("active", false));

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<String, SampleObjects.Employee> map = instance.getMap("employees");
        fillMap(map);

        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.is("active").and(entryObject.get("age").equal(23));
        long tookWithout = runQuery(map, predicate);
        map.clear();

        map = instance.getMap("employees2");
        fillMap(map);
        entryObject = new PredicateBuilder().getEntryObject();
        predicate = entryObject.is("active").and(entryObject.get("age").equal(23));

        long start = Clock.currentTimeMillis();
        Set<Map.Entry<String, SampleObjects.Employee>> entries = map.entrySet(predicate);
        long tookWithIndex = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        assertTrue("tookWithIndex: " + tookWithIndex + ", tookWithoutIndex: " + tookWithout, tookWithIndex < (tookWithout / 2));
    }
}
