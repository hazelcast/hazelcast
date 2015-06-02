package com.hazelcast.map.query;

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

import java.util.Map;
import java.util.Set;

import static com.hazelcast.instance.GroupProperties.PROP_PARTITION_MAX_PARALLEL_REPLICATIONS;
import static com.hazelcast.test.TimeConstants.MINUTE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class QuerySlowTest extends HazelcastTestSupport {

    @Test(timeout = MINUTE)
    public void testIndexPerformanceUsingPredicate() {
        HazelcastInstance h1 = createHazelcastInstance(newConfig());
        IMap imap = h1.getMap("employees");
        fillMap(imap);

        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        long tookWithout = runQuery(imap, predicate);
        imap.clear();

        imap = h1.getMap("employees2");
        imap.addIndex("name", false);
        imap.addIndex("active", false);
        imap.addIndex("age", true);

        fillMap(imap);
        waitAllForSafeState(h1);
        long tookWithIndex = runQuery(imap, predicate);

        assertTrue("withIndex: " + tookWithIndex + ", without: " + tookWithout, tookWithIndex < (tookWithout / 2));
    }

    private static Config newConfig() {
        Config conf = new Config();
        conf.getMapConfig("default").setBackupCount(0);
        // disable replication so that indexes are used for queries
        conf.setProperty(PROP_PARTITION_MAX_PARALLEL_REPLICATIONS, "0");
        return conf;
    }

    private long runQuery(IMap imap, Predicate predicate) {
        long start = Clock.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(predicate);
        long tookWithout = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        return tookWithout;
    }

    private static void assertEntriesMatch(Set<Map.Entry> entries) {
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            SampleObjects.Employee c = (SampleObjects.Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }

    private static void fillMap(IMap imap) {
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new SampleObjects.Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
    }

    @Test(timeout=1000*60)
    public void testIndexSQLPerformance() {
        Config cfg = new Config();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        IMap imap = h1.getMap("employees");
        fillMap(imap);
        long start = Clock.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active=true and age=23"));
        long tookWithout = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        imap.clear();
        imap = h1.getMap("employees2");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        fillMap(imap);
        start = Clock.currentTimeMillis();
        entries = imap.entrySet(new SqlPredicate("active and age=23"));
        long tookWithIndex = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        assertTrue(tookWithIndex < (tookWithout / 2));
    }

    @Test(timeout=1000*60)
    public void testRangeIndexSQLPerformance() {
        Config cfg = new Config();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        IMap imap = h1.getMap("employees");
        fillMap(imap);
        long start = Clock.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and salary between 4010.99 and 4032.01"));
        long tookWithout = (Clock.currentTimeMillis() - start);
        assertEquals(11, entries.size());
        for (Map.Entry entry : entries) {
            SampleObjects.Employee c = (SampleObjects.Employee) entry.getValue();
            assertTrue(c.getAge() < 4033);
            assertTrue(c.isActive());
        }
        imap.clear();
        imap = h1.getMap("employees2");
        imap.addIndex("name", false);
        imap.addIndex("salary", false);
        imap.addIndex("active", false);
        fillMap(imap);
        imap.put(String.valueOf(10), new SampleObjects.Employee("name" + 10, 10, true, 44010.99D));
        imap.put(String.valueOf(11), new SampleObjects.Employee("name" + 11, 11, true, 44032.01D));
        start = Clock.currentTimeMillis();
        entries = imap.entrySet(new SqlPredicate("active and salary between 44010.99 and 44032.01"));
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
            imap.put(String.valueOf(i), new SampleObjects.Employee("name" + i, i % 60, ((i & 1) == 1), 100.25D));
        }
        entries = imap.entrySet(new SqlPredicate("salary between 99.99 and 100.25"));
        assertEquals(50000, entries.size());
        for (Map.Entry entry : entries) {
            SampleObjects.Employee c = (SampleObjects.Employee) entry.getValue();
            assertTrue(c.getSalary() == 100.25D);
        }
    }

    @Test(timeout=1000*60)
    public void testIndexPerformance() {
        Config cfg = new Config();
        final MapConfig mapConfig = cfg.getMapConfig("employees2");
        mapConfig.addMapIndexConfig(new MapIndexConfig("name", false))
                .addMapIndexConfig(new MapIndexConfig("age", true))
                .addMapIndexConfig(new MapIndexConfig("active", false));

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(4);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        IMap imap = h1.getMap("employees");
        fillMap(imap);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        long start;
        Set<Map.Entry> entries;
        long tookWithout = runQuery(imap, predicate);
        imap.clear();

        imap = h1.getMap("employees2");
        fillMap(imap);
        e = new PredicateBuilder().getEntryObject();
        predicate = e.is("active").and(e.get("age").equal(23));
        start = Clock.currentTimeMillis();
        entries = imap.entrySet(predicate);
        long tookWithIndex = (Clock.currentTimeMillis() - start);
        assertEntriesMatch(entries);
        assertTrue("tookWithIndex: " + tookWithIndex + ", tookWithoutIndex: " + tookWithout,  tookWithIndex < (tookWithout / 2));
    }
}
