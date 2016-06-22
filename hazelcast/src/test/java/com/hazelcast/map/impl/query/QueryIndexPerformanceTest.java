package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryIndexPerformanceTest extends HazelcastTestSupport {

    private IMap<String, Employee> mapWithoutIndex;
    private IMap<String, Employee> mapWithIndex;

    @Before
    public void setup() {
        HazelcastInstance instance = createHazelcastInstance();

        mapWithoutIndex = instance.getMap("employees");
        fillMap(mapWithoutIndex);

        mapWithIndex = instance.getMap("employees2");
        mapWithIndex.addIndex("name", false);
        mapWithIndex.addIndex("active", false);
        mapWithIndex.addIndex("salary", true);
        mapWithIndex.addIndex("age", true);
        fillMap(mapWithIndex);
    }

    @Test
    public void testIndexPerformance() {
        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.is("active").and(entryObject.get("age").equal(23));
        testIndexPerformance(predicate);
    }

    @Test
    public void testRangeIndexPerformance() {
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active")
                .and(e.get("salary").between(2000.22, 4000.44))
                .and(e.get("age").between(10, 90));
        testIndexPerformance(predicate);
    }

    private void testIndexPerformance(Predicate predicate) {
        long tookWithoutIndex = runQuery(mapWithoutIndex, predicate);
        long tookWithIndex = runQuery(mapWithIndex, predicate);

        assertTrue("withIndex: " + tookWithIndex + " nanos, withoutIndex: " + tookWithoutIndex + " nanos",
                tookWithIndex < (tookWithoutIndex / 2));
    }

    private static long runQuery(IMap<String, Employee> map, Predicate predicate) {
        long start = System.nanoTime();
        Collection<Employee> values = map.values(predicate);
        long end = System.nanoTime();
        assertNotNull(values);
        assertFalse(values.isEmpty());
        return (end - start);
    }

    private static void fillMap(IMap<String, Employee> map) {
        for (int i = 0; i < 10000; i++) {
            boolean active = i % 10 != 0;
            int age = i % 100;
            double salary = 1000 + i + Math.random();
            map.put(String.valueOf(i), new Employee("name" + i, age, active, salary));
        }
    }
}
