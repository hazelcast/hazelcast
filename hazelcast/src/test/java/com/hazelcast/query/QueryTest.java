/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.TestUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryTest extends TestUtil {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    HazelcastInstance newInstance() {
        return Hazelcast.newHazelcastInstance(null);
    }

    @Test
    public void testQueryWithTTL() throws Exception {
        Config cfg = new Config();
        Map<String, MapConfig> mapConfigs = new HashMap<String, MapConfig>();
        MapConfig mCfg = new MapConfig();
        int TTL = 2;
        mCfg.setTimeToLiveSeconds(TTL);
        mapConfigs.put("employees", mCfg);
        cfg.setMapConfigs(mapConfigs);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(cfg);
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        int expectedCount = 0;
        for (int i = 0; i < 1000; i++) {
            Employee employee = new Employee("joe" + i, i % 60, ((i & 1) == 1), Double.valueOf(i));
            if (employee.getName().startsWith("joe15") && employee.isActive()) {
                expectedCount++;
                System.out.println(employee);
            }
            imap.put(String.valueOf(i), employee);
        }
        Collection<Employee> values = imap.values(new SqlPredicate("active and name LIKE 'joe15%'"));
        for (Employee employee : values) {
//            System.out.println(employee);
            assertTrue(employee.isActive());
        }
        assertEquals(expectedCount, values.size());
        Thread.sleep((TTL + 1) * 1000);
        assertEquals(0, imap.size());
        values = imap.values(new SqlPredicate("active and name LIKE 'joe15%'"));
        assertEquals(0, values.size());
        Thread.sleep(5000);
        values = imap.values(new SqlPredicate("active and name LIKE 'joe15%'"));
        assertEquals(0, values.size());
    }

    @Test
    public void testQueryDuringAndAfterMigrationWithIndex() throws Exception {
        Config cfg = null;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(cfg);
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 10000; i++) {
            imap.put(String.valueOf(i), new Employee("joe" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(cfg);
        long startNow = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startNow) < 50000) {
            Collection<Employee> values = imap.values(new SqlPredicate("active and name LIKE 'joe15%'"));
            for (Employee employee : values) {
                assertTrue(employee.isActive());
            }
            assertEquals(56, values.size());
        }
    }


    @Test
    public void testQueryDuringAndAfterMigration() throws Exception {
        Config cfg = null;
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(cfg);
        int count = 100000;
        IMap imap = h1.getMap("values");
        for (int i = 0; i < count; i++) {
            imap.put(i, i);
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(cfg);
        long startNow = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startNow) < 50000) {
            Collection<Employee> values = imap.values();
            assertEquals(count, values.size());
        }
    }

    @Test
    public void testTwoNodesWithIndexes() throws Exception {
        HazelcastInstance h1 = newInstance();
        HazelcastInstance h2 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        imap = h2.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test
    public void testQueryWithIndexesWhileMigrating() throws Exception {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
//        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 500; i++) {
            Map temp = new HashMap(100);
            for (int j = 0; j < 100; j++) {
                String key = String.valueOf((i * 100000) + j);
                temp.put(key, new Employee("name" + key, i % 60, ((i & 1) == 1), Double.valueOf(i)));
            }
            imap.putAll(temp);
        }
        assertEquals(50000, imap.size());
//        HazelcastInstance h2 = newInstance();
//        HazelcastInstance h3 = newInstance();
//        HazelcastInstance h4 = newInstance();
        for (int i = 0; i < 1; i++) {
            Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active=true and age>44"));
            assertEquals(6400, entries.size());
        }
    }

    @Test
    public void testOneMemberWithoutIndex() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testOneMemberWithIndex() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testOneMemberSQLWithoutIndex() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalSQLQueryTest(imap);
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and age>23"));
        assertEquals(27, entries.size());
    }

    @Test
    public void testOneMemberSQLWithIndex() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalSQLQueryTest(imap);
    }

    @Test
    public void testIndexSQLPerformance() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        long start = System.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active=true and age=23"));
        long tookWithout = (System.currentTimeMillis() - start);
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.clear();
        imap = h1.getMap("employees2");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        start = System.currentTimeMillis();
        entries = imap.entrySet(new SqlPredicate("active and age=23"));
        long tookWithIndex = (System.currentTimeMillis() - start);
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        assertTrue(tookWithIndex < (tookWithout / 2));
    }

    @Test
    public void testRangeIndexSQLPerformance() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < 50000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        long start = System.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and salary between 4010.99 and 4032.01"));
        long tookWithout = (System.currentTimeMillis() - start);
        assertEquals(11, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertTrue(c.getAge() < 4033);
            assertTrue(c.isActive());
        }
        imap.clear();
        imap = h1.getMap("employees2");
        imap.addIndex("name", false);
        imap.addIndex("salary", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 50000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        imap.put(String.valueOf(10), new Employee("name" + 10, 10, true, 44010.99D));
        imap.put(String.valueOf(11), new Employee("name" + 11, 11, true, 44032.01D));
        start = System.currentTimeMillis();
        entries = imap.entrySet(new SqlPredicate("active and salary between 44010.99 and 44032.01"));
        long tookWithIndex = (System.currentTimeMillis() - start);
        assertEquals(13, entries.size());
        boolean foundFirst = false;
        boolean foundLast = false;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
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
        System.out.println(tookWithIndex + " vs. " + tookWithout);
        assertTrue(tookWithIndex < (tookWithout / 2));
        for (int i = 0; i < 50000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), 100.25D));
        }
        entries = imap.entrySet(new SqlPredicate("salary between 99.99 and 100.25"));
        assertEquals(50000, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertTrue(c.getSalary() == 100.25D);
        }
    }

    @Test
    public void testIndexPerformance() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        long start = System.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(predicate);
        long tookWithout = (System.currentTimeMillis() - start);
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.clear();
        imap = h1.getMap("employees2");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        e = new PredicateBuilder().getEntryObject();
        predicate = e.is("active").and(e.get("age").equal(23));
        start = System.currentTimeMillis();
        entries = imap.entrySet(predicate);
        long tookWithIndex = (System.currentTimeMillis() - start);
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        assertTrue(tookWithIndex < (tookWithout / 2));
    }

    @Test
    public void testIndexPerformanceUsingPredicate() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        long start = System.currentTimeMillis();
        Set<Map.Entry> entries = imap.entrySet(predicate);
        long tookWithout = (System.currentTimeMillis() - start);
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.clear();
        imap = h1.getMap("employees2");
        imap.addIndex(Predicates.get("name"), false);
        imap.addIndex(Predicates.get("active"), false);
        imap.addIndex(Predicates.get("age"), true);
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        e = new PredicateBuilder().getEntryObject();
        predicate = e.is("active").and(e.get("age").equal(23));
        start = System.currentTimeMillis();
        entries = imap.entrySet(predicate);
        long tookWithIndex = (System.currentTimeMillis() - start);
        assertEquals(83, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        assertTrue(tookWithIndex < (tookWithout / 2));
    }

    @Test
    public void testTwoMembers() {
        HazelcastInstance h1 = newInstance();
        HazelcastInstance h2 = newInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testTwoMembersWithIndexes() {
        HazelcastInstance h1 = newInstance();
        HazelcastInstance h2 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown() {
        HazelcastInstance h1 = newInstance();
        HazelcastInstance h2 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
        assertEquals(101, imap.size());
        h2.shutdown();
        assertEquals(101, imap.size());
        Set<Map.Entry> entries = imap.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown2() {
        HazelcastInstance h1 = newInstance();
        HazelcastInstance h2 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
        assertEquals(101, imap.size());
        h1.shutdown();
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

    @Test
    public void testTwoMembersWithIndexesAndShutdown3() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
        assertEquals(101, imap.size());
        HazelcastInstance h2 = newInstance();
        assertEquals(101, imap.size());
        h1.shutdown();
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

    @Test
    public void testSecondMemberAfterAddingIndexes() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        HazelcastInstance h2 = newInstance();
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testWithDashInTheNameAndSqlPredicate() {
        IMap<String, Employee> map = Hazelcast.getMap("employee");
        Employee toto = new Employee("toto", 23, true, 165765.0);
        System.out.println("put " + toto);
        map.put("1", toto);
        Employee toto2 = new Employee("toto-super+hero", 23, true, 165765.0);
        map.put("2", toto2);
        //Works well
        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("name='toto-super+hero'"));
        System.out.println("Set entries = " + entries);
        assertTrue(entries.size() > 0);
        for (Map.Entry<String, Employee> entry : entries) {
            Employee e = entry.getValue();
            System.out.println(e);
            assertEquals(e, toto2);
        }
    }

    public void doFunctionalSQLQueryTest(IMap imap) {
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);
        entries = imap.entrySet(new SqlPredicate("active=true and age=23"));
        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.remove("2");
        entries = imap.entrySet(new SqlPredicate("active=true and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        entries = imap.entrySet(new SqlPredicate("age!=33"));
        for(Map.Entry entry : entries) {
        	Employee c = (Employee) entry.getValue();
        	assertTrue(c.getAge() != 33);
        }
        entries = imap.entrySet(new SqlPredicate("active!=false"));
        for (Map.Entry entry : entries) {
        	Employee c = (Employee) entry.getValue();
        	assertTrue(c.isActive());
        }
    }

    public void doFunctionalQueryTest(IMap imap) {
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        entries = imap.entrySet(predicate);
//        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.remove("2");
        entries = imap.entrySet(predicate);
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        entries = imap.entrySet(new SqlPredicate(" (age >= " + 30 + ") AND (age <= " + 40 + ")"));
        assertEquals(23, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            System.out.println(c);
            assertTrue(c.getAge() >= 30);
            assertTrue(c.getAge() <= 40);
        }
    }
}
