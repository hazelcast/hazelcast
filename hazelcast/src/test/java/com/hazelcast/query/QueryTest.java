/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;


public class QueryTest {
    List<HazelcastInstance> lsInstances = new CopyOnWriteArrayList();

    @After
    public void cleanUp() {
        for (HazelcastInstance h : lsInstances) {
            h.shutdown();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
        lsInstances.clear();
    }

    HazelcastInstance newInstance() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        lsInstances.add(h);
        return h;
    }

    @Test
    public void testOneMemberWithoutIndex() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testIndexPerformance() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i % 2) == 1), Double.valueOf(i)));
        }

        EntryObject e = new PredicateBuilder().getRoot();
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
        imap.addIndex(Predicates.get("age"), true);
        imap.addIndex(Predicates.get("active"), false);

        for (int i = 0; i < 5000; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i % 2) == 1), Double.valueOf(i)));
        }

        e = new PredicateBuilder().getRoot();
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
    public void testOneMemberWithIndex() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex(Predicates.get("name"), false);
        imap.addIndex(Predicates.get("age"), true);
        imap.addIndex(Predicates.get("active"), false);
        doFunctionalQueryTest(imap);
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
        imap.addIndex(Predicates.get("name"), false);
        imap.addIndex(Predicates.get("age"), true);
        imap.addIndex(Predicates.get("active"), false);
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testSecondMemberAfterAddingIndexes() {
        HazelcastInstance h1 = newInstance();
        IMap imap = h1.getMap("employees");
        imap.addIndex(Predicates.get("name"), false);
        imap.addIndex(Predicates.get("age"), true);
        imap.addIndex(Predicates.get("active"), false);
        HazelcastInstance h2 = newInstance();
        doFunctionalQueryTest(imap);
    }

    public void doFunctionalQueryTest(IMap imap) {
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i % 2) == 1), Double.valueOf(i)));
        }

        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);

        EntryObject e = new PredicateBuilder().getRoot();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));

        entries = imap.entrySet(predicate);
        assertEquals(3, entries.size());
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
    }

    public static class Employee implements Serializable {
        String name;
        int age;
        boolean active;
        double salary;

        public Employee(String name, int age, boolean live, double price) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = price;
        }

        public Employee() {
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        public double getSalary() {
            return salary;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("Employee");
            sb.append("{name='").append(name).append('\'');
            sb.append(", age=").append(age);
            sb.append(", active=").append(active);
            sb.append(", salary=").append(salary);
            sb.append('}');
            return sb.toString();
        }
    }
}
