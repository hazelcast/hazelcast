/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.sql.support.model.person.Person;
import com.hazelcast.sql.support.model.person.PersonKey;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * Tests for aggregates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregateSqlTest extends SqlTestSupport {
    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);

        member = factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        ModelGenerator.generatePerson(member);
    }

    @AfterClass
    public static void afterClass() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void testSimpleAggregate() {
        Map<PersonKey, Person> personMap = member.getMap("person");

        PersonSalaryCollector collector = new PersonSalaryCollector();

        for (Person person : personMap.values()) {
            collector.add(person.getSalary());
        }

        SqlCursor cursor = executeQuery(
            member,
            "SELECT SUM(salary), COUNT(salary), AVG(salary), MIN(salary), MAX(salary) FROM person"
        );

        assertEquals(5, cursor.getColumnCount());

        List<SqlRow> rows = getQueryRows(cursor);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);

        assertEquals(collector.getSum(), row.getObject(0));
        assertEquals(collector.getCnt(), row.getObject(1));
        assertEquals(collector.getAvg(), row.getObject(2));
        assertEquals(collector.getMin(), row.getObject(3));
        assertEquals(collector.getMax(), row.getObject(4));
    }

    @Test public void testSimpleAggregateWithGroupByCollocated() {
        Map<PersonKey, Person> personMap = member.getMap("person");

        Map<Long, PersonSalaryCollector> collectors = new HashMap<>();

        for (Map.Entry<PersonKey, Person> entry : personMap.entrySet()) {
            long deptId = entry.getKey().getDeptId();
            long salary = entry.getValue().getSalary();

            collectors.computeIfAbsent(deptId, (k) -> new PersonSalaryCollector()).add(salary);
        }

        SqlCursor cursor = executeQuery(
            member,
            "SELECT deptId, SUM(salary), COUNT(salary), AVG(salary), MIN(salary), MAX(salary) FROM person GROUP BY deptId"
        );

        assertEquals(6, cursor.getColumnCount());

        List<SqlRow> rows = getQueryRows(cursor);
        assertEquals(collectors.size(), rows.size());

        for (SqlRow row : rows) {
            long deptId = row.getObject(0);
            PersonSalaryCollector collector = collectors.get(deptId);
            assertNotNull(collector);

            assertEquals(collector.getSum(), row.getObject(1));
            assertEquals(collector.getCnt(), row.getObject(2));
            assertEquals(collector.getAvg(), row.getObject(3));
            assertEquals(collector.getMin(), row.getObject(4));
            assertEquals(collector.getMax(), row.getObject(5));
        }
    }

    @Test
    public void testSimpleAggregateWithGroupByNonCollocated() {
        Map<PersonKey, Person> personMap = member.getMap("person");

        Map<String, PersonSalaryCollector> collectors = new HashMap<>();

        for (Person person : personMap.values()) {
            String deptTitle = person.getDeptTitle();
            long salary = person.getSalary();

            collectors.computeIfAbsent(deptTitle, (k) -> new PersonSalaryCollector()).add(salary);
        }

        SqlCursor cursor = executeQuery(
            member,
            "SELECT deptTitle, SUM(salary), COUNT(salary), AVG(salary), MIN(salary), MAX(salary) FROM person GROUP BY deptTitle"
        );

        assertEquals(6, cursor.getColumnCount());

        List<SqlRow> rows = getQueryRows(cursor);
        assertEquals(collectors.size(), rows.size());

        for (SqlRow row : rows) {
            String deptTitle = row.getObject(0);
            PersonSalaryCollector collector = collectors.get(deptTitle);
            assertNotNull(collector);

            assertEquals(collector.getSum(), row.getObject(1));
            assertEquals(collector.getCnt(), row.getObject(2));
            assertEquals(collector.getAvg(), row.getObject(3));
            assertEquals(collector.getMin(), row.getObject(4));
            assertEquals(collector.getMax(), row.getObject(5));
        }
    }

    /**
     * Convenient collector for results.
     */
    private static class PersonSalaryCollector {
        private long sum;
        private long cnt;
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;

        public void add(long salary) {
            sum += salary;
            cnt++;
            min = Math.min(salary, min);
            max = Math.max(salary, max);
        }

        public Long getSum() {
            return cnt == 0 ? null : sum;
        }

        public Long getCnt() {
            return cnt;
        }

        public Long getMin() {
            return cnt == 0 ? null : min;
        }

        public Long getMax() {
            return cnt == 0 ? null : max;
        }

        public double getAvg() {
            return cnt == 0 ? 0 : (double) sum / cnt;
        }
    }
}
