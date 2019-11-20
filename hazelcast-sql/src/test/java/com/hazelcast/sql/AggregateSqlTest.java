/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.SqlCursorImpl;
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

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

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
        Map<PersonKey, Person> personMap = member.<PersonKey, Person>getMap("person");

        long sum = 0;
        long cnt = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (Person person : personMap.values()) {
            long salary = person.getSalary();

            sum += salary;
            cnt += 1;
            min = Math.min(salary, min);
            max = Math.max(salary, max);
        }

        double avg = (double)sum / cnt;

        SqlCursorImpl cursor = executeQuery(
            member,
            "SELECT SUM(salary), COUNT(salary), AVG(salary), MIN(salary), MAX(salary) FROM person"
        );

        List<SqlRow> rows = getQueryRows(cursor);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(5, row.getColumnCount());

        assertEquals((Long)sum, row.getColumn(0));
        assertEquals((Long)cnt, row.getColumn(1));
        assertEquals(avg, row.getColumn(2));
        assertEquals((Long)min, row.getColumn(3));
        assertEquals((Long)max, row.getColumn(4));
    }
}
