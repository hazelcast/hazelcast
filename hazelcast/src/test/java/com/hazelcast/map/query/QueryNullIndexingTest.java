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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueryNullIndexingTest extends HazelcastTestSupport {

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(false,
                                                 Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessEqualPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(false,
                                                 Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(false,
                                                 Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterEqualPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(false,
                                                 Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithNotEqualPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(false,
                                                 Predicates.notEqual("date", 2000000L));
        assertEquals(4, dates.size());
        assertTrue(dates.containsAll(
                Arrays.asList(4000000L, 6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(true,
                                                 Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessEqualPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(true,
                                                 Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(true,
                                                 Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterEqualPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(true,
                                                 Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithNotEqualPredicate() {
        final List<Long> dates =
                queryIndexedDateFieldAsNullValue(true,
                                                 Predicates.notEqual("date", 2000000L));
        assertEquals(4, dates.size());
        assertTrue(dates.containsAll(
                Arrays.asList(4000000L, 6000000L, 8000000L, 10000000L)));
    }

    private List<Long> queryIndexedDateFieldAsNullValue(boolean ordered, Predicate pred) {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Integer, SampleObjects.Employee> map = instance.getMap("default");

        map.addIndex("date", ordered);
        for (int i = 10; i >= 1; i--) {
            Employee employee = new Employee(i, "name-" + i, i, true, i * 100);
            if (i % 2 == 0) {
                employee.setDate(new Timestamp(i * 1000000));
            } else {
                employee.setDate(null);
            }
            map.put(i, employee);
        }

        final List<Long> dates = new ArrayList<Long>();
        for (SampleObjects.Employee employee : map.values(pred)) {
            dates.add(employee.getDate().getTime());
        }

        return dates;
    }

}
