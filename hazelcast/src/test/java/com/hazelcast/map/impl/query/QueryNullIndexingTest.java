/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryNullIndexingTest extends HazelcastTestSupport {

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithNotEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.notEqual("date", 2000000L));
        assertEquals(9, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(4000000L, 6000000L, 8000000L, 10000000L, null)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(2000000L, 4000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(6000000L, 8000000L, 10000000L)));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithNotEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.notEqual("date", 2000000L));
        assertEquals(9, dates.size());
        assertTrue(dates.containsAll(Arrays.asList(4000000L, 6000000L, 8000000L, 10000000L, null)));
    }

    private List<Long> queryIndexedDateFieldAsNullValue(boolean ordered, Predicate pred) {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, SampleObjects.Employee> map = instance.getMap("default");

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

        List<Long> dates = new ArrayList<Long>();
        for (SampleObjects.Employee employee : map.values(pred)) {
            Timestamp date = employee.getDate();
            dates.add(date == null ? null : date.getTime());
        }

        return dates;
    }
}
