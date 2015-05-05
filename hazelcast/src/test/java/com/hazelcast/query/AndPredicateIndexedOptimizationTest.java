/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.query.Predicates.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests added to make sure no regression has been introduced by the ad-hoc optimization
 * of AndPredicate at {@link AndPredicate#tryOptimized(QueryContext)}
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AndPredicateIndexedOptimizationTest extends HazelcastTestSupport {
    private static final double  SALARY = 1000;
    private static final boolean LIVE   = true;
    private static final String  NAME   = "name";

    private static final int RECORD_COUNT = 20;

    @Test
    public void testOptimization_naturalOrder() {
        //The Simplest Case: age >= 0 AND age <= 19
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);

        Predicate from = greaterEqual("age", 0);
        Predicate to = lessEqual("age", RECORD_COUNT - 1);
        Predicate andPredicate = and(from, to);

        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    @Test
    public void testOptimization_reversedOrder() {
        //Case: age <= 19 AND age >= 0
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);

        Predicate from = greaterEqual("age", 0);
        Predicate to = lessEqual("age", RECORD_COUNT - 1);
        Predicate andPredicate = and(to, from);

        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    @Test
    public void testCrossedPredicates() {
        //Case: age >= 10 AND age <= 5
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);

        Predicate from = greaterEqual("age", 10);
        Predicate to = lessEqual("age", 5);
        Predicate andPredicate = and(from, to);

        Collection<Employee> values = map.values(andPredicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testWrongNumberOfPredicates() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);

        Predicate to = lessEqual("age", RECORD_COUNT - 1);
        Predicate andPredicate = and(to);

        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    @Test
    public void testNotAllArgumentsAreInstancesOfGreaterLessPredicate() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);

        Predicate to = lessEqual("age", RECORD_COUNT - 1);
        Predicate pred2 = equal("active", true);
        Predicate andPredicate = and(to, pred2);

        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    @Test
    public void testDifferentAttributes() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);

        Predicate to1 = lessEqual("age", RECORD_COUNT - 1);
        Predicate to2 = lessEqual("salary", SALARY);
        Predicate andPredicate = and(to1, to2);

        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    @Test
    public void testFirstArgumentNotInclusive() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);
        Predicate from = greaterThan("age", 0);
        Predicate to = lessEqual("age", RECORD_COUNT - 1);
        Predicate andPredicate = and(from, to);
        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT - 1, values.size());
    }

    @Test
    public void testSecondArgumentNotInclusive() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);
        Predicate from = greaterEqual("age", 0);
        Predicate to = lessThan("age", RECORD_COUNT - 1);
        Predicate andPredicate = and(from, to);
        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT - 1, values.size());
    }

    @Test
    public void testBothArgumentAre_LessEquals() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);
        Predicate to1 = lessEqual("age", RECORD_COUNT - 1);
        Predicate to2 = lessEqual("age", RECORD_COUNT - 1);
        Predicate andPredicate = and(to1, to2);
        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    @Test
    public void testBothArgumentAre_GreaterEquals() {
        IMap<String, Employee> map = prepareMap(RECORD_COUNT);
        Predicate to1 = greaterEqual("age", 0);
        Predicate to2 = greaterEqual("age", 0);
        Predicate andPredicate = and(to1, to2);
        Collection<Employee> values = map.values(andPredicate);
        assertEquals(RECORD_COUNT, values.size());
    }

    private IMap<String, Employee> prepareMap(int recordCount) {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<String, Employee> map = instance.getMap("map");
        map.addIndex("age", true);
        map.addIndex("name", true);
        map.addIndex("active", false);
        loadInitialData(map, recordCount);
        return map;
    }

    private void loadInitialData(IMap<String, Employee> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put("" + i, new Employee(i, NAME, i, LIVE, SALARY));
        }
    }
}
