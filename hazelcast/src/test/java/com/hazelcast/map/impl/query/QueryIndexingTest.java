/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryIndexingTest extends HazelcastTestSupport {

    private int count = 2000;
    private Map<Integer, Employee> employees;

    private TestHazelcastInstanceFactory nodeFactory;
    private HazelcastInstance h1;
    private HazelcastInstance h2;

    private Predicate predicate;

    @Before
    public void setUp() {
        employees = newEmployees(count);

        nodeFactory = createHazelcastInstanceFactory(2);
        Config config = newConfig();
        h1 = nodeFactory.newHazelcastInstance(config);
        h2 = nodeFactory.newHazelcastInstance(config);

        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        predicate = entryObject.get("name").equal(null).and(entryObject.get("city").isNull());

        assertClusterSizeEventually(2, h1);
    }

    @After
    public void tearDown() {
        nodeFactory.shutdownAll();
    }

    @Test
    public void testResultsHaveNullFields_whenPredicateTestsForNull() {
        IMap<Integer, Employee> map = h1.getMap("employees");
        map.putAll(employees);
        waitAllForSafeState(h1, h2);

        Collection<Employee> matchingEntries = runQueryNTimes(3, h2.<String, Employee>getMap("employees"));

        assertEquals(count / 2, matchingEntries.size());
        // N queries result in getters called N times
        assertGettersCalledNTimes(matchingEntries, 3);
        assertFieldsAreNull(matchingEntries);
    }

    @Test
    public void testResultsHaveNullFields_whenUsingIndexes() {
        IMap<Integer, Employee> map = h1.getMap("employees");

        map.addIndex("name", false);
        map.addIndex("city", true);

        map.putAll(employees);
        waitAllForSafeState(h1, h2);

        Collection<Employee> matchingEntries = runQueryNTimes(3, h2.<String, Employee>getMap("employees"));
        assertEquals(count / 2, matchingEntries.size());

        assertFieldsAreNull(matchingEntries);
    }

    private static Map<Integer, Employee> newEmployees(int employeeCount) {
        Map<Integer, Employee> employees = new HashMap<Integer, Employee>();
        for (int i = 0; i < employeeCount; i++) {
            Employee val;
            if (i % 2 == 0) {
                val = new Employee(i, null, null, 0, true, i);
            } else {
                val = new Employee(i, "name" + i, "city" + i, 0, true, i);
            }
            Employee spy = MockUtil.serializableSpy(Employee.class, val);
            employees.put(i, spy);
        }
        return employees;
    }

    private static Config newConfig() {
        Config conf = new Config();
        conf.getMapConfig("employees").setInMemoryFormat(InMemoryFormat.OBJECT).setBackupCount(0);
        // disabling replication since we don't use backups in this test
        conf.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        return conf;
    }

    private Collection<Employee> runQueryNTimes(int queryCount, IMap<String, Employee> map) {
        Collection<Employee> result = emptyList();
        for (int i = 0; i < queryCount; i++) {
            result = map.values(predicate);
        }
        return result;
    }

    private static void assertGettersCalledNTimes(Collection<Employee> matchingEmployees, int expectedCalls) {
        for (Employee employee : matchingEmployees) {
            verify(employee, times(expectedCalls)).getCity();
            verify(employee, times(expectedCalls)).getName();
        }
    }

    private static void assertFieldsAreNull(Collection<Employee> matchingEmployees) {
        for (Employee employee : matchingEmployees) {
            assertNull("city", employee.getCity());
            assertNull("name", employee.getName());
        }
    }
}
