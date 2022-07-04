/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexStatsQueryingInCollectionsAndArraysTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    private static final int PARTITIONS = 137;
    private static final int EMPLOYEE_ARRAY_SIZE = 10;

    private String mapName;
    private HazelcastInstance instance;
    private IMap<Integer, Department> map;

    @Before
    public void before() {
        mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), Integer.toString(PARTITIONS));
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);

        instance = createHazelcastInstance(config);
        map = instance.getMap(mapName);
    }

    @Test
    public void testHitAndQueryCounting_WhenAllIndexesHit() {
        addIndex(map, "employees[0].id", true);
        addIndex(map, "employees[any].id", true);

        assertEquals(0, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());

        for (int i = 0; i < 10; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j);
            }
            map.put(i, new Department(persons));
        }

        for (int i = 0; i < 100; i++) {
            map.entrySet(Predicates.alwaysTrue());
            map.entrySet(Predicates.equal("employees[0].id", "0"));
            map.entrySet(Predicates.equal("employees[any].id", "5"));
            map.entrySet(Predicates.lessEqual("employees[0].id", "1"));
            map.entrySet(Predicates.lessEqual("employees[any].id", "6"));
        }

        assertEquals(500, stats().getQueryCount());
        assertEquals(400, stats().getIndexedQueryCount());
        assertEquals(200, valueStats("employees[0].id").getHitCount());
        assertEquals(200, valueStats("employees[0].id").getQueryCount());
        assertEquals(200, valueStats("employees[any].id").getHitCount());
        assertEquals(200, valueStats("employees[any].id").getQueryCount());
    }

    @Test
    public void testHitAndQueryCounting_WhenSingleNumberIndexHit() {
        addIndex(map, "employees[0].id", true);
        addIndex(map, "employees[any].id", true);

        assertEquals(0, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());

        for (int i = 0; i < 10; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j);
            }
            map.put(i, new Department(persons));
        }

        for (int i = 0; i < 100; i++) {
            map.entrySet(Predicates.alwaysTrue());
            map.entrySet(Predicates.equal("employees[0].id", "0"));
        }

        assertEquals(200, stats().getQueryCount());
        assertEquals(100, stats().getIndexedQueryCount());
        assertEquals(100, valueStats("employees[0].id").getHitCount());
        assertEquals(100, valueStats("employees[0].id").getQueryCount());
        assertEquals(0, valueStats("employees[any].id").getHitCount());
        assertEquals(0, valueStats("employees[any].id").getQueryCount());
    }

    @Test
    public void testHitAndQueryCounting_WhenSingleAnyIndexHit() {
        addIndex(map, "employees[0].id", true);
        addIndex(map, "employees[any].id", true);

        assertEquals(0, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());

        for (int i = 0; i < 10; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j);
            }
            map.put(i, new Department(persons));
        }

        for (int i = 0; i < 100; i++) {
            map.entrySet(Predicates.alwaysTrue());
            map.entrySet(Predicates.equal("employees[any].id", "5"));
        }

        assertEquals(200, stats().getQueryCount());
        assertEquals(100, stats().getIndexedQueryCount());
        assertEquals(0, valueStats("employees[0].id").getHitCount());
        assertEquals(0, valueStats("employees[0].id").getQueryCount());
        assertEquals(100, valueStats("employees[any].id").getHitCount());
        assertEquals(100, valueStats("employees[any].id").getQueryCount());
    }

    @Test
    public void testHitCounting_WhenIndexHitMultipleTimes() {
        addIndex(map, "employees[0].id", true);
        addIndex(map, "employees[any].id", true);

        assertEquals(0, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());

        for (int i = 0; i < 10; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j);
            }
            map.put(i, new Department(persons));
        }

        for (int i = 0; i < 100; i++) {
            map.entrySet(Predicates.alwaysTrue());
            map.entrySet(Predicates.or(Predicates.equal("employees[any].id", "0"), Predicates.equal("employees[any].id", "5")));
            map.entrySet(Predicates.or(Predicates.equal("employees[0].id", "0"), Predicates.equal("employees[any].id", "6")));
        }

        assertEquals(300, stats().getQueryCount());
        assertEquals(200, stats().getIndexedQueryCount());
        assertEquals(100, valueStats("employees[0].id").getHitCount());
        assertEquals(100, valueStats("employees[0].id").getQueryCount());
        assertEquals(300, valueStats("employees[any].id").getHitCount());
        assertEquals(200, valueStats("employees[any].id").getQueryCount());

    }

    @Test
    public void testAverageQuerySelectivityCalculation() {
        double expectedEqual_0 = 0.5;
        double expectedEqual_Any = 0.99;
        double expectedGreaterThan_0 = 0.6;
        double expectedGreaterThan_Any = 0.75;

        int iterations = 100;

        addIndex(map, "employees[0].id", true);
        addIndex(map, "employees[any].id", true);
        for (int i = 0; i < 50; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j);
            }
            map.put(i, new Department(persons));
        }
        for (int i = 50; i < 100; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(i + j);
            }
            map.put(i, new Department(persons));
        }

        assertEquals(0.0, valueStats("employees[0].id").getAverageHitSelectivity(), 0.0);
        assertEquals(0.0, valueStats("employees[any].id").getAverageHitSelectivity(), 0.0);

        for (int i = 0; i < iterations; i++) {
            // select entries 0-49
            map.entrySet(Predicates.equal("employees[0].id", "0"));
            // select entry 50
            map.entrySet(Predicates.equal("employees[any].id", "50"));
            assertEquals(expectedEqual_0, valueStats("employees[0].id").getAverageHitSelectivity(), 0.015);
            assertEquals(expectedEqual_Any, valueStats("employees[any].id").getAverageHitSelectivity(), 0.015);
        }

        for (int i = 0; i < iterations; i++) {
            // select entries 60-99
            map.entrySet(Predicates.greaterThan("employees[0].id", "59"));
            // select entries 75-99
            map.entrySet(Predicates.greaterThan("employees[any].id", "83"));
            assertEquals((expectedEqual_0 * iterations + expectedGreaterThan_0 * i) / (iterations + i),
                    valueStats("employees[0].id").getAverageHitSelectivity(), 0.015);
            assertEquals((expectedEqual_Any * iterations + expectedGreaterThan_Any * i) / (iterations + i),
                    valueStats("employees[any].id").getAverageHitSelectivity(), 0.015);

        }

    }

    @Test
    public void testOperationsCounting() {
        addIndex(map, "employees[0].id", true);
        addIndex(map, "employees[any].id", true);

        for (int i = 0; i < 100; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j);
            }
            map.put(i, new Department(persons));
        }
        checkOperations(100, 0, 0);

        for (int i = 50; i < 100; ++i) {
            map.remove(i);
        }
        checkOperations(100, 0, 50);

        for (int i = 0; i < 50; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j * j);
            }
            map.put(i, new Department(persons));
        }
        checkOperations(100, 50, 50);

        for (int i = 50; i < 100; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j + 1);
            }
            map.set(i, new Department(persons));
        }
        checkOperations(150, 50, 50);

        for (int i = 0; i < 50; i++) {
            Employee[] persons = new Employee[EMPLOYEE_ARRAY_SIZE];
            for (int j = 0; j < EMPLOYEE_ARRAY_SIZE; j++) {
                persons[j] = new Employee(j + 1);
            }
            map.set(i, new Department(persons));
        }
        checkOperations(150, 100, 50);
    }

    protected LocalMapStats stats() {
        return map.getLocalMapStats();
    }

    protected LocalIndexStats valueStats(String stat) {
        return stats().getIndexStats().get(stat);
    }

    private void checkOperations(int expectedInserts, int expectedUpdates, int expectedRemoves) {
        assertEquals(expectedInserts, valueStats("employees[0].id").getInsertCount());
        assertEquals(expectedUpdates, valueStats("employees[0].id").getUpdateCount());
        assertEquals(expectedRemoves, valueStats("employees[0].id").getRemoveCount());
        assertEquals(expectedInserts, valueStats("employees[any].id").getInsertCount());
        assertEquals(expectedUpdates, valueStats("employees[any].id").getUpdateCount());
        assertEquals(expectedRemoves, valueStats("employees[any].id").getRemoveCount());
    }

    private static void addIndex(IMap map, String attribute, boolean ordered) {
        IndexConfig config = new IndexConfig(ordered ? IndexType.SORTED : IndexType.HASH, attribute).setName(attribute);

        map.addIndex(config);
    }

    private static class Employee implements Serializable {

        private final int id;

        private Employee(int age) {
            this.id = age;
        }

        public int getAge() {
            return id;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 11 * hash + this.id;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Employee other = (Employee) obj;
            if (this.id != other.id) {
                return false;
            }
            return true;
        }

    }

    private static class Department implements Serializable {

        private final Employee[] employees;

        private Department(Employee[] employees) {
            this.employees = employees;
        }

        public Employee[] getEmployees() {
            return employees;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 89 * hash + Arrays.deepHashCode(this.employees);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Department other = (Department) obj;
            if (!Arrays.deepEquals(this.employees, other.employees)) {
                return false;
            }
            return true;
        }
    }
}
