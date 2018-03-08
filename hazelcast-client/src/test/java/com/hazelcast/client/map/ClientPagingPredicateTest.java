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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Used for testing {@link PagingPredicate}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPagingPredicateTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private HazelcastInstance client;
    private HazelcastInstance server;
    private IMap<Integer, Integer> map;
    private int pageSize = 5;
    private int size = 50;

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
        map = client.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testWithoutAnchor() {
        final PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);
        predicate.nextPage();
        predicate.nextPage();
        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 10, 11, 12, 13, 14);
        predicate.previousPage();

        values = map.values(predicate);
        assertIterableEquals(values, 5, 6, 7, 8, 9);
        predicate.previousPage();

        values = map.values(predicate);
        assertIterableEquals(values, 0, 1, 2, 3, 4);
    }

    @Test
    public void testGoToPreviousPageBeforeTheStart() {
        final PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);
        predicate.previousPage();

        Collection<Integer> values = map.values(predicate);
        values = map.values(predicate);
        assertIterableEquals(values, 0, 1, 2, 3, 4);
    }

    @Test
    public void testGoToNextPageAfterTheEnd() {
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);
        predicate.setPage(size / pageSize - 1);

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 45, 46, 47, 48, 49);

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());

    }

    @Test
    public void testPagingWithoutFilteringAndComparator() {
        Set<Integer> set = new HashSet<Integer>();
        final PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);

        Collection<Integer> values = map.values(predicate);
        while (values.size() > 0) {
            assertEquals(pageSize, values.size());
            set.addAll(values);
            predicate.nextPage();
            values = map.values(predicate);
        }

        assertEquals(size, set.size());
    }

    @Test
    public void testPagingWithFilteringAndComparator() {
        final Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8);
        final TestComparator comparator = new TestComparator(false, IterationType.VALUE);
        final PagingPredicate<Integer, Integer> predicate
                = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize);

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 8, 7, 6, 5, 4);

        predicate.nextPage();
        values = map.values(predicate);
        assertIterableEquals(values, 3, 2, 1, 0);

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testKeyPaging() {
        map.clear();
        for (int i = 0; i < size; i++) {   // keys [50-1] values [0-49]
            map.put(size - i, i);
        }
        final Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8); // less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.KEY); //ascending keys
        final PagingPredicate<Integer, Integer> predicate
                = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize);

        Set<Integer> keySet = map.keySet(predicate);
        assertIterableEquals(keySet, 42, 43, 44, 45, 46);

        predicate.nextPage();
        keySet = map.keySet(predicate);
        assertIterableEquals(keySet, 47, 48, 49, 50);

        predicate.nextPage();
        keySet = map.keySet(predicate);
        assertEquals(0, keySet.size());
    }

    @Test
    public void testEqualValuesPaging() {
        for (int i = size; i < 2 * size; i++) { //keys[50-99] values[0-49]
            map.put(i, i - size);
        }

        final Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8); // entries which has value less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        final PagingPredicate<Integer, Integer> predicate
                = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize); //pageSize = 5

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 0, 0, 1, 1, 2);

        predicate.nextPage();
        values = map.values(predicate);
        assertIterableEquals(values, 2, 3, 3, 4, 4);

        predicate.nextPage();
        values = map.values(predicate);
        assertIterableEquals(values, 5, 5, 6, 6, 7);

        predicate.nextPage();
        values = map.values(predicate);
        assertIterableEquals(values, 7, 8, 8);
    }

    @Test
    public void testNextPageAfterResultSetEmpty() {
        final Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 3); // entries which has value less than 3
        final TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        final PagingPredicate<Integer, Integer> predicate
                = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize); //pageSize = 5

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 0, 1, 2, 3);

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void mapPagingPredicateEmployeeObjectWithOrderedIndexSmallTest() {
        mapPagingPredicateEmployeeObjectWithOrderedIndex(10);
    }

    @Test
    public void mapPagingPredicateEmployeeObjectWithOrderedIndexLargeTest() {
        mapPagingPredicateEmployeeObjectWithOrderedIndex(5000);
    }

    private void mapPagingPredicateEmployeeObjectWithOrderedIndex(int maxEmployee) {
        final IMap<Integer, Employee> map = makeEmployeeMap(maxEmployee);

        map.addIndex("id", true);

        Predicate pred = Predicates.lessThan("id", 2);
        PagingPredicate<Integer, Employee> predicate = new PagingPredicate<Integer, Employee>(pred, 2);

        Collection<Employee> values;

        values = map.values(predicate);
        System.out.println(values);
        assertEquals(2, values.size());

        predicate.nextPage();

        values = map.values(predicate);
        System.out.println(values);
        assertEquals(0, values.size());
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void betweenPagingPredicateWithEmployeeTest() {
        final int minId = 10;
        final int maxId = 15;
        final int pageSz = 5;

        IMap<Integer, Employee> map = makeEmployeeMap(1000);
        Predicate<Integer, Employee> p = Predicates.between("id", minId, maxId);

        List<Employee> expected = new ArrayList<Employee>();
        for (Employee e : map.values()) {
            if (e.getId() >= minId && e.getId() <= maxId) {
                expected.add(e);
            }
        }
        List<Employee> actual = pagingPredicateWithEmployeeObjectTest(map, p, pageSz);

        EmployeeIdComparitor compId = new EmployeeIdComparitor();
        Collections.sort(expected, compId);
        Collections.sort(actual, compId);
        assertEquals(expected, actual);
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void lessThanPredicateWithEmployeeTest() {
        final int maxId = 500;
        final int pageSz = 5;

        IMap<Integer, Employee> map = makeEmployeeMap(1000);
        Predicate<Integer, Employee> p = Predicates.lessThan("id", maxId);

        List<Employee> expected = new ArrayList<Employee>();
        for (Employee e : map.values()) {
            if (e.getId() < maxId) {
                expected.add(e);
            }
        }
        List<Employee> actual = pagingPredicateWithEmployeeObjectTest(map, p, pageSz);

        EmployeeIdComparitor compId = new EmployeeIdComparitor();
        Collections.sort(expected, compId);
        Collections.sort(actual, compId);
        assertEquals(expected, actual);
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void equalsPredicateWithEmployeeTest() {
        final String name = Employee.getRandomName();
        final int pageSz = 5;

        IMap<Integer, Employee> map = makeEmployeeMap(1000);
        Predicate<Integer, Employee> p = Predicates.equal("name", name);

        List<Employee> expected = new ArrayList<Employee>();
        for (Employee e : map.values()) {
            if (e.getName().equals(name)) {
                expected.add(e);
            }
        }
        List<Employee> actual = pagingPredicateWithEmployeeObjectTest(map, p, pageSz);

        EmployeeIdComparitor compId = new EmployeeIdComparitor();
        Collections.sort(expected, compId);
        Collections.sort(actual, compId);
        assertEquals(expected, actual);
    }

    private IMap<Integer, Employee> makeEmployeeMap(int maxEmployees) {
        final IMap<Integer, Employee> map = server.getMap(randomString());
        for (int i = 0; i < maxEmployees; i++) {
            Employee e = new Employee(i);
            map.put(e.id, e);
        }
        return map;
    }

    private List<Employee> pagingPredicateWithEmployeeObjectTest(IMap<Integer, Employee> map, Predicate<Integer, Employee> predicate, int pageSize) {
        PagingPredicate<Integer, Employee> pagingPredicate = new PagingPredicate<Integer, Employee>(predicate, pageSize);
        Set<Map.Entry<Integer, Employee>> set;
        List<Employee> results = new ArrayList<Employee>();
        do {
            set = map.entrySet(pagingPredicate);
            for (Map.Entry<Integer, Employee> entry : set) {
                Employee e = entry.getValue();
                QueryEntry qe = new QueryEntry((InternalSerializationService) serializationService,
                        serializationService.toData(e.getId()), e, Extractors.empty());
                assertTrue(predicate.apply(qe));
                results.add(e);
            }
            pagingPredicate.nextPage();
        } while (!set.isEmpty());

        return results;
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void testIssue3047() {
        final IMap<Integer, Employee> map = client.getMap("employeeMap");
        final int PAGE_SIZE = 5;
        final int START_ID_FOR_QUERY = 0;
        final int FINISH_ID_FOR_QUERY = 50;
        final int queriedElementCount = FINISH_ID_FOR_QUERY - START_ID_FOR_QUERY + 1;
        final int expectedPageCount =
                (queriedElementCount / PAGE_SIZE) +
                        (queriedElementCount % PAGE_SIZE == 0 ? 0 : 1);

        for (int i = 0; i < 1000; i++) {
            map.put(i, new Employee(i));
        }

        map.addIndex("id", true);

        Predicate<Integer, Employee> pred = Predicates.between("id", START_ID_FOR_QUERY, FINISH_ID_FOR_QUERY);

        PagingPredicate<Integer, Employee> predicate = new PagingPredicate<Integer, Employee>(pred, PAGE_SIZE);
        Collection<Employee> values;
        int passedPageCount = 0;

        for (values = map.values(predicate); !values.isEmpty() &&
                passedPageCount <= expectedPageCount; // To prevent from infinite loop
             values = map.values(predicate)) {
            predicate.nextPage();
            passedPageCount++;
        }

        assertEquals(expectedPageCount, passedPageCount);
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test(expected = IllegalArgumentException.class)
    public void testIssue3047ForNonComparableEntitiesThrowsIllegalArgumentException() {
        final IMap<Integer, BaseEmployee> map = client.getMap("baseEmployeeMap");
        final int PAGE_SIZE = 5;
        final int START_ID_FOR_QUERY = 0;
        final int FINISH_ID_FOR_QUERY = 50;

        for (int i = 0; i < 100; i++) {
            map.put(i, new BaseEmployee(i));
        }

        map.addIndex("id", true);

        Predicate<Integer, Employee> pred = Predicates.between("id", START_ID_FOR_QUERY, FINISH_ID_FOR_QUERY);

        PagingPredicate<Integer, Employee> predicate = new PagingPredicate<Integer, Employee>(pred, PAGE_SIZE);
        Collection<BaseEmployee> values;

        for (values = map.values(predicate); !values.isEmpty() &&
                values != null;
             values = map.values(predicate)) {
            predicate.nextPage();
        }
    }

    @Test
    public void testLargePageSizeIsNotCausingIndexOutBoundsExceptions() {
        final int[] pageSizesToCheck = new int[]{
                Integer.MAX_VALUE / 2, Integer.MAX_VALUE - 1000, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};

        final int[] pagesToCheck = {1, 1000,
                                    Integer.MAX_VALUE / 2, Integer.MAX_VALUE - 1000, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};

        for (int pageSize : pageSizesToCheck) {
            final PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);

            assertEquals(size, map.keySet(predicate).size());
            for (int page : pagesToCheck) {
                predicate.setPage(page);
                assertEquals(0, map.keySet(predicate).size());
            }
        }
    }

    private static class TestComparator implements Comparator<Map.Entry<Integer, Integer>>, Serializable {

        int ascending = 1;

        IterationType iterationType = IterationType.ENTRY;

        TestComparator() {
        }

        TestComparator(boolean ascending, IterationType iterationType) {
            this.ascending = ascending ? 1 : -1;
            this.iterationType = iterationType;
        }

        public int compare(Map.Entry<Integer, Integer> e1, Map.Entry<Integer, Integer> e2) {
            Map.Entry<Integer, Integer> o1 = e1;
            Map.Entry<Integer, Integer> o2 = e2;

            switch (iterationType) {
                case KEY:
                    return (o1.getKey() - o2.getKey()) * ascending;
                case VALUE:
                    return (o1.getValue() - o2.getValue()) * ascending;
                default:
                    int result = (o1.getValue() - o2.getValue()) * ascending;
                    if (result != 0) {
                        return result;
                    }
                    return (o1.getKey() - o2.getKey()) * ascending;
            }
        }
    }


    private static class EmployeeIdComparitor implements Comparator<Employee> {
        public int compare(Employee e1, Employee e2) {
            return e1.getId() - e2.getId();
        }
    }

    private static class BaseEmployee implements Serializable {

        public static final int MAX_AGE = 75;
        public static final double MAX_SALARY = 1000.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        protected int id;
        protected String name;
        protected int age;
        protected boolean active;
        protected double salary;

        public BaseEmployee() {
        }

        public BaseEmployee(int id) {
            this.id = id;
            setAtributesRandomly();
        }

        public static String getRandomName() {
            return names[random.nextInt(names.length)];
        }

        public void setAtributesRandomly() {
            name = names[random.nextInt(names.length)];
            age = random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = random.nextDouble() * MAX_SALARY;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
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
        public boolean equals(Object obj) {
            if (obj instanceof Employee) {
                return id == ((Employee) obj).getId();
            }
            return false;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", active=" + active +
                    ", salary=" + salary +
                    '}';
        }

    }

    private static class Employee extends BaseEmployee implements Comparable<Employee> {

        public Employee() {
        }

        public Employee(int id) {
            super(id);
        }

        @Override
        public int compareTo(Employee employee) {
            return id - employee.id;
        }

    }

}
