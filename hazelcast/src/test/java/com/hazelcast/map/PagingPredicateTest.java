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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.IterationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PagingPredicateTest extends HazelcastTestSupport {

    protected static final int size = 50;
    protected static final int pageSize = 5;

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    protected final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    protected HazelcastInstance local;
    protected HazelcastInstance remote;
    protected IMap<Integer, Integer> map;

    protected HazelcastInstance createRemote(Config config) {
        return hazelcastFactory.newHazelcastInstance(config);
    }

    @Before
    public void setup() {
        Config config = getConfig();
        local = hazelcastFactory.newHazelcastInstance(config);
        remote = createRemote(config);
        map = remote.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
    }

    @After
    public void after() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testLocalPaging() {
        IMap<Integer, Integer> map1 = local.getMap("testSort");
        IMap<Integer, Integer> map2 = remote.getMap("testSort");

        for (int i = 0; i < size; i++) {
            map1.put(i + 10, i);
        }

        PagingPredicate<Integer, Integer> predicate1 = Predicates.pagingPredicate(pageSize);
        Set<Integer> keySet = map1.localKeySet(predicate1);

        int value = 9;
        Set<Integer> whole = new HashSet<>(size);
        while (keySet.size() > 0) {
            for (Integer integer : keySet) {
                assertTrue(integer > value);
                value = integer;
                whole.add(integer);
            }
            predicate1.nextPage();
            keySet = map1.localKeySet(predicate1);
        }

        PagingPredicate<Integer, Integer> predicate2 = Predicates.pagingPredicate(pageSize);
        value = 9;
        keySet = map2.localKeySet(predicate2);
        while (keySet.size() > 0) {
            for (Integer integer : keySet) {
                assertTrue(integer > value);
                value = integer;
                whole.add(integer);
            }
            predicate2.nextPage();
            keySet = map2.localKeySet(predicate2);
        }

        assertEquals(size, whole.size());
    }

    @Test
    public void testWithoutAnchor() {
        final PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);
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
    public void testPagingWithoutFilteringAndComparator() {
        List<Integer> set = new ArrayList<>();
        PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);

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
        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8);
        PagingPredicate<Integer, Integer> predicate
                = Predicates.pagingPredicate(lessEqual, new TestComparator(false, IterationType.VALUE), pageSize);

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
    public void testPagingWithFilteringAndComparatorAndIndex() {
        map.addIndex(IndexType.SORTED, "this");
        Predicate<Integer, Integer> lessEqual = Predicates.between("this", 12, 20);
        TestComparator comparator = new TestComparator(false, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(lessEqual, comparator, pageSize);

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 20, 19, 18, 17, 16);

        predicate.nextPage();
        values = map.values(predicate);
        assertIterableEquals(values, 15, 14, 13, 12);

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testKeyPaging() {
        map.clear();
        // keys [50-1] values [0-49]
        for (int i = 0; i < size; i++) {
            map.put(size - i, i);
        }

        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8);    // values less than 8
        TestComparator comparator = new TestComparator(true, IterationType.KEY);    //ascending keys
        PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(lessEqual, comparator, pageSize);

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
        // keys[50-99] values[0-49]
        for (int i = size; i < 2 * size; i++) {
            map.put(i, i - size);
        }

        // entries which has value less than 8
        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8);
        // ascending values
        TestComparator comparator = new TestComparator(true, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate
                = Predicates.pagingPredicate(lessEqual, comparator, pageSize); //pageSize = 5

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
    public void testGoToPreviousPageBeforeTheStart() {
        final PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);
        predicate.previousPage();

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 0, 1, 2, 3, 4);
    }

    @Test
    public void testNextPageAfterResultSetEmpty() {
        // entries which has value less than 3
        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 3);
        // ascending values
        TestComparator comparator = new TestComparator(true, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate
                = Predicates.pagingPredicate(lessEqual, comparator, pageSize); //pageSize = 5

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
    public void testLargePageSizeIsNotCausingIndexOutBoundsExceptions() {
        final int[] pageSizesToCheck = new int[]{
                Integer.MAX_VALUE / 2,
                Integer.MAX_VALUE - 1000,
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE,
        };

        final int[] pagesToCheck = new int[]{
                1,
                1000,
                Integer.MAX_VALUE / 2,
                Integer.MAX_VALUE - 1000,
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE,
        };

        for (int pageSize : pageSizesToCheck) {
            final PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);

            assertEquals(size, map.keySet(predicate).size());
            for (int page : pagesToCheck) {
                predicate.setPage(page);
                assertEquals(0, map.keySet(predicate).size());
            }
        }
    }

    @Test
    public void testEmptyIndexResultIsNotCausingFullScan() {
        map.addIndex(IndexType.HASH, "this");
        for (int i = 0; i < size; ++i) {
            map.set(i, i);
        }

        int resultSize = map.entrySet(new PagingPredicateImpl<Integer, Integer>(Predicates.equal("this", size), pageSize) {
            @Override
            public boolean apply(Map.Entry mapEntry) {
                fail("full scan is not expected");
                return false;
            }
        }).size();
        assertEquals(0, resultSize);
    }

    @Test
    public void testCustomComparatorAbleToActOnKeysAndValues() {
        Set<Integer> keys = map.keySet(Predicates.pagingPredicate(new CustomComparator(), pageSize));
        assertEquals(pageSize, keys.size());
        int counter = 0;
        for (Integer key : keys) {
            assertEquals(counter++, (int) key);
        }

        Collection<Integer> values = map.values(Predicates.pagingPredicate(new CustomComparator(), pageSize));
        assertEquals(pageSize, values.size());
        counter = 0;
        for (Integer value : values) {
            assertEquals(counter++, (int) value);
        }

        Set<Map.Entry<Integer, Integer>> entries = map.entrySet(Predicates.pagingPredicate(new CustomComparator(), pageSize));
        assertEquals(pageSize, entries.size());
        counter = 0;
        for (Map.Entry<Integer, Integer> entry : entries) {
            assertEquals(counter, (int) entry.getKey());
            assertEquals(counter, (int) entry.getValue());
            ++counter;
        }
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void betweenPagingPredicateWithEmployeeTest() {
        final int minId = 10;
        final int maxId = 15;
        final int pageSz = 5;

        IMap<Integer, Employee> map = makeEmployeeMap(1000);
        Predicate<Integer, Employee> p = Predicates.between("id", minId, maxId);

        List<Employee> expected = new ArrayList<>();
        for (Employee e : map.values()) {
            if (e.getId() >= minId && e.getId() <= maxId) {
                expected.add(e);
            }
        }
        List<Employee> actual = pagingPredicateWithEmployeeObjectTest(map, p, pageSz);

        EmployeeIdComparator compId = new EmployeeIdComparator();
        expected.sort(compId);
        actual.sort(compId);
        assertEquals(expected, actual);
    }

    @SuppressWarnings({"SameParameterValue", "unchecked"})
    private List<Employee> pagingPredicateWithEmployeeObjectTest(IMap<Integer, Employee> map,
                                                                 Predicate<Integer, Employee> predicate, int pageSize) {
        PagingPredicate<Integer, Employee> pagingPredicate = Predicates.pagingPredicate(predicate, pageSize);
        Set<Map.Entry<Integer, Employee>> set;
        List<Employee> results = new ArrayList<>();
        do {
            set = map.entrySet(pagingPredicate);
            for (Map.Entry<Integer, Employee> entry : set) {
                Employee e = entry.getValue();
                InternalSerializationService ss = (InternalSerializationService) serializationService;
                QueryEntry qe = new QueryEntry(ss, ss.toData(e.getId()), e, Extractors.newBuilder(ss).build());
                assertTrue(predicate.apply(qe));
                results.add(e);
            }
            pagingPredicate.nextPage();
        } while (!set.isEmpty());

        return results;
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

        map.addIndex(IndexType.SORTED, "id");

        Predicate innerPredicate = Predicates.lessThan("id", 2);
        PagingPredicate<Integer, Employee> predicate = Predicates.pagingPredicate(innerPredicate, 2);

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
    public void lessThanPredicateWithEmployeeTest() {
        final int maxId = 500;
        final int pageSz = 5;

        IMap<Integer, Employee> map = makeEmployeeMap(1000);
        Predicate<Integer, Employee> p = Predicates.lessThan("id", maxId);

        List<Employee> expected = new ArrayList<>();
        for (Employee e : map.values()) {
            if (e.getId() < maxId) {
                expected.add(e);
            }
        }
        List<Employee> actual = pagingPredicateWithEmployeeObjectTest(map, p, pageSz);

        EmployeeIdComparator compId = new EmployeeIdComparator();
        expected.sort(compId);
        actual.sort(compId);
        assertEquals(expected, actual);
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void equalsPredicateWithEmployeeTest() {
        final String name = Employee.getRandomName();
        final int pageSz = 5;

        IMap<Integer, Employee> map = makeEmployeeMap(1000);
        Predicate<Integer, Employee> p = Predicates.equal("name", name);

        List<Employee> expected = new ArrayList<>();
        for (Employee e : map.values()) {
            if (e.getName().equals(name)) {
                expected.add(e);
            }
        }
        List<Employee> actual = pagingPredicateWithEmployeeObjectTest(map, p, pageSz);

        EmployeeIdComparator compId = new EmployeeIdComparator();
        expected.sort(compId);
        actual.sort(compId);
        assertEquals(expected, actual);
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test
    public void testIssue3047() {
        final IMap<Integer, Employee> map = local.getMap("employeeMap");
        final int PAGE_SIZE = 5;
        final int START_ID_FOR_QUERY = 0;
        final int FINISH_ID_FOR_QUERY = 50;
        final int queriedElementCount = FINISH_ID_FOR_QUERY - START_ID_FOR_QUERY + 1;
        final int expectedPageCount = (queriedElementCount / PAGE_SIZE) + (queriedElementCount % PAGE_SIZE == 0 ? 0 : 1);

        for (int i = 0; i < 1000; i++) {
            map.put(i, new Employee(i));
        }

        map.addIndex(IndexType.SORTED, "id");

        Predicate<Integer, Employee> innerPredicate = Predicates.between("id", START_ID_FOR_QUERY, FINISH_ID_FOR_QUERY);

        PagingPredicate<Integer, Employee> predicate = Predicates.pagingPredicate(innerPredicate, PAGE_SIZE);
        Collection<Employee> values;
        int passedPageCount = 0;

        do {
            predicate.nextPage();
            passedPageCount++;
            values = map.values(predicate);
            // to prevent infinite loop
        } while (!values.isEmpty() && passedPageCount <= expectedPageCount);

        assertEquals(expectedPageCount, passedPageCount);
    }

    // https://github.com/hazelcast/hazelcast/issues/3047
    @Test(expected = IllegalArgumentException.class)
    public void testIssue3047ForNonComparableEntitiesThrowsIllegalArgumentException() {
        final IMap<Integer, BaseEmployee> map = local.getMap("baseEmployeeMap");
        final int PAGE_SIZE = 5;
        final int START_ID_FOR_QUERY = 0;
        final int FINISH_ID_FOR_QUERY = 50;

        for (int i = 0; i < 100; i++) {
            map.put(i, new BaseEmployee(i));
        }

        map.addIndex(IndexType.SORTED, "id");

        Predicate<Integer, BaseEmployee> innerPredicate = Predicates.between("id", START_ID_FOR_QUERY, FINISH_ID_FOR_QUERY);

        PagingPredicate<Integer, BaseEmployee> predicate = Predicates.pagingPredicate(innerPredicate, PAGE_SIZE);
        Collection<BaseEmployee> values;

        for (values = map.values(predicate); !values.isEmpty(); values = map.values(predicate)) {
            predicate.nextPage();
        }
    }

    private IMap<Integer, Employee> makeEmployeeMap(int maxEmployees) {
        final IMap<Integer, Employee> map = remote.getMap(randomString());
        for (int i = 0; i < maxEmployees; i++) {
            Employee e = new Employee(i);
            map.put(e.id, e);
        }
        return map;
    }

    static class TestComparator implements Comparator<Map.Entry<Integer, Integer>>, Serializable {

        final int ascending;

        final IterationType iterationType;

        TestComparator(boolean ascending, IterationType iterationType) {
            this.ascending = ascending ? 1 : -1;
            this.iterationType = iterationType;
        }

        @Override
        public int compare(Map.Entry<Integer, Integer> e1, Map.Entry<Integer, Integer> e2) {
            switch (iterationType) {
                case KEY:
                    return (e1.getKey() - e2.getKey()) * ascending;
                case VALUE:
                    return (e1.getValue() - e2.getValue()) * ascending;
                default:
                    int result = (e1.getValue() - e2.getValue()) * ascending;
                    if (result != 0) {
                        return result;
                    }
                    return (e1.getKey() - e2.getKey()) * ascending;
            }
        }
    }

    static class CustomComparator implements Comparator<Map.Entry<Integer, Integer>>, Serializable {

        @Override
        public int compare(Map.Entry<Integer, Integer> a, Map.Entry<Integer, Integer> b) {
            assertNotNull(a.getKey());
            assertNotNull(a.getValue());
            assertNotNull(b.getKey());
            assertNotNull(b.getValue());
            return a.getKey() - b.getValue();
        }

    }

    private static class BaseEmployee implements Serializable {

        public static final int MAX_AGE = 75;
        public static final double MAX_SALARY = 1000.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        protected final int id;
        protected String name;
        protected int age;
        protected boolean active;
        protected double salary;

        BaseEmployee(int id) {
            this.id = id;
            setAttributesRandomly();
        }

        public static String getRandomName() {
            return names[random.nextInt(names.length)];
        }

        public void setAttributesRandomly() {
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

        @SuppressWarnings("unused")
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
            return "Employee{"
                    + "id=" + id
                    + ", name='" + name + '\''
                    + ", age=" + age
                    + ", active=" + active
                    + ", salary=" + salary
                    + '}';
        }

    }

    private static class Employee extends BaseEmployee implements Comparable<Employee> {

        Employee(int id) {
            super(id);
        }

        @Override
        public int compareTo(Employee employee) {
            return id - employee.id;
        }

    }

    private static class EmployeeIdComparator implements Comparator<Employee> {
        public int compare(Employee e1, Employee e2) {
            return e1.getId() - e2.getId();
        }
    }


}
