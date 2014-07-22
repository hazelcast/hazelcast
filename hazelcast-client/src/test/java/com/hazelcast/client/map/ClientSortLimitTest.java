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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Used for testing {@link PagingPredicate}
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientSortLimitTest extends HazelcastTestSupport {

    static HazelcastInstance client;
    static HazelcastInstance server1;
    static HazelcastInstance server2;

    static IMap map;
    static int pageSize = 5;
    static int size = 50;

    @BeforeClass
    public static void createInstances(){
        server1 = Hazelcast.newHazelcastInstance();
        server2 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void shutdownInstances(){
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void init(){
        map = client.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
    }

    @After
    public void reset(){
        map.destroy();
    }

    @Test
    public void testWithoutAnchor() {
        final PagingPredicate predicate = new PagingPredicate(pageSize);
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
    public void testGoTo_previousPage_BeforeTheStart() {
        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.previousPage();

        Collection<Integer> values = map.values(predicate);
        values = map.values(predicate);
        assertIterableEquals(values, 0, 1, 2, 3, 4);
    }

    @Test
    public void testGoTo_NextPage_AfterTheEnd() {
        final PagingPredicate predicate = new PagingPredicate(pageSize);

        for ( int i=0; i < size/pageSize; i++ ) {
            predicate.nextPage();
        }

        Collection<Integer> values = map.values(predicate);
        values = map.values(predicate);

        assertEquals( size/pageSize - 1, predicate.getPage());
        assertIterableEquals(values, 45, 46, 47, 48, 49);
    }

    @Test
    public void testPagingWithoutFilteringAndComparator() {
        Set<Integer> set = new HashSet<Integer>();
        final PagingPredicate predicate = new PagingPredicate(pageSize);

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
        final Predicate lessEqual = Predicates.lessEqual("this", 8);
        final TestComparator comparator = new TestComparator(false, IterationType.VALUE);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize);

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 8, 7, 6, 5, 4);

        predicate.nextPage();
        assertEquals(4, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertIterableEquals(values, 3, 2, 1, 0);

        predicate.nextPage();
        assertEquals(0, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testKeyPaging() {
        map.clear();
        for (int i = 0; i < size; i++) {   // keys [50-1] values [0-49]
            map.put(size - i, i);
        }
        final Predicate lessEqual = Predicates.lessEqual("this", 8);    // less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.KEY);  //ascending keys
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize);

        Set<Integer> keySet = map.keySet(predicate);
        assertIterableEquals(keySet, 42, 43, 44, 45, 46);


        predicate.nextPage();
        assertEquals(46, predicate.getAnchor().getKey());
        keySet = map.keySet(predicate);
        assertIterableEquals(keySet, 47, 48, 49, 50);

        predicate.nextPage();
        assertEquals(50, predicate.getAnchor().getKey());
        keySet = map.keySet(predicate);
        assertEquals(0, keySet.size());
    }

    @Test
    public void testEqualValuesPaging() {
        for (int i = size; i < 2 * size; i++) { //keys[50-99] values[0-49]
            map.put(i, i - size);
        }

        final Predicate lessEqual = Predicates.lessEqual("this", 8); // entries which has value less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize); //pageSize = 5

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
    public void testNextPageAfterResultSetEmpty(){
        final Predicate lessEqual = Predicates.lessEqual("this", 3); // entries which has value less than 3
        final TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize); //pageSize = 5

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
    public void test_whilePageingPredicateRunning_Controle(){
        final Predicate lessEqual = Predicates.lessEqual("this", 5);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, 5);

        Collection<Integer> values = map.values(predicate);
        for(int i=0; i<5; i++){
            assertTrue(values.contains(i));
        }

        predicate.nextPage();
        values = map.values(predicate);
        assertTrue(values.contains(5));
        assertEquals(1, values.size());
    }

    @Test
    public void test_whilePageingPredicateRunning_changeValueToFailePredicate(){
        final Predicate lessEqual = Predicates.lessEqual("this", 5);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, 5);

        Collection<Integer> values = map.values(predicate);
        for(int i=0; i<5; i++){
            assertTrue(values.contains(i));
        }

        map.put(5, 500);

        predicate.nextPage();
        values = map.values(predicate);
        assertTrue(values.isEmpty());
    }

    @Test
    @Category(ProblematicTest.class)
    public void test_whilePageingPredicateRunning_changeValueToPassPredicate(){
        final Predicate lessEqual = Predicates.lessEqual("this", 5);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, 5);

        Collection<Integer> values = map.values(predicate);
        for(int i=0; i<5; i++){
            assertTrue( values.contains(i) );
        }

        map.put(5, 0);
        predicate.nextPage();
        values = map.values(predicate);
        assertTrue( values.contains(0) );
    }

    @Test
    public void test_whilePageingPredicateRunning_changeValueToFailePredicate_fromDiffMap(){
        final Predicate lessEqual = Predicates.lessEqual("this", 5);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, 5);

        Collection<Integer> values = map.values(predicate);
        for(int i=0; i<5; i++){
            assertTrue(values.contains(i));
        }

        String name = map.getName();
        IMap server_map = server1.getMap(name);
        server_map.put(5, 500);


        predicate.nextPage();
        values = map.values(predicate);
        assertTrue(values.isEmpty());
    }

    @Test
    @Category(ProblematicTest.class)
    public void test_whilePageingPredicateRunning_changeValueToPassPredicate_fromDiffMap(){
        final Predicate lessEqual = Predicates.lessEqual("this", 5);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, 5);

        Collection<Integer> values = map.values(predicate);
        for(int i=0; i<5; i++){
            assertTrue( values.contains(i) );
        }

        String name = map.getName();
        IMap server_map = server1.getMap(name);
        server_map.put(5, 0);

        predicate.nextPage();
        values = map.values(predicate);
        assertTrue( values.contains(0) );
    }



    @Test
    @Category(ProblematicTest.class)
    public void play2(){
        final Random random = new Random();
        final IMap<Integer, Employee> map = server1.getMap("playMap1");

        for(int i=0; i<100; i++){
            map.put(i, new Employee(i));
        }

        map.addIndex( "salary", true );

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    Employee e = map.get(random.nextInt(map.size()));
                    e.setInfo();
                    map.put(e.id, e);

                }

            }
        }).start();

        Predicate  pred = Predicates.between("salary", 250.0, 750.0);
        PagingPredicate predicate = new PagingPredicate(pred, 5);
        Collection<Employee> values;
        do{
            values = map.values(predicate);
            predicate.nextPage();

            System.out.println(values.size());
        }while(! values.isEmpty());
    }


    @Test
    @Category(ProblematicTest.class)
    public void play3(){
        final IMap<Integer, Employee> map = server1.getMap("playMap1");

        for(int i=0; i<100; i++){
            map.put(i, new Employee(i));
        }

        map.addIndex("id", true );


        Predicate  pred = Predicates.between("id", 10, 15);

        PagingPredicate predicate = new PagingPredicate(pred, 5);
        Collection<Employee> values;

        do{
            values = map.values(predicate);
            predicate.nextPage();

            for(Employee e : values){
                System.out.println(e);
            }
            System.out.println(values.size());

        }while(! values.isEmpty());
    }


    @Test
    @Category(ProblematicTest.class)
    public void loopingForEver_OrVeryVeryLong(){
        final IMap<Integer, Employee> map = server1.getMap("playMap1");

        for(int i=0; i<1000; i++){
            map.put(i, new Employee(i));
        }

        map.addIndex("id", true );


        Predicate  pred = Predicates.between("id", 0, 50);

        PagingPredicate predicate = new PagingPredicate(pred, 5);
        Collection<Employee> values;

        do{
            values = map.values(predicate);
            predicate.nextPage();

            for(Employee e : values){
                System.out.println(e);
            }
            System.out.println(values.size());

        }while(! values.isEmpty());
    }



    static class TestComparator implements Comparator<Map.Entry>, Serializable {

        int ascending = 1;

        IterationType iterationType = IterationType.ENTRY;

        TestComparator() {
        }

        TestComparator(boolean ascending, IterationType iterationType) {
            this.ascending = ascending ? 1 : -1;
            this.iterationType = iterationType;
        }

        public int compare(Map.Entry e1, Map.Entry e2) {
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




    public static class Employee implements Serializable {

        public static final int MAX_AGE=75;
        public static final double MAX_SALARY=1000.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        private int id;
        private String name;
        private int age;
        private boolean active;
        private double salary;


        public Employee(int id) {
            this.id = id;
            setInfo();
        }

        public Employee() {
        }

        public void setInfo(){
            name = names[random.nextInt(names.length)];
            age = random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = random.nextDouble() * MAX_SALARY;
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
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", active=" + active +
                    ", salary=" + salary +
                    '}';
        }
    }


}
