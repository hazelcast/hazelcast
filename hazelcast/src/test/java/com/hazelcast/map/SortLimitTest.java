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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.impl.predicate.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Used for testing {@link PagingPredicate}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SortLimitTest extends HazelcastTestSupport {

    static int size = 50;
    static int pageSize = 5;

    @Test
    public void testLocalPaging() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map1 = instance1.getMap("testSort");
        final IMap<Integer, Integer> map2 = instance2.getMap("testSort");

        for (int i = 0; i < size; i++) {
            map1.put(i + 10, i);
        }

        final PagingPredicate predicate1 = new PagingPredicate(pageSize);
        Set<Integer> keySet = map1.localKeySet(predicate1);

        int value = 9;
        Set<Integer> whole = new HashSet<Integer>(size);
        while (keySet.size() > 0) {
            for (Integer integer : keySet) {
                assertTrue(integer > value);
                value = integer;
                whole.add(integer);
            }
            predicate1.nextPage();
            keySet = map1.localKeySet(predicate1);
        }

        final PagingPredicate predicate2 = new PagingPredicate(pageSize);
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
        final IMap<Integer, Integer> map = initMap();

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();
        predicate.nextPage();
        Collection<Integer> values = map.values(predicate);
        assertEquals(5, values.size());
        Integer value = 10;
        for (Integer val : values) {
            assertEquals(value++, val);
        }
        predicate.previousPage();

        values = map.values(predicate);
        assertEquals(5, values.size());
        value = 5;
        for (Integer val : values) {
            assertEquals(value++, val);
        }
        predicate.previousPage();

        values = map.values(predicate);
        assertEquals(5, values.size());
        value = 0;
        for (Integer val : values) {
            assertEquals(value++, val);
        }

    }

    @Test
    public void testPagingWithoutFilteringAndComparator() {
        final IMap<Integer, Integer> map = initMap();

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
        final IMap<Integer, Integer> map = initMap();

        final Predicate lessEqual = Predicates.lessEqual("this", 8);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, new TestComparator(false, IterationType.VALUE), pageSize);

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
    public void testPagingWithFilteringAndComparatorAndIndex() {
        final IMap<Integer, Integer> map = initMap();

        map.addIndex("this", true);
        final Predicate lessEqual = Predicates.between("this", 12, 20);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, new TestComparator(false, IterationType.VALUE), pageSize);

        Collection<Integer> values = map.values(predicate);
        assertIterableEquals(values, 20, 19, 18, 17, 16);

        predicate.nextPage();
        assertEquals(16, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertIterableEquals(values, 15, 14, 13, 12);

        predicate.nextPage();
        assertEquals(12, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testKeyPaging() {
        final IMap<Integer, Integer> map = initMap();
        map.clear();
        for (int i = 0; i < size; i++) {    // keys [50-1] values [0-49]
            map.put(size - i, i);
        }

        final Predicate lessEqual = Predicates.lessEqual("this", 8);    // values less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.KEY);    //ascending keys
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
        final IMap<Integer, Integer> map = initMap();
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
    public void testNextPageAfterResultSetEmpty() {
        final IMap<Integer, Integer> map = initMap();

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

    private IMap<Integer, Integer> initMap(){
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();
        final IMap<Integer, Integer> map = instance1.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        return map;
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

}
