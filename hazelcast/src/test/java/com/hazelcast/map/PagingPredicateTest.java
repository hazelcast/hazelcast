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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Before;
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
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PagingPredicateTest extends HazelcastTestSupport {

    private static int size = 50;
    private static int pageSize = 5;

    private HazelcastInstance local;
    private HazelcastInstance remote;
    private IMap<Integer, Integer> map;

    @Before
    public void setup() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        local = nodeFactory.newHazelcastInstance(config);
        remote = nodeFactory.newHazelcastInstance(config);
        map = local.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
    }

    @Test
    public void testLocalPaging() {
        IMap<Integer, Integer> map1 = local.getMap("testSort");
        IMap<Integer, Integer> map2 = remote.getMap("testSort");

        for (int i = 0; i < size; i++) {
            map1.put(i + 10, i);
        }

        PagingPredicate<Integer, Integer> predicate1 = new PagingPredicate<Integer, Integer>(pageSize);
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

        PagingPredicate<Integer, Integer> predicate2 = new PagingPredicate<Integer, Integer>(pageSize);
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
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);
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
        Set<Integer> set = new HashSet<Integer>();
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);

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
                = new PagingPredicate<Integer, Integer>(lessEqual, new TestComparator(false, IterationType.VALUE), pageSize);

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
        map.addIndex("this", true);
        Predicate<Integer, Integer> lessEqual = Predicates.between("this", 12, 20);
        TestComparator comparator = new TestComparator(false, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize);

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
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize);

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
        // entries which has value less than 3
        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 3);
        // ascending values
        TestComparator comparator = new TestComparator(true, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate
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
            final PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);

            assertEquals(size, map.keySet(predicate).size());
            for (int page : pagesToCheck) {
                predicate.setPage(page);
                assertEquals(0, map.keySet(predicate).size());
            }
        }
    }

    @Test
    public void testEmptyIndexResultIsNotCausingFullScan() {
        map.addIndex("this", false);
        for (int i = 0; i < size; ++i) {
            map.set(i, i);
        }

        int resultSize = map.entrySet(new PagingPredicate(Predicates.equal("this", size), pageSize) {
            @Override
            public boolean apply(Map.Entry mapEntry) {
                fail("full scan is not expected");
                return false;
            }
        }).size();
        assertEquals(0, resultSize);
    }

    static class TestComparator implements Comparator<Map.Entry<Integer, Integer>>, Serializable {

        int ascending = 1;

        IterationType iterationType = IterationType.ENTRY;

        TestComparator() {
        }

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
}
