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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;

/**
 * TODO add JavaDoc
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientSortLimitTest {

    @After
    public void reset(){
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testWithoutAnchor() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Integer, Integer> map = client.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

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
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Integer, Integer> map = client.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

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
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Integer, Integer> map = client.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        Integer value = 8;
        final Predicate lessEqual = Predicates.lessEqual("this", value);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, new TestComparator(false, IterationType.VALUE), pageSize);

        Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());
        for (Integer val : values) {
            assertEquals(value--, val);
        }


        predicate.nextPage();
        assertEquals(value + 1, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertEquals(4, values.size());
        for (Integer val : values) {
            assertEquals(value--, val);
        }

        predicate.nextPage();
        assertEquals(value + 1, predicate.getAnchor().getValue());
        assertEquals(0, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertEquals(0, values.size());

    }

    @Test
    public void testKeyPaging() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Integer, Integer> map = client.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {   // keys [50-1] values [0-49]
            map.put(size - i, i);
        }
        Integer value = 8;
        final Predicate lessEqual = Predicates.lessEqual("this", value);    // less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.KEY);  //ascending keys
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize);

        Set<Integer> keySet = map.keySet(predicate); //
        assertEquals(pageSize, keySet.size());
        value = 42;
        for (Integer val : keySet) {
            assertEquals(value++, val);
        }


        predicate.nextPage();
        assertEquals(46, predicate.getAnchor().getKey());
        keySet = map.keySet(predicate);
        assertEquals(4, keySet.size());
        for (Integer val : keySet) {
            assertEquals(value++, val);
        }

        predicate.nextPage();
        assertEquals(50, predicate.getAnchor().getKey());
        keySet = map.keySet(predicate);
        assertEquals(0, keySet.size());
    }

    @Test
    public void testEqualValuesPaging() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Integer, Integer> map = client.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {      //keys[0-49] values[0-49]
            map.put(i, i);
        }

        for (int i = size; i < 2 * size; i++) { //keys[50-99] values[0-49]
            map.put(i, i - size);
        }

        Integer value = 8;
        final Predicate lessEqual = Predicates.lessEqual("this", value); // entries which has value less than 8
        final TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize); //pageSize = 5

        Collection<Integer> values = map.values(predicate); // [0, 0, 1, 1, 2]
        assertEquals(pageSize, values.size());
        Iterator<Integer> iterator = values.iterator();
        assertEquals(0, iterator.next().intValue());
        assertEquals(0, iterator.next().intValue());
        assertEquals(1, iterator.next().intValue());
        assertEquals(1, iterator.next().intValue());
        assertEquals(2, iterator.next().intValue());
        assertFalse(iterator.hasNext());

        predicate.nextPage();
        values = map.values(predicate);     // [2, 3, 3, 4, 4]
        assertEquals(pageSize, values.size());
        iterator = values.iterator();
        assertEquals(2, iterator.next().intValue());
        assertEquals(3, iterator.next().intValue());
        assertEquals(3, iterator.next().intValue());
        assertEquals(4, iterator.next().intValue());
        assertEquals(4, iterator.next().intValue());
        assertFalse(iterator.hasNext());

        predicate.nextPage();
        values = map.values(predicate);     // [5, 5, 6, 6, 7]
        assertEquals(pageSize, values.size());
        iterator = values.iterator();
        assertEquals(5, iterator.next().intValue());
        assertEquals(5, iterator.next().intValue());
        assertEquals(6, iterator.next().intValue());
        assertEquals(6, iterator.next().intValue());
        assertEquals(7, iterator.next().intValue());
        assertFalse(iterator.hasNext());

        predicate.nextPage();
        values = map.values(predicate);     // [7, 8, 8]
        assertEquals(pageSize-2, values.size());
        iterator = values.iterator();
        assertEquals(7, iterator.next().intValue());
        assertEquals(8, iterator.next().intValue());
        assertEquals(8, iterator.next().intValue());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void testNextPageAfterResultSetEmpty(){
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Integer, Integer> map = client.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {      //keys[0-49] values[0-49]
            map.put(i, i);
        }

        Integer value = 3;
        final Predicate lessEqual = Predicates.lessEqual("this", value); // entries which has value less than 3
        final TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        final PagingPredicate predicate = new PagingPredicate(lessEqual, comparator, pageSize); //pageSize = 5

        Collection<Integer> values = map.values(predicate);
        assertEquals(4, values.size());
        final Iterator<Integer> iter = values.iterator();
        assertEquals(0, iter.next().intValue());
        assertEquals(1, iter.next().intValue());
        assertEquals(2, iter.next().intValue());
        assertEquals(3, iter.next().intValue());
        assertFalse(iter.hasNext());

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());

        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(0, values.size());
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
