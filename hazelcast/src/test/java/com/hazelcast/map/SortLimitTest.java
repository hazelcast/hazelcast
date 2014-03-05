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
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * @ali 05/12/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SortLimitTest extends HazelcastTestSupport {

    @Test
    public void testLocalPaging() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map1 = instance1.getMap("testSort");
        final IMap<Integer, Integer> map2 = instance2.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map1.put(i+10, i);
        }

        final PagingPredicate predicate1 = new PagingPredicate(pageSize);
        Set<Integer> keySet = map1.localKeySet(predicate1);

        int value = 9;
        Set<Integer> whole = new HashSet<Integer>(size);
        while (keySet.size() > 0 ) {
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
        while (keySet.size() > 0 ) {
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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map = instance1.getMap("testSort");
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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map = instance1.getMap("testSort");
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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map = instance1.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        Integer value = 8;
        final Predicate lessEqual = Predicates.lessEqual("this", value);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, new TestComparator(false), pageSize);

        Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());
        for(Integer val:values){
            assertEquals(value--, val);
        }


        predicate.nextPage();
        assertEquals(value + 1, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertEquals(4, values.size());for(Integer val : values){assertEquals(value--, val);}

        predicate.nextPage();
        assertEquals(value + 1, predicate.getAnchor().getValue());
        assertEquals(0, predicate.getAnchor().getValue());
        values = map.values(predicate);
        assertEquals(0, values.size());

    }

    @Test
    public void testKeyPaging() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map = instance1.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(size - i, i);
        }
        Integer value = 8;
        final Predicate lessEqual = Predicates.lessEqual("this", value);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, new TestComparator(true), pageSize);

        Set<Integer> keySet = map.keySet(predicate);
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

    static class TestComparator implements Comparator<Integer>, Serializable {

        int ascending = 1;

        TestComparator() {
        }

        TestComparator(boolean ascending) {
            this.ascending = ascending ? 1 : -1;
        }

        public int compare(Integer o1, Integer o2) {
            return (o1 - o2) * ascending;
        }
    }

}
