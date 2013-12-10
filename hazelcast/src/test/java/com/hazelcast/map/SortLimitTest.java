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

/**
 * @ali 05/12/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SortLimitTest extends HazelcastTestSupport {

    @Test
    public void testPagingWithoutFilteringAndComparator(){
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        final IMap<Integer, Integer> map = instance1.getMap("testSort");
        final int size = 50;
        final int pageSize = 5;
        for (int i=0; i<size; i++) {
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
        for (int i=0; i<size; i++) {
            map.put(i, i);
        }
        Integer value = 8;
        final Predicate lessEqual = Predicates.lessEqual("this", value);
        final PagingPredicate predicate = new PagingPredicate(lessEqual, new TestComparator(false), pageSize);

        Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());


        for (Integer val : values) {
            assertEquals(value--, val);
        }

        assertEquals(value+1, predicate.getAnchor());

        predicate.nextPage();

        values = map.values(predicate);
        assertEquals(4, values.size());

        for (Integer val : values) {
            assertEquals(value--, val);
        }
        assertEquals(value + 1, predicate.getAnchor());
        assertEquals(0, predicate.getAnchor());

        predicate.nextPage();

        values = map.values(predicate);
        assertEquals(0, values.size());

        assertEquals(value+1, predicate.getAnchor());
        assertEquals(0, predicate.getAnchor());
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
