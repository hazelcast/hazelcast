/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.projection;

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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapProjectionPagingTest extends HazelcastTestSupport {

    private static int size = 50;
    private static int pageSize = 5;

    private IMap<Integer, Integer> map;

    @Before
    public void setup() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        map = local.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
    }

    @Test
    public void testWithoutAnchor() {
        Projection<Map.Entry<Integer, Integer>, Integer> projection = new TestProjection();
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);
        predicate.nextPage();
        predicate.nextPage();
        Collection<Integer> values = map.project(projection, predicate);
        assertEquals(5, values.size());
        Integer value = 10;
        for (Integer val : values) {
            assertEquals(value++, val);
        }
        predicate.previousPage();

        values = map.project(projection, predicate);
        assertEquals(5, values.size());
        value = 5;
        for (Integer val : values) {
            assertEquals(value++, val);
        }
        predicate.previousPage();

        values = map.project(projection, predicate);
        assertEquals(5, values.size());
        value = 0;
        for (Integer val : values) {
            assertEquals(value++, val);
        }
    }

    @Test
    public void testPagingWithoutFilteringAndComparator() {
        Projection<Map.Entry<Integer, Integer>, Integer> projection = new TestProjection();
        Set<Integer> set = new HashSet<Integer>();
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(pageSize);

        Collection<Integer> values = map.project(projection, predicate);
        while (values.size() > 0) {
            assertEquals(pageSize, values.size());
            set.addAll(values);

            predicate.nextPage();
            values = map.project(projection, predicate);
        }

        assertEquals(size, set.size());
    }

    @Test
    public void testPagingWithFilteringAndComparator() {
        Projection<Map.Entry<Integer, Integer>, Integer> projection = new TestProjection();
        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8);
        TestComparator comparator = new TestComparator(false, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize);

        Collection<Integer> values = map.project(projection, predicate);
        assertIterableEquals(values, 8, 7, 6, 5, 4);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertIterableEquals(values, 3, 2, 1, 0);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testPagingWithFilteringAndComparatorAndIndex() {
        Projection<Map.Entry<Integer, Integer>, Integer> projection = new TestProjection();
        map.addIndex("this", true);
        Predicate<Integer, Integer> lessEqual = Predicates.between("this", 12, 20);
        TestComparator comparator = new TestComparator(false, IterationType.VALUE);
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize);

        Collection<Integer> values = map.project(projection, predicate);
        assertIterableEquals(values, 20, 19, 18, 17, 16);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertIterableEquals(values, 15, 14, 13, 12);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertEquals(0, values.size());
    }

    @Test
    public void testEqualValuesPaging() {
        Projection<Map.Entry<Integer, Integer>, Integer> projection = new TestProjection();
        // keys[50-99] values[0-49]
        for (int i = size; i < 2 * size; i++) {
            map.put(i, i - size);
        }

        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 8); // entries which has value less than 8
        TestComparator comparator = new TestComparator(true, IterationType.VALUE); //ascending values
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize); //pageSize = 5

        Collection<Integer> values = map.project(projection, predicate);
        assertIterableEquals(values, 0, 0, 1, 1, 2);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertIterableEquals(values, 2, 3, 3, 4, 4);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertIterableEquals(values, 5, 5, 6, 6, 7);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertIterableEquals(values, 7, 8, 8);
    }

    @Test
    public void testNextPageAfterResultSetEmpty() {
        Projection<Map.Entry<Integer, Integer>, Integer> projection = new TestProjection();
        Predicate<Integer, Integer> lessEqual = Predicates.lessEqual("this", 3); // entries which has value less than 3
        TestComparator comparator = new TestComparator(true, IterationType.VALUE); // ascending values
        PagingPredicate<Integer, Integer> predicate = new PagingPredicate<Integer, Integer>(lessEqual, comparator, pageSize); // pageSize = 5

        Collection<Integer> values = map.project(projection, predicate);
        assertIterableEquals(values, 0, 1, 2, 3);

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertEquals(0, values.size());

        predicate.nextPage();
        values = map.project(projection, predicate);
        assertEquals(0, values.size());
    }

    private static class TestProjection extends Projection<Map.Entry<Integer, Integer>, Integer> {
        @Override
        public Integer transform(Map.Entry<Integer, Integer> input) {
            return input.getValue();
        }
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
