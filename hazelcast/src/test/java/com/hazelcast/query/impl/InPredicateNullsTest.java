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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InPredicateNullsTest extends HazelcastTestSupport {

    private IMap<Integer, Value> map;

    @Before
    public void before() {
        map = createHazelcastInstance(smallInstanceConfig()).getMap("map");
    }

    @Test
    public void testWithoutIndex() {
        verifyQueries();
    }

    @Test
    public void testWithIndex() {
        map.addIndex(IndexType.HASH, "value");
        verifyQueries();
    }

    private void verifyQueries() {
        verifyQuery(Predicates.in("value", (Comparable) null));
        verifyQuery(Predicates.in("value", 3));
        verifyQuery(Predicates.in("value", null, 3));

        map.put(3, new Value(3));
        verifyQuery(Predicates.in("value", (Comparable) null));
        verifyQuery(Predicates.in("value", 3), 3);
        verifyQuery(Predicates.in("value", null, 3), 3);

        map.put(0, new Value(null));
        verifyQuery(Predicates.in("value", (Comparable) null), 0);
        verifyQuery(Predicates.in("value", 3), 3);
        verifyQuery(Predicates.in("value", null, 3), 3, 0);

        map.put(9, new Value(9));
        verifyQuery(Predicates.in("value", (Comparable) null), 0);
        verifyQuery(Predicates.in("value", 3), 3);
        verifyQuery(Predicates.in("value", null, 3), 3, 0);

        for (int i = 10; i < 20; ++i) {
            map.put(i, new Value(i));
        }
        verifyQuery(Predicates.in("value", (Comparable) null), 0);
        verifyQuery(Predicates.in("value", 3), 3);
        verifyQuery(Predicates.in("value", null, 3), 3, 0);

        map.put(100, new Value(null));
        verifyQuery(Predicates.in("value", (Comparable) null), 0, 100);
        verifyQuery(Predicates.in("value", 3), 3);
        verifyQuery(Predicates.in("value", null, 3), 3, 0, 100);
    }

    private void verifyQuery(Predicate<Integer, Value> predicate, int... expected) {
        Collection<Integer> actual = map.keySet(predicate);
        Set<Integer> expectedSet = new HashSet<>();
        for (int i : expected) {
            expectedSet.add(i);
        }
        assertCollection(expectedSet, actual);
    }

    static class Value implements Serializable {

        final Integer value;

        Value(Integer value) {
            this.value = value;
        }

    }

}
