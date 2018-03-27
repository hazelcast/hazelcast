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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapRemoveAllTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int MAP_SIZE = 1000;
    private static final int NODE_COUNT = 3;

    private HazelcastInstance member;

    @Before
    public void setUp() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        HazelcastInstance[] instances = factory.newInstances();
        member = instances[1];
    }

    @Test
    public void throws_exception_whenPredicateNull() throws Exception {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("predicate cannot be null");

        IMap<Integer, Integer> map = member.getMap("test");
        map.removeAll(null);
    }

    @Test
    public void removes_all_entries_whenPredicateTrue() throws Exception {
        IMap<Integer, Integer> map = member.getMap("test");
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(TruePredicate.INSTANCE);

        assertEquals(0, map.size());
    }

    @Test
    public void removes_no_entries_whenPredicateFalse() throws Exception {
        IMap<Integer, Integer> map = member.getMap("test");
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(FalsePredicate.INSTANCE);

        assertEquals(MAP_SIZE, map.size());
    }

    @Test
    public void removes_odd_keys_whenPredicateOdd() throws Exception {
        IMap<Integer, Integer> map = member.getMap("test");
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(new OddFinderPredicate());

        assertEquals(500, map.size());
    }

    private static final class OddFinderPredicate implements Predicate<Integer, Integer> {
        @Override
        public boolean apply(Map.Entry<Integer, Integer> mapEntry) {
            return mapEntry.getKey() % 2 != 0;
        }
    }
}
