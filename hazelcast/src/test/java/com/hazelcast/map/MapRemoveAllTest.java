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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapRemoveAllTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final int MAP_SIZE = 1000;
    private static final int NODE_COUNT = 3;

    private HazelcastInstance member;
    private HazelcastInstance[] instances;

    @Before
    public void setUp() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(getConfig());
        member = instances[1];
    }

    @Test
    public void throws_exception_whenPredicateNull() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("predicate cannot be null");

        IMap<Integer, Integer> map = member.getMap("test");
        map.removeAll(null);
    }

    @Test
    public void removes_all_entries_whenPredicateTrue() {
        IMap<Integer, Integer> map = member.getMap("test");
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(Predicates.alwaysTrue());

        assertEquals(0, map.size());
    }

    @Test
    public void removes_no_entries_whenPredicateFalse() {
        IMap<Integer, Integer> map = member.getMap("test");
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(Predicates.alwaysFalse());

        assertEquals(MAP_SIZE, map.size());
    }

    @Test
    public void removes_odd_keys_whenPredicateOdd() {
        IMap<Integer, Integer> map = member.getMap("test");
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(new OddFinderPredicate());

        assertEquals(500, map.size());
    }

    @Test
    public void removes_same_number_of_entries_from_owner_and_backup() {
        String mapName = "test";
        IMap<Integer, Integer> map = member.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.removeAll(Predicates.sql("__key >= 100"));

        waitAllForSafeState(instances);

        long totalOwnedEntryCount = 0;
        for (HazelcastInstance instance : instances) {
            LocalMapStats localMapStats = instance.getMap(mapName).getLocalMapStats();
            totalOwnedEntryCount += localMapStats.getOwnedEntryCount();
        }

        long totalBackupEntryCount = 0;
        for (HazelcastInstance instance : instances) {
            LocalMapStats localMapStats = instance.getMap(mapName).getLocalMapStats();
            totalBackupEntryCount += localMapStats.getBackupEntryCount();
        }

        assertEquals(100, totalOwnedEntryCount);
        assertEquals(100, totalBackupEntryCount);
    }

    // see https://github.com/hazelcast/hazelcast/issues/15515
    @Test
    public void removeAll_doesNotTouchNonMatchingEntries() {
        String mapName = "test";
        IMap<Integer, Integer> map = member.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        long expirationTime = map.getEntryView(1).getExpirationTime();

        map.removeAll(Predicates.sql("__key >= 100"));
        assertEquals("Expiration time of non-matching key 1 should be same as original",
                expirationTime, map.getEntryView(1).getExpirationTime());
    }

    private static final class OddFinderPredicate implements Predicate<Integer, Integer> {
        @Override
        public boolean apply(Map.Entry<Integer, Integer> mapEntry) {
            return mapEntry.getKey() % 2 != 0;
        }
    }
}
