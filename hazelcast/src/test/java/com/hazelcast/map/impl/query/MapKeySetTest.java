/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.Predicates.partitionPredicate;
import static com.hazelcast.test.Accessors.getSerializationService;
import static com.hazelcast.test.TestCollectionUtils.setOf;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapKeySetTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private SerializationService serializationService;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        map = instance.getMap(randomName());
        serializationService = getSerializationService(instance);
    }

    @Test(expected = NullPointerException.class)
    public void whenPredicateNull() {
        map.entrySet(null);
    }

    @Test
    public void whenMapEmpty() {
        Set<String> result = map.keySet(Predicates.alwaysTrue());

        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<String> result = map.keySet();

        assertEquals(setOf("1", "2", "3"), result);
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<String> result = map.keySet(Predicates.alwaysTrue());

        assertEquals(setOf("1", "2", "3"), result);
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Set<String> result = map.keySet(new GoodPredicate());

        assertEquals(setOf("1", "3"), result);
    }

    @Test
    public void whenSelectingPartitionSubset() {
        PartitionIdSet partitionSubset = new PartitionIdSet(4, asList(1, 3));
        Set<String> matchingKeys = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            String key = generateKeyForPartition(instance, i);
            map.put(key, key);
            if (partitionSubset.contains(i)) {
                matchingKeys.add(key);
            }
        }

        Set<String> result = ((MapProxyImpl<String, String>) map).keySet(Predicates.alwaysTrue(), partitionSubset);
        assertEquals(matchingKeys, result);
    }

    @Test
    public void when_selectingPartitionSubset_and_partitionPredicate() {
        PartitionIdSet partitionSubset = new PartitionIdSet(4, asList(1, 3));
        Set<String> matchingKeys = new HashSet<>();
        String key1 = null;
        for (int i = 0; i < 5; i++) {
            String key = generateKeyForPartition(instance, i);
            if (i == 1) {
                key1 = key;
            }
            map.put(key, key);
            if (partitionSubset.contains(i)) {
                matchingKeys.add(key);
            }
        }

        Set<String> result = ((MapProxyImpl<String, String>) map)
                .keySet(partitionPredicate(key1, Predicates.alwaysTrue()), partitionSubset);
        assertEquals(Collections.singleton(key1), result);
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Set<String> entries = map.keySet(Predicates.alwaysTrue());

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, entries);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        assertEquals(serializationService.toData("1"), row.getKey());
        assertNull(row.getValue());
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }
    }
}
