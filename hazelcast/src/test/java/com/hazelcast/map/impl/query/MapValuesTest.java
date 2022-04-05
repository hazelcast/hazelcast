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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.Predicates.partitionPredicate;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapValuesTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private SerializationService serializationService;

    @Before
    public void setup() {
        Config config = regularInstanceConfig();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        config.getMapConfig("indexed").addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        instance = createHazelcastInstance(config);
        map = instance.getMap(randomName());
        serializationService = getSerializationService(instance);
    }

    @Test(expected = NullPointerException.class)
    public void whenPredicateNull() {
        map.values(null);
    }

    @Test
    public void whenMapEmpty() {
        Collection<String> result = map.values(Predicates.alwaysTrue());

        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Collection<String> result = map.values();

        assertEquals(3, result.size());
        assertContains(result, "a");
        assertContains(result, "b");
        assertContains(result, "c");
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Collection<String> result = map.values(Predicates.alwaysTrue());

        assertEquals(3, result.size());
        assertContains(result, "a");
        assertContains(result, "b");
        assertContains(result, "c");
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Collection<String> result = map.values(new GoodPredicate());

        assertEquals(2, result.size());
        assertContains(result, "good1");
        assertContains(result, "good2");
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

        Collection<String> result = ((MapProxyImpl<String, String>) map).values(Predicates.alwaysTrue(), partitionSubset);
        assertEquals(matchingKeys, result);
    }

    @Test
    public void whenSelectingPartitionSubset_withIndex() {
        PartitionIdSet partitionSubset = new PartitionIdSet(4, asList(1, 3));
        Set<String> matchingKeys = new HashSet<>();
        map = instance.getMap("indexed");
        for (int i = 0; i < 5; i++) {
            String key = generateKeyForPartition(instance, i);
            map.put(key, key);
            if (partitionSubset.contains(i)) {
                matchingKeys.add(key);
            }
        }

        // "" is sorted before any non-null string - internally all items from all partitions are added
        // to the result (because the index is global), but there's a code that eliminates partitions not
        // in the subset - this test aims to test that code
        Predicate<String, String> predicate = Predicates.greaterThan("this", "");
        Collection<String> result =
                ((MapProxyImpl<String, String>) map).values(predicate, partitionSubset);
        assertEquals(2, result.size());
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

        Collection<String> result = ((MapProxyImpl<String, String>) map)
                .values(partitionPredicate(key1, Predicates.alwaysTrue()), partitionSubset);
        assertEquals(Collections.singleton(key1), result);
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Collection<String> entries = map.values(Predicates.alwaysTrue());

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, entries);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        // there should only be a value, no key
        assertNull(row.getKey());
        assertEquals(serializationService.toData("a"), row.getValue());
    }

    @Test
    public void testSerializationServiceNullClassLoaderProblem() {
        // if the classloader is null the following call throws NullPointerException
        map.values(new InstanceOfPredicate(SampleTestObjects.PortableEmployee.class));
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }
    }
}
