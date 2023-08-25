/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class MultiPartitionPredicateTestSupport extends HazelcastTestSupport {

    private static final int PARTITIONS = 10;
    private static final int ITEMS_PER_PARTITION = 20;

    private TestHazelcastFactory factory;
    private String mapName;
    private String aggMapName;

    private String partitionKey1;
    private int partitionId1;
    private String partitionKey2;
    private int partitionId2;
    private String partitionKey3;
    private int partitionId3;
    private Set<String> partitionKeys;

    private Predicate<String, Integer> predicate;
    private Predicate<String, Integer> aggPredicate;

    protected abstract IMap<String, Integer> getMap(String name);
    protected abstract HazelcastInstance getInstance();
    protected abstract void setupInternal();
    protected TestHazelcastFactory getFactory() {
        return factory;
    }
    protected Set<String> getPartitionKeys() {
        return partitionKeys;
    }
    protected Predicate getPredicate() {
        return predicate;
    }


    @Before
    public void setUp() {
        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + PARTITIONS);

        factory = new TestHazelcastFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);
        mapName = randomString();
        aggMapName = randomString();
        IMap<String, Integer> map = local.getMap(mapName);
        IMap<String, Integer> aggMap = local.getMap(aggMapName);
        for (int p = 0; p < PARTITIONS; p++) {
            for (int k = 0; k < ITEMS_PER_PARTITION; k++) {
                map.put(generateKeyForPartition(local, p), p);
                aggMap.put(generateKeyForPartition(local, p), k);
            }
        }

        partitionKey1 = randomString();
        partitionId1 = local.getPartitionService().getPartition(partitionKey1).getPartitionId();
        while (true) {
            partitionKey2 = randomString();
            partitionId2 = local.getPartitionService().getPartition(partitionKey2).getPartitionId();
            if (partitionId2 != partitionId1) {
                break;
            }
        }
        while (true) {
            partitionKey3 = randomString();
            partitionId3 = local.getPartitionService().getPartition(partitionKey3).getPartitionId();
            if (partitionId3 == partitionId1) {
                break;
            }
        }
        partitionKeys = new HashSet<>(Arrays.asList(partitionKey1, partitionKey2, partitionKey3));


        predicate = Predicates.multiPartitionPredicate(partitionKeys, Predicates.alwaysTrue());
        aggPredicate = Predicates.multiPartitionPredicate(partitionKeys, Predicates.or(Predicates.equal("this", partitionId1), Predicates.equal("this", partitionId2), Predicates.equal("this", partitionId3)));
        setupInternal();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void values() {
        Collection<Integer> values = getMap(mapName).values(predicate);
        assertEquals(ITEMS_PER_PARTITION * 2, values.size());
        for (Integer value : values) {
            assertTrue(value.intValue() == partitionId1 || value.intValue() == partitionId2);
        }
    }

    @Test
    public void keySet() {
        Collection<String> keys = getMap(mapName).keySet(predicate);

        assertEquals(ITEMS_PER_PARTITION * 2, keys.size());
        for (String key : keys) {
            int partition =  getInstance().getPartitionService().getPartition(key).getPartitionId();
            assertTrue(partition == partitionId1 || partition == partitionId2);
        }
    }

    @Test
    public void entries() {
        Collection<Map.Entry<String, Integer>> entries = getMap(mapName).entrySet(predicate);

        assertEquals(ITEMS_PER_PARTITION * 2, entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            int partition =  getInstance().getPartitionService().getPartition(entry.getKey()).getPartitionId();
            int value = entry.getValue().intValue();
            assertTrue(partition == partitionId1 || partition == partitionId2);
            assertEquals(partition, value);
        }
    }


    @Test
    public void aggregate() {
        Long aggregate = getMap(aggMapName).aggregate(Aggregators.count(), aggPredicate);
        assertEquals(4, aggregate.longValue()); //matches two per partition
    }

    @Test
    public void project() {
        Collection<Integer> values = getMap(aggMapName).project(Projections.
                singleAttribute("this"), aggPredicate);
        assertEquals(4, values.size()); //matches two per partition
        assertTrue(values.contains(partitionId1));
        assertTrue(values.contains(partitionId2));
    }

    @Test
    public void executeOnEntries() {
        PartitionPredicate<String, Integer> lessThan10pp = Predicates.multiPartitionPredicate(partitionKeys,
                Predicates.lessThan("this", 10));
        Map<String, Integer> result = getMap(aggMapName).executeOnEntries(new EntryNoop<>(), lessThan10pp);

        assertEquals(20, result.size());
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            int partition = getInstance().getPartitionService().getPartition(entry.getKey()).getPartitionId();
            assertTrue(partition == partitionId1 || partition == partitionId2);
            assertEquals(-1, (int) entry.getValue());
        }
    }

    @Test
    public void removeAll() {
        IMap<String, Integer> map = getMap(mapName);
        int sizeBefore = map.size();
        int partitionSizeBefore = map.keySet(predicate).size();

        map.removeAll(predicate);
        assertEquals(sizeBefore - partitionSizeBefore, map.size());
        assertEquals(0, map.keySet(predicate).size());

        for (int i = 0; i < ITEMS_PER_PARTITION; ++i) {
            String key;
            do {
                key = generateKeyForPartition(getInstance(), partitionId1);
            } while (map.containsKey(key));

            map.put(key, i);
        }
        for (int i = 0; i < ITEMS_PER_PARTITION; ++i) {
            String key;
            do {
                key = generateKeyForPartition(getInstance(), partitionId2);
            } while (map.containsKey(key));

            map.put(key, i);
        }
        sizeBefore = map.size();
        partitionSizeBefore = map.keySet(predicate).size();
        assertEquals(ITEMS_PER_PARTITION * 2, partitionSizeBefore);
        map.removeAll(Predicates.multiPartitionPredicate(partitionKeys, Predicates.equal("this", ITEMS_PER_PARTITION - 1)));
        assertEquals(sizeBefore - 2, map.size());
        assertEquals(partitionSizeBefore - 2, map.keySet(predicate).size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void apply() {
        assertTrue(predicate.apply(null));
    }

    @Test
    public void testToString() {
        assertContains(predicate.toString(), "MultiPartitionPredicate");
        assertContains(predicate.toString(), "partitionKeys=[");
        assertContains(predicate.toString(), partitionKey1);
        assertContains(predicate.toString(), partitionKey2);
        assertContains(predicate.toString(), partitionKey3);
    }

    @Test
    public void multiPartitionPredicateWithNullSetThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> Predicates.multiPartitionPredicate(null, a -> true));
    }

    @Test
    public void multiPartitionPredicateWithEmptySetThrowsNullPointerException() {
        assertThrows(IllegalArgumentException.class, () -> Predicates.multiPartitionPredicate(Collections.emptySet(), a -> true));
    }

    @Test
    public void multiPartitionPredicateWithNullPredicateThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> Predicates.multiPartitionPredicate(Collections.singleton("foo"), null));
    }

    private static class EntryNoop<K, V> implements EntryProcessor<K, V, Integer> {
        @Override
        public Integer process(Map.Entry<K, V> entry) {
            return -1;
        }
    }
}
