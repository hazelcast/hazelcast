/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.PartitionsPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionsPredicateTest extends HazelcastTestSupport {

    private static final int PARTITIONS = 10;
    private static final int ITEMS_PER_PARTITION = 20;

    private HazelcastInstance local;
    private IMap<String, Integer> map;
    private IMap<String, Integer> aggMap;

    private String partitionKey1;
    private int partitionId1;
    private String partitionKey2;
    private int partitionId2;
    private String partitionKey3;
    private int partitionId3;
    private List<String> partitionKeys;

    private Predicate<String, Integer> predicate;
    private Predicate<String, Integer> aggPredicate;

    @Before
    public void setUp() {
        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + PARTITIONS);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        local = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance remote = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        map = local.getMap(randomString());
        aggMap = local.getMap(randomString());
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
        partitionKeys = Arrays.asList(partitionKey1, partitionKey2, partitionKey3);


        predicate = Predicates.partitionsPredicate(partitionKeys, Predicates.alwaysTrue());
        aggPredicate = Predicates.partitionsPredicate(partitionKeys, Predicates.or(Predicates.equal("this", partitionId1), Predicates.equal("this", partitionId2), Predicates.equal("this", partitionId3)));


    }

    @Test
    public void values() {
        Collection<Integer> values = map.values(predicate);
        assertEquals(ITEMS_PER_PARTITION * 2, values.size());
        for (Integer value : values) {
            assertTrue(value.intValue() == partitionId1 || value.intValue() == partitionId2);
        }
    }

    @Test
    public void keySet() {
        Collection<String> keys = map.keySet(predicate);

        assertEquals(ITEMS_PER_PARTITION * 2, keys.size());
        for (String key : keys) {
            int partition =  local.getPartitionService().getPartition(key).getPartitionId();
            assertTrue(partition == partitionId1 || partition == partitionId2);
        }
    }

    @Test
    public void entries() {
        Collection<Map.Entry<String, Integer>> entries = map.entrySet(predicate);

        assertEquals(ITEMS_PER_PARTITION * 2, entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            int partition =  local.getPartitionService().getPartition(entry.getKey()).getPartitionId();
            int value = entry.getValue().intValue();
            assertTrue(partition == partitionId1 || partition == partitionId2);
            assertEquals(partition, value);
        }
    }


    @Test
    public void aggregate() {
        Long aggregate = aggMap.aggregate(Aggregators.count(), aggPredicate);
        assertEquals(4, aggregate.longValue()); //matches two per partition
    }

    @Test
    public void project() {
        Collection<Integer> values = aggMap.project(Projections.
                singleAttribute("this"), aggPredicate);
        assertEquals(4, values.size()); //matches two per partition
        assertTrue(values.contains(partitionId1));
        assertTrue(values.contains(partitionId2));
    }

    @Test
    public void executeOnEntries() {
        PartitionsPredicate<String, Integer> lessThan10pp = Predicates.partitionsPredicate(partitionKeys,
                Predicates.lessThan("this", 10));
        Map<String, Integer> result = aggMap.executeOnEntries(new EntryNoop<>(), lessThan10pp);

        assertEquals(20, result.size());
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            int partition = local.getPartitionService().getPartition(entry.getKey()).getPartitionId();
            assertTrue(partition == partitionId1 || partition == partitionId2);
            assertEquals(-1, (int) entry.getValue());
        }
    }

    @Test
    public void removeAll() {
        int sizeBefore = map.size();
        int partitionSizeBefore = map.keySet(predicate).size();

        map.removeAll(predicate);
        assertEquals(sizeBefore - partitionSizeBefore, map.size());
        assertEquals(0, map.keySet(predicate).size());

        for (int i = 0; i < ITEMS_PER_PARTITION; ++i) {
            String key;
            do {
                key = generateKeyForPartition(local, partitionId1);
            } while (map.containsKey(key));

            map.put(key, i);
        }
        for (int i = 0; i < ITEMS_PER_PARTITION; ++i) {
            String key;
            do {
                key = generateKeyForPartition(local, partitionId2);
            } while (map.containsKey(key));

            map.put(key, i);
        }
        sizeBefore = map.size();
        partitionSizeBefore = map.keySet(predicate).size();
        assertEquals(ITEMS_PER_PARTITION * 2, partitionSizeBefore);
        map.removeAll(Predicates.partitionsPredicate(partitionKeys, Predicates.equal("this", ITEMS_PER_PARTITION - 1)));
        assertEquals(sizeBefore - 2, map.size());
        assertEquals(partitionSizeBefore - 2, map.keySet(predicate).size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void apply() {
        assertTrue(predicate.apply(null));
    }

    @Test
    public void testToString() {
        assertContains(predicate.toString(), "PartitionsPredicate");
        assertContains(predicate.toString(), "partitionKeys=");
        assertContains(predicate.toString(), partitionKey1);
        assertContains(predicate.toString(), partitionKey2);
        assertContains(predicate.toString(), partitionKey3);
    }

    @Test
    public void testSerialization() {
        SerializationService serializationService = getSerializationService(local);
        Data serialized = serializationService.toData(predicate);
        PartitionsPredicate deserialized = serializationService.toObject(serialized);

        assertEquals(partitionKeys, deserialized.getPartitionKeys());
        assertEquals(Predicates.alwaysTrue(), deserialized.getTarget());
    }


    @Test
    public void testPartitionsGetExecutedCorrectNumberOfTimes() {
         PartitionsPredicate<String, Integer> lessThan10pp = Predicates.partitionsPredicate(partitionKeys,
                 Predicates.lessThan("this", 10));
         Map<String, Integer> result = aggMap.aggregate(new CountExecutions(), lessThan10pp);

         assertEquals(20, result.size());
         //each entry should be executed once for each partition.
         for (Map.Entry<String, Integer> entry : result.entrySet()) {
             int partition = local.getPartitionService().getPartition(entry.getKey()).getPartitionId();
             assertTrue(partition == partitionId1 || partition == partitionId2);
             assertEquals(1, (int) entry.getValue());
         }

    }


    private static class EntryNoop<K, V> implements EntryProcessor<K, V, Integer> {
        @Override
        public Integer process(Map.Entry<K, V> entry) {
            return -1;
        }
    }

    private static class CountExecutions implements Aggregator<Map.Entry<String, Integer>, Map<String, Integer>> {
        private static Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

        private Map<String, Integer> counts = new HashMap<>();

        @Override
        public void accumulate(Entry<String, Integer> input) {
            AtomicInteger counter = counters.computeIfAbsent(input.getKey(), __ -> new AtomicInteger());
            counts.compute(input.getKey(), (k, v) -> counter.incrementAndGet());
        }

        @Override
        public void combine(Aggregator aggregator) {
            CountExecutions countAggregator = (CountExecutions) aggregator;
            countAggregator.counts.forEach((key, value) -> {
                counts.compute(key, (__, current) -> {
                    if (current == null) {
                        current = 0;
                    }
                    return value + current;
                });
            });

        }

        @Override
        public Map<String, Integer> aggregate() {
            return counts;
        }
    }
}
