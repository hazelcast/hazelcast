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

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionPredicateTest extends HazelcastTestSupport {

    private static final int PARTITIONS = 10;
    private static final int ITEMS_PER_PARTITION = 20;

    private HazelcastInstance local;
    private IMap<String, Integer> map;
    private IMap<String, Integer> aggMap;

    private String partitionKey;
    private int partitionId;

    private String localPartitionKey;
    private int localPartitionId;

    private Predicate<String, Integer> predicate;
    private Predicate<String, Integer> aggPredicate;
    private Predicate<String, Integer> localPredicate;

    @Before
    public void setUp() {
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "" + PARTITIONS);

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

        partitionKey = randomString();
        partitionId = local.getPartitionService().getPartition(partitionKey).getPartitionId();

        predicate = new PartitionPredicate<String, Integer>(partitionKey, TruePredicate.INSTANCE);
        aggPredicate = new PartitionPredicate<String, Integer>(partitionKey, Predicates.equal("this", partitionId));

        localPartitionKey = generateKeyOwnedBy(local);
        localPartitionId = local.getPartitionService().getPartition(localPartitionKey).getPartitionId();
        localPredicate = new PartitionPredicate<String, Integer>(localPartitionKey, Predicates.equal("this", localPartitionId));

    }

    @Test
    public void values() {
        Collection<Integer> values = map.values(predicate);

        assertEquals(ITEMS_PER_PARTITION, values.size());
        for (Integer value : values) {
            assertEquals(partitionId, value.intValue());
        }
    }

    @Test
    public void keySet() {
        Collection<String> keys = map.keySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, keys.size());
        for (String key : keys) {
            assertEquals(partitionId, local.getPartitionService().getPartition(key).getPartitionId());
        }
    }

    @Test
    public void entries() {
        Collection<Map.Entry<String, Integer>> entries = map.entrySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            assertEquals(partitionId, local.getPartitionService().getPartition(entry.getKey()).getPartitionId());
            assertEquals(partitionId, entry.getValue().intValue());
        }
    }

    @Test
    public void localKeySet() {
        Collection<String> keys = aggMap.localKeySet(localPredicate);

        assertEquals(1, keys.size());
        for (String key : keys) {
            assertEquals(localPartitionId, local.getPartitionService().getPartition(key).getPartitionId());
        }
    }

    @Test
    public void aggregate() {
        Long aggregate = aggMap.aggregate(Aggregators.<Map.Entry<String, Integer>>count(), aggPredicate);
        assertEquals(1, aggregate.longValue());
    }

    @Test
    public void project() {
        Collection<Integer> values = aggMap.project(Projections.<Map.Entry<String, Integer>, Integer>
                singleAttribute("this"), aggPredicate);
        assertEquals(1, values.size());
        assertEquals(partitionId, values.iterator().next().intValue());
    }

    @Test
    public void executeOnEntries() {
        PartitionPredicate<String, Integer> lessThan10pp = new PartitionPredicate<String, Integer>(partitionKey,
                Predicates.lessThan("this", 10));
        Map<String, Object> result = aggMap.executeOnEntries(new EntryNoop(), lessThan10pp);

        assertEquals(10, result.size());
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            assertEquals(partitionId, local.getPartitionService().getPartition(entry.getKey()).getPartitionId());
            assertEquals(-1, entry.getValue());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void apply() {
        assertTrue(predicate.apply(null));
    }

    @Test
    public void testToString() {
        assertContains(predicate.toString(), "PartitionPredicate");
        assertContains(predicate.toString(), "partitionKey=" + partitionKey);
    }

    @Test
    public void testSerialization() {
        SerializationService serializationService = getSerializationService(local);
        Data serialized = serializationService.toData(predicate);
        PartitionPredicate deserialized = serializationService.toObject(serialized);

        assertEquals(partitionKey, deserialized.getPartitionKey());
        assertEquals(TruePredicate.INSTANCE, deserialized.getTarget());
    }

    private static class EntryNoop extends AbstractEntryProcessor<String, Integer> {
        @Override
        public Object process(Map.Entry<String, Integer> entry) {
            return -1;
        }
    }
}
