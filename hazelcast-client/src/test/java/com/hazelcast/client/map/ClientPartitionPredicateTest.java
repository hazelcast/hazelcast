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

package com.hazelcast.client.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPartitionPredicateTest extends HazelcastTestSupport {

    private static final int PARTITIONS = 10;
    private static final int ITEMS_PER_PARTITION = 20;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server;
    private HazelcastInstance client;
    private IMap<String, Integer> map;

    private String partitionKey;
    private int partitionId;

    private Predicate<String, Integer> predicate;

    @Before
    public void setUp() {
        Config config = getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), "" + PARTITIONS);

        server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance remote = hazelcastFactory.newHazelcastInstance(config);
        warmUpPartitions(server, remote);

        client = hazelcastFactory.newHazelcastClient();

        String mapName = randomString();
        map = client.getMap(mapName);

        for (int p = 0; p < PARTITIONS; p++) {
            for (int k = 0; k < ITEMS_PER_PARTITION; k++) {
                map.put(generateKeyForPartition(server, p), p);
            }
        }
        partitionKey = randomString();
        partitionId = server.getPartitionService().getPartition(partitionKey).getPartitionId();
        predicate = new PartitionPredicate<String, Integer>(partitionKey, TruePredicate.INSTANCE);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void values_withPagingPredicate() {
        PagingPredicate<String, Integer> pagingPredicate = new PagingPredicate<String, Integer>(Predicates.alwaysTrue(), 1);
        predicate = new PartitionPredicate<String, Integer>(randomString(), pagingPredicate);

        for (int i = 0; i < ITEMS_PER_PARTITION; i++) {
            int size = map.values(predicate).size();
            assertEquals(1, size);
            pagingPredicate.nextPage();
        }
        int size = map.values(predicate).size();
        assertEquals(0, size);
    }

    @Test
    public void keys_withPagingPredicate() {
        PagingPredicate<String, Integer> pagingPredicate = new PagingPredicate<String, Integer>(Predicates.alwaysTrue(), 1);
        predicate = new PartitionPredicate<String, Integer>(randomString(), pagingPredicate);

        for (int i = 0; i < ITEMS_PER_PARTITION; i++) {
            int size = map.keySet(predicate).size();
            assertEquals(1, size);
            pagingPredicate.nextPage();
        }
        int size = map.keySet(predicate).size();
        assertEquals(0, size);
    }

    @Test
    public void entries_withPagingPredicate() {
        PagingPredicate<String, Integer> pagingPredicate = new PagingPredicate<String, Integer>(Predicates.alwaysTrue(), 1);
        predicate = new PartitionPredicate<String, Integer>(randomString(), pagingPredicate);

        for (int i = 0; i < ITEMS_PER_PARTITION; i++) {
            int size = map.entrySet(predicate).size();
            assertEquals(1, size);
            pagingPredicate.nextPage();
        }
        int size = map.entrySet(predicate).size();
        assertEquals(0, size);
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
            assertEquals(partitionId, server.getPartitionService().getPartition(key).getPartitionId());
        }
    }

    @Test
    public void entries() {
        Collection<Map.Entry<String, Integer>> entries = map.entrySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            assertEquals(partitionId, server.getPartitionService().getPartition(entry.getKey()).getPartitionId());
            assertEquals(partitionId, entry.getValue().intValue());
        }
    }

    @Test
    public void aggregate() throws Exception {
        int partitionId = 2;
        String keyForPartition = generateKeyForPartition(server, partitionId);
        Predicate partitionPredicate = new PartitionPredicate<String, Integer>(keyForPartition, TruePredicate.INSTANCE);
        Long aggregate = map.aggregate(Aggregators.<Map.Entry<String, Integer>>integerSum(), partitionPredicate);
        Long sum = (long) (partitionId * ITEMS_PER_PARTITION);
        assertEquals(sum, aggregate);
    }

    @Test
    public void executeOnEntries() throws Exception {
        int partitionId = 2;
        String keyForPartition = generateKeyForPartition(server, partitionId);
        Predicate partitionPredicate = new PartitionPredicate<String, Integer>(keyForPartition, TruePredicate.INSTANCE);
        Map<String, Object> entries = map.executeOnEntries(new MyProcessor(), partitionPredicate);
        assertEquals(ITEMS_PER_PARTITION, entries.size());

    }

    static class MyProcessor extends AbstractEntryProcessor<String, Integer> {

        public MyProcessor() {
        }

        @Override
        public Object process(Map.Entry<String, Integer> entry) {
            Integer in = entry.getValue();
            entry.setValue(in * 10);
            return entry;
        }
    }

    @Test
    public void project() throws Exception {
        Predicate partitionPredicate = new PartitionPredicate<String, Integer>(1, TruePredicate.INSTANCE);
        Collection<Integer> collection = map.project(new PrimitiveValueIncrementingProjection(), partitionPredicate);
        assertEquals(ITEMS_PER_PARTITION, collection.size());
    }

    public static class PrimitiveValueIncrementingProjection extends Projection<Map.Entry<String, Integer>, Integer> {
        @Override
        public Integer transform(Map.Entry<String, Integer> input) {
            return input.getValue() + 1;
        }
    }

}
