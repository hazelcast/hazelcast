/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapStore;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class TransactionOrderTest extends HazelcastTestSupport {
    private static final int TEST_SIZE = 100;

    private HazelcastInstance hz;
    private BlockingQueue<String> eventQueue;

    @Before
    public void setup() {
        eventQueue = new ArrayBlockingQueue<>(TEST_SIZE);
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new SimpleMapStore(eventQueue))
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteBatchSize(1);
        MapConfig mapConfig = new MapConfig("map1").setMapStoreConfig(mapStoreConfig);
        PartitioningStrategyConfig partitioningStrategyConfig = new PartitioningStrategyConfig(StringPartitioningStrategy.INSTANCE);
        mapConfig.setPartitioningStrategyConfig(partitioningStrategyConfig);
        Config config = new Config().addMapConfig(mapConfig);

        hz = createHazelcastInstance(config);
    }

    @After
    public void shutdown() {
        hz.shutdown();
    }

    @Test
    public void verifyTransactionOrder() {
        final String PARTITION_KEY = "GOOGL";

        hz.executeTransaction(context -> {
            TransactionalMap<Object, Object> txMap = context.getMap("map1");
            for (int i = 0; i < TEST_SIZE; i++) {
                txMap.put(i + "@" + PARTITION_KEY, String.valueOf(i));
            }
            return null;
        });

        for (int i = 0; i < TEST_SIZE; i++) {
            String key = eventQueue.poll();
            Assert.assertEquals(i + "@" + PARTITION_KEY, key);
        }
    }

    public static class SimpleMapStore implements MapStore<String, String> {

        private final BlockingQueue<String> eventQueue;

        public SimpleMapStore(BlockingQueue<String> eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void store(String key, String value) {
            // simulating event publishing
            this.eventQueue.add(key);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            map.entrySet().forEach(entry -> store(entry.getKey(), entry.getValue()));
        }

        @Override
        public void delete(String key) {
        }

        @Override
        public void deleteAll(Collection<String> keys) {
        }

        @Override
        public String load(String key) {
            return null;
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            return null;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            return null;
        }
    }
}
