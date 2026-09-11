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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.server.OperationPacketFilter;
import com.hazelcast.internal.server.PacketFilter;
import com.hazelcast.map.MapStore;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.PacketFiltersUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class TransactionOrderTest extends HazelcastTestSupport {

    private static final String PARTITION_KEY = "1";
    private static final String MAP_NAME = "map1";

    private static final int TEST_SIZE = 20;
    private static final int POLL_TIMEOUT_SECONDS = 15;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance node1;
    private HazelcastInstance node2;

    private BlockingQueue<String> eventQueue;

    @Before
    public void setup() {
        eventQueue = new ArrayBlockingQueue<>(TEST_SIZE);

        factory = createHazelcastInstanceFactory(2);
        node1 = factory.newHazelcastInstance(buildConfig(MAP_NAME, "node1"));
        node2 = factory.newHazelcastInstance(buildConfig(MAP_NAME, "node2"));
        assertClusterSizeEventually(2, node1, node2);
    }

    @After
    public void shutdown() {
        PacketFiltersUtil.resetPacketFiltersFrom(node1);
        PacketFiltersUtil.resetPacketFiltersFrom(node2);
        eventQueue.clear();
        factory.shutdownAll();
    }

    @Test
    public void multiNode_singlePartitionKey_preservesInsertionOrder()
            throws InterruptedException {
        InternalSerializationService ss = getNode(node1).getSerializationService();
        RejectEveryNthFilter filter = new RejectEveryNthFilter(ss, 7);

        for (HazelcastInstance node : Arrays.asList(node1, node2)) {
            node.executeTransaction(context -> {
                TransactionalMap<Object, Object> txMap = context.getMap(MAP_NAME);
                for (int i = 0; i < TEST_SIZE; i++) {
                    txMap.put(i + "@" + PARTITION_KEY, String.valueOf(i));
                }
                // Apply packet filter mid-transaction to simulate glitch
                PacketFiltersUtil.setCustomFilter(node, filter);
                return null;
            });

            for (int i = 0; i < TEST_SIZE; i++) {
                String key = eventQueue.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertEquals(i + "@" + PARTITION_KEY, key);
            }
        }
    }

    private Config buildConfig(String mapName, String nodeName) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new SimpleMapStore(eventQueue, nodeName))
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteBatchSize(1);

        MapConfig mapConfig = new MapConfig(mapName)
                .setBackupCount(0)
                .setAsyncBackupCount(0)
                .setMapStoreConfig(mapStoreConfig)
                .setPartitioningStrategyConfig(
                        new com.hazelcast.config.PartitioningStrategyConfig(
                                com.hazelcast.partition.strategy.StringPartitioningStrategy.INSTANCE));

        Config config = new Config().addMapConfig(mapConfig);
        config.setProperty("hazelcast.event.thread.count", "1");
        return config;
    }

    public static class SimpleMapStore implements MapStore<String, String> {
        private final BlockingQueue<String> eventQueue;
        private final String nodeName;

        public SimpleMapStore(BlockingQueue<String> eventQueue, String nodeName) {
            this.eventQueue = eventQueue;
            this.nodeName = nodeName;
        }

        @Override
        public void store(String key, String value) {
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
            return Collections.emptyMap();
        }

        @Override
        public Iterable<String> loadAllKeys() {
             return Collections.emptyList();
        }
    }

    public static class RejectEveryNthFilter extends OperationPacketFilter {

        private final AtomicInteger counter = new AtomicInteger(0);
        private final int everyN;

        RejectEveryNthFilter(InternalSerializationService ss, int everyN) {
            super(ss);
            this.everyN = everyN;
        }

        @Override
        protected PacketFilter.Action filterOperation(Address endpoint, int factory, int type) {
            int count = counter.incrementAndGet();
            return (count % everyN == 0) ? PacketFilter.Action.DELAY : PacketFilter.Action.ALLOW;
        }
    }
}
