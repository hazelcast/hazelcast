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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalQueue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindFlushTest.assertWriteBehindQueuesEmpty;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionsWithWriteBehind_whenNoCoalescingQueueIsFullTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void prepare_step_throws_reached_max_size_exception_when_two_phase() {
        expectedException.expect(TransactionException.class);
        expectedException.expectCause(isA(ReachedMaxSizeException.class));

        String mapName = "map";
        long maxWbqCapacity = 100;

        Config config = getConfig(mapName, maxWbqCapacity);
        HazelcastInstance node = createHazelcastInstance(config);

        TransactionContext context = newTransactionContext(node, TWO_PHASE);
        context.beginTransaction();

        TransactionalMap map = context.getMap(mapName);

        for (int i = 0; i < 101; i++) {
            map.put("item-" + i, "value");
        }
        context.commitTransaction();
    }

    private static TransactionContext newTransactionContext(HazelcastInstance node,
                                                            TransactionOptions.TransactionType twoPhase) {
        TransactionOptions options = new TransactionOptions()
                .setTransactionType(twoPhase);
        return node.newTransactionContext(options);
    }

    @Test
    public void commit_step_does_not_throw_reached_max_size_exception_when_two_phase() {
        String mapName = "map";
        long maxWbqCapacity = 10;
        int keySpace = 10;

        Config config = getConfig(mapName, maxWbqCapacity);
        HazelcastInstance node = createHazelcastInstance(config);

        TransactionContext context = newTransactionContext(node, TWO_PHASE);
        context.beginTransaction();

        TransactionalMap txMap = context.getMap(mapName);

        for (int i = 0; i < keySpace; i++) {
            txMap.remove("item-" + i);
        }

        try {
            context.commitTransaction();
        } catch (TransactionException e) {
            fail("no txn exception is expected here...");
        }

        assertEquals(0, node.getMap(mapName).size());
        assertWriteBehindQueuesEmpty(mapName, Collections.singletonList(node));
        assertEquals(0, getTotalNumOfTxnReservedCapacity(mapName, node));
        assertEquals(0, getNodeWideUsedCapacity(node));
    }

    @Test
    public void rollback_successful_when_prepare_step_throws_exception_when_two_phase() {
        String mapName = "map";
        String queueName = "queue";
        long maxWbqCapacity = 100;

        Config config = getConfig(mapName, maxWbqCapacity);
        HazelcastInstance node = createHazelcastInstance(config);

        TransactionContext context = newTransactionContext(node, TWO_PHASE);
        try {
            context.beginTransaction();

            TransactionalQueue queue = context.getQueue(queueName);
            queue.offer(1);
            queue.offer(2);
            queue.offer(3);

            TransactionalMap map = context.getMap(mapName);
            for (int i = 0; i < 1000; i++) {
                map.put("item-" + i, "value");
            }

            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
        }

        assertEquals(0, node.getQueue(queueName).size());
        assertEquals(0, node.getMap(mapName).size());
    }

    @Test
    public void throws_reached_max_size_exception_when_one_phase() {
        expectedException.expect(ReachedMaxSizeException.class);

        String mapName = "map";
        Config config = getConfig(mapName, 100);
        HazelcastInstance node = createHazelcastInstance(config);

        TransactionContext context = newTransactionContext(node, ONE_PHASE);
        context.beginTransaction();

        TransactionalMap map = context.getMap("map");

        for (int i = 0; i < 101; i++) {
            map.put("item-" + i, "value");
        }

        context.commitTransaction();
    }

    @Ignore
    @Test
    public void rollback_does_not_preserve_latest_state_after_reached_max_size_exception_when_one_phase() {
        String mapName = "map";
        long maxWbqCapacity = 100;

        Config config = getConfig(mapName, maxWbqCapacity);
        HazelcastInstance node = createHazelcastInstance(config);

        TransactionContext context = newTransactionContext(node, ONE_PHASE);
        context.beginTransaction();

        TransactionalMap map = context.getMap(mapName);

        for (int i = 0; i < 101; i++) {
            map.put("item-" + i, "value");
        }

        try {
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
        }

        assertEquals(100, node.getMap(mapName).size());
    }

    private Config getConfig(String mapName, long maxWbqCapacity) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.toString(),
                String.valueOf(maxWbqCapacity));
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .getMapStoreConfig()
                .setEnabled(true)
                .setImplementation(new MapStoreAdapter())
                .setWriteCoalescing(false)
                .setWriteDelaySeconds(6);
        return config;
    }

    @Test
    public void no_exception_after_prepare_phase_when_wbq_is_full() {

    }

    @Test
    public void stable_state_after_rollback() {

    }

    @Test
    public void no_exception_when_wbq_has_empty_slot() {

    }

    @Test
    public void multiple_tx_rollback_successful_when_prepare_step_throws_exception_when_two_phase() {

    }

    @Test
    public void stress() throws InterruptedException {
        final String mapName = "map-name";
        final long maxWbqCapacity = 50;
        final int keySpace = 100;

        final Config config = getConfig(mapName, maxWbqCapacity);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        AtomicBoolean stop = new AtomicBoolean(false);
        int availableProcessors = Math.min(4, RuntimeAvailableProcessors.get());
        ExecutorService executorService = Executors.newFixedThreadPool(availableProcessors);

        for (int i = 0; i < availableProcessors; i++) {
            executorService.submit(() -> {
                while (!stop.get()) {
                    OpType[] values = OpType.values();
                    OpType op = values[RandomPicker.getInt(values.length)];
                    op.doOp(mapName, node1, RandomPicker.getInt(2, keySpace));
                }
            });
        }

        executorService.submit(() -> {
            while (!stop.get()) {
                HazelcastInstance node3 = factory.newHazelcastInstance(config);
                sleepSeconds(2);
                node3.shutdown();
            }
        });

        sleepSeconds(30);
        stop.set(true);
        executorService.shutdown();
        if (!executorService.awaitTermination(30, SECONDS)) {
            throw new IllegalStateException("Not terminated yet...");
        }

        node1.getMap(mapName).flush();

        assertTrueEventually(() -> {
            Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
            for (HazelcastInstance node : instances) {
                String msg = "Failed on instance " + node;
                assertWriteBehindQueuesEmpty(mapName, Collections.singletonList(node));

                long nodeWideUsedCapacity = getNodeWideUsedCapacity(node);
                assertEquals(msg + ", reserved capacity not zero, node wide capacity=" + nodeWideUsedCapacity,
                        0, getTotalNumOfTxnReservedCapacity(mapName, node));
                assertEquals(msg + ", capacity not zero", 0, nodeWideUsedCapacity);
            }
        }, 30);

    }

    private static long getTotalNumOfTxnReservedCapacity(String mapName, HazelcastInstance node) {
        long reservedCapacity = 0;
        MapServiceContext mapServiceContext
                = ((MapService) ((MapProxyImpl) node.getMap(mapName)).getService()).getMapServiceContext();
        PartitionContainer[] partitionContainers = mapServiceContext.getPartitionContainers();
        for (PartitionContainer partitionContainer : partitionContainers) {
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
            if (recordStore != null) {
                reservedCapacity += recordStore.getMapDataStore().getTxnReservedCapacityCounter().getReservedCapacityCountPerTxnId().size();
            }
        }
        return reservedCapacity;
    }

    private static long getNodeWideUsedCapacity(HazelcastInstance node) {
        MapServiceContext mapServiceContext
                = ((MapService) getNodeEngineImpl(node).getService(MapService.SERVICE_NAME)).getMapServiceContext();
        return mapServiceContext.getNodeWideUsedCapacityCounter().currentValue();
    }

    private enum OpType {
        TX_PUT {
            @Override
            void doOp(String mapName, HazelcastInstance node, int keySpace) {
                TransactionContext context = newTransactionContext(node, TWO_PHASE);
                context.beginTransaction();

                TransactionalMap map = context.getMap(mapName);

                for (int i = 0; i < keySpace; i++) {
                    map.put("item-" + i, "value");
                }

                try {
                    context.commitTransaction();
                } catch (TransactionException e) {
                    context.rollbackTransaction();
                }
            }
        },

        TX_REMOVE {
            @Override
            void doOp(String mapName, HazelcastInstance node, int keySpace) {
                TransactionContext context = newTransactionContext(node, TWO_PHASE);
                context.beginTransaction();

                TransactionalMap map = context.getMap(mapName);

                for (int i = 0; i < keySpace; i++) {
                    map.remove("item-" + i);
                }

                try {
                    context.commitTransaction();
                } catch (TransactionException e) {
                    context.rollbackTransaction();
                }
            }
        },

        TX_PUT_REMOVE {
            @Override
            void doOp(String mapName, HazelcastInstance node, int keySpace) {
                TransactionContext context = newTransactionContext(node, TWO_PHASE);
                context.beginTransaction();

                TransactionalMap map = context.getMap(mapName);

                for (int i = 0; i < keySpace; i++) {
                    map.put("item-" + i, i);
                }

                for (int i = 0; i < keySpace; i++) {
                    map.remove("item-" + i);
                }

                try {
                    context.commitTransaction();
                } catch (TransactionException e) {
                    context.rollbackTransaction();
                }
            }
        };

        OpType() {
        }

        abstract void doOp(String mapName, HazelcastInstance node, int keySpace);
    }

    @Test
    public void name() {
        String mapName = "map";
        Config config = getConfig(mapName, 50);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        for (int ii = 0; ii < 100; ii++) {
            TransactionContext context = newTransactionContext(node1, TWO_PHASE);
            context.beginTransaction();

            TransactionalMap map = context.getMap("map");

            for (int i = 0; i < 51; i++) {
                map.put("item-" + i, "value");
            }

            try {
                context.commitTransaction();
            } catch (TransactionException e) {
                context.rollbackTransaction();
            }
        }

        HazelcastInstance node3 = factory.newHazelcastInstance(config);

        assertTrueEventually(() -> {
            Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
            for (HazelcastInstance node : instances) {
                assertWriteBehindQueuesEmpty(mapName, Collections.singletonList(node));

                assertEquals(0, getNodeWideUsedCapacity(node));
                assertEquals(0, getTotalNumOfTxnReservedCapacity(mapName, node));
            }
        }, 30);
    }
}
