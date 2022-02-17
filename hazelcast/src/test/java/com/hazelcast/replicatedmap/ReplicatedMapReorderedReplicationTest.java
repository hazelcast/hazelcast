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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicateUpdateOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Random;

import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ReplicatedMapReorderedReplicationTest extends HazelcastTestSupport {

    // if data serializable factory of ReplicatedMapDataSerializerHook is replaced by updateFactory()
    // during a test, this field stores its original value to be restored on test tear down
    private DataSerializableFactory replicatedMapDataSerializableFactory;
    private Field field;

    @After
    public void tearDown() throws Exception {
        // if updateFactory() has been executed, field & replicatedMapDataSerializableFactory are populated
        if (replicatedMapDataSerializableFactory != null && field != null) {
            // restore original value of ReplicatedMapDataSerializerHook.FACTORY
            updateFactoryField(replicatedMapDataSerializableFactory);
        }
    }

    @Test
    public void testNonConvergingReplicatedMaps() throws Exception {
        final int nodeCount = 4;
        final int keyCount = 10000;
        final int threadCount = 2;

        updateFactory();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final Config config = new Config();
        final HazelcastInstance[] instances = factory
                .newInstances(config, nodeCount);

        warmUpPartitions(instances);

        final int partitionId = randomPartitionOwnedBy(instances[0]).getPartitionId();

        final String mapName = randomMapName();
        final NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int startIndex = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = startIndex; j < keyCount; j += threadCount) {
                        put(nodeEngine, mapName, partitionId, j, j);
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        final ReplicatedRecordStore[] stores = new ReplicatedRecordStore[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            ReplicatedMapService service = getNodeEngineImpl(instances[i]).getService(ReplicatedMapService.SERVICE_NAME);
            service.triggerAntiEntropy();
            stores[i] = service.getReplicatedRecordStore(mapName, false, partitionId);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long version = stores[0].getVersion();

                for (ReplicatedRecordStore store : stores) {
                    assertEquals(version, store.getVersion());
                    assertFalse(store.isStale(version));
                }
            }
        });

        for (int i = 0; i < keyCount; i++) {
            for (ReplicatedRecordStore store : stores) {
                assertTrue(store.containsKey(i));
            }
        }
    }

    private <K, V> V put(final NodeEngine nodeEngine, final String mapName, final int partitionId, K key, V value) {
        isNotNull(key, "key must not be null!");
        isNotNull(value, "value must not be null!");
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);

        PutOperation putOperation = new PutOperation(mapName, dataKey, dataValue);
        InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                .invokeOnPartition(ReplicatedMapService.SERVICE_NAME, putOperation,
                        partitionId);
        VersionResponsePair result = (VersionResponsePair) future.join();
        return nodeEngine.toObject(result.getResponse());
    }

    private void updateFactory() throws Exception {
        // Get Field to manipulate and save it's old value to replicatedMapDataSerializableFactory
        field = ReplicatedMapDataSerializerHook.class.getDeclaredField("FACTORY");
        field.setAccessible(true);
        final DataSerializableFactory factory = (DataSerializableFactory) field.get(null);
        replicatedMapDataSerializableFactory = factory;

        updateFactoryField(new TestReplicatedMapDataSerializerFactory(factory));
    }

    private static class TestReplicatedMapDataSerializerFactory implements DataSerializableFactory {

        private final DataSerializableFactory factory;

        TestReplicatedMapDataSerializerFactory(DataSerializableFactory factory) {
            this.factory = factory;
        }

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == ReplicatedMapDataSerializerHook.REPLICATE_UPDATE) {
                return new RetriedReplicateUpdateOperation();
            }
            return factory.create(typeId);
        }
    }

    private static class RetriedReplicateUpdateOperation extends ReplicateUpdateOperation {

        private final Random random = new Random();

        @Override
        public void run() throws Exception {
            if (random.nextInt(10) < 2) {
                throw new RetryableHazelcastException();
            }
            super.run();
        }
    }

    private void updateFactoryField(DataSerializableFactory factory) {
        final Object staticFieldBase = UnsafeUtil.UNSAFE.staticFieldBase(field);
        final long staticFieldOffset = UnsafeUtil.UNSAFE.staticFieldOffset(field);
        UnsafeUtil.UNSAFE.putObject(staticFieldBase, staticFieldOffset, factory);
    }
}
