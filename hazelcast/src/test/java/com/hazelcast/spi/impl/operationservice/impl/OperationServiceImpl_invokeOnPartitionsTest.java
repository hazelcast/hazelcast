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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_invokeOnPartitionsTest extends HazelcastTestSupport {

    @Test
    public void test_onAllPartitions() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Map<Integer, Object> result = opService.invokeOnAllPartitions(null, new OperationFactoryImpl());

        assertEquals(100, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void test_onSelectedPartitions() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Collection<Integer> partitions = new LinkedList<>();
        Collections.addAll(partitions, 1, 2, 3);
        Map<Integer, Object> result = opService.invokeOnPartitions(null, new OperationFactoryImpl(), partitions);

        assertEquals(3, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void test_onEmptyPartitionLIst() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Map<Integer, Object> result = opService.invokeOnPartitions(null, new OperationFactoryImpl(), emptyList());

        assertEquals(0, result.size());
    }

    @Test
    public void testAsync_onAllPartitions_getResponeViaFuture() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Future<Map<Integer, Object>> future = opService.invokeOnAllPartitionsAsync(null, new OperationFactoryImpl());

        Map<Integer, Object> result = future.get();
        assertEquals(100, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void testAsync_onSelectedPartitions_getResponeViaFuture() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Collection<Integer> partitions = new LinkedList<>();
        Collections.addAll(partitions, 1, 2, 3);
        Future<Map<Integer, Object>> future = opService.invokeOnPartitionsAsync(null, new OperationFactoryImpl(), partitions);

        Map<Integer, Object> result = future.get();
        assertEquals(3, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void testAsync_onEmptyPartitionList_getResponeViaFuture() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Future<Map<Integer, Object>> future = opService.invokeOnPartitionsAsync(null, new OperationFactoryImpl(), emptyList());

        Map<Integer, Object> result = future.get();
        assertEquals(0, result.size());
    }

    @Test
    public void testAsync_onAllPartitions_getResponseViaCallback() {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        final AtomicReference<Map<Integer, Object>> resultReference = new AtomicReference<Map<Integer, Object>>();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        opService.invokeOnAllPartitionsAsync(null, new OperationFactoryImpl()).whenComplete((v, t) -> {
            if (t == null) {
                resultReference.set(v);
                responseLatch.countDown();
            }
        });

        assertOpenEventually(responseLatch);
        Map<Integer, Object> result = resultReference.get();
        assertEquals(100, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void testAsync_onSelectedPartitions_getResponseViaCallback() {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Collection<Integer> partitions = new LinkedList<>();
        Collections.addAll(partitions, 1, 2, 3);

        final AtomicReference<Map<Integer, Object>> resultReference = new AtomicReference<>();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        opService.invokeOnPartitionsAsync(null, new OperationFactoryImpl(), partitions).whenComplete((v, t) -> {
            if (t == null) {
                resultReference.set(v);
                responseLatch.countDown();
            }
        });

        assertOpenEventually(responseLatch);
        Map<Integer, Object> result = resultReference.get();
        assertEquals(3, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void testAsync_onEmptyPartitionList_getResponseViaCallback() {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        final AtomicReference<Map<Integer, Object>> resultReference = new AtomicReference<>();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        opService.invokeOnPartitionsAsync(null, new OperationFactoryImpl(), emptyList()).whenComplete((v, t) -> {
            if (t == null) {
                resultReference.set(v);
                responseLatch.countDown();
            }
        });

        assertOpenEventually(responseLatch);
        Map<Integer, Object> result = resultReference.get();
        assertEquals(0, result.size());
    }

    @Test
    public void testLongRunning() throws Exception {
        Config config = new Config()
                .setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "10000")
                .setProperty(PARTITION_COUNT.getName(), "10");
        config.getSerializationConfig().addDataSerializableFactory(123, new SlowOperationSerializationFactory());
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = hzFactory.newHazelcastInstance(config);
        HazelcastInstance hz2 = hzFactory.newHazelcastInstance(config);
        warmUpPartitions(hz1, hz2);
        OperationServiceImpl opService = getOperationService(hz1);

        Map<Integer, Object> result = opService.invokeOnAllPartitions(null, new SlowOperationFactoryImpl());

        assertEquals(10, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void testPartitionScopeIsRespectedForPartitionAwareFactories() throws Exception {
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "100");
        config.getSerializationConfig()
                .addDataSerializableFactory(321, new PartitionAwareOperationFactoryDataSerializableFactory());
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationService(hz);

        Map<Integer, Object> result = opService
                .invokeOnPartitions(null, new PartitionAwareOperationFactoryImpl(new int[]{0, 1, 2}), new int[]{1});

        assertEquals(1, result.size());
        assertEquals(2, result.values().iterator().next());
    }

    private static class OperationFactoryImpl extends AbstractOperationFactor {
        @Override
        public Operation createOperation() {
            return new OperationImpl();
        }

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    private static class SlowOperationFactoryImpl extends AbstractOperationFactor {
        @Override
        public Operation createOperation() {
            return new SlowOperation();
        }

        @Override
        public int getFactoryId() {
            return 123;
        }

        @Override
        public int getClassId() {
            return 145;
        }
    }

    private static class SlowOperationSerializationFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new SlowOperationFactoryImpl();
        }
    }

    private abstract static class AbstractOperationFactor implements OperationFactory {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private static class OperationImpl extends Operation {
        private int response;

        @Override
        public void run() {
            response = getPartitionId() * 2;
        }

        @Override
        public Object getResponse() {
            return response;
        }
    }

    private static class SlowOperation extends Operation {
        private int response;

        @Override
        public void run() {
            sleepSeconds(20);
            response = getPartitionId() * 2;
        }

        @Override
        public Object getResponse() {
            return response;
        }
    }

    private static class PartitionAwareOperationFactoryImpl extends PartitionAwareOperationFactory {
        PartitionAwareOperationFactoryImpl(int[] partitions) {
            this.partitions = partitions;
        }

         PartitionAwareOperationFactoryImpl() {
        }

        @Override
        public Operation createPartitionOperation(int partition) {
            return new OperationImpl();
        }

        @Override
        public int getFactoryId() {
            return 321;
        }

        @Override
        public int getClassId() {
            return 654;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeIntArray(partitions);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.partitions = in.readIntArray();
        }
    }

    private static class PartitionAwareOperationFactoryDataSerializableFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            return new PartitionAwareOperationFactoryImpl();
        }
    }

}
