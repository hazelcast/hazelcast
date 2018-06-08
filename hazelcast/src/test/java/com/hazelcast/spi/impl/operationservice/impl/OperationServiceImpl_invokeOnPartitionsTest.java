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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationServiceImpl_invokeOnPartitionsTest extends HazelcastTestSupport {

    @Test
    public void test() throws Exception {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "" + 100);
        config.getSerializationConfig().addDataSerializableFactory(123, new SlowOperationSerializationFactory());
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl opService = getOperationServiceImpl(hz);

        Map<Integer, Object> result = opService.invokeOnAllPartitions(null, new OperationFactoryImpl());

        assertEquals(100, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
    }

    @Test
    public void testLongRunning() throws Exception {
        Config config = new Config()
                .setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "2000")
                .setProperty(PARTITION_COUNT.getName(), "" + 100);
        config.getSerializationConfig().addDataSerializableFactory(123, new SlowOperationSerializationFactory());
        TestHazelcastInstanceFactory hzFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = hzFactory.newHazelcastInstance(config);
        HazelcastInstance hz2 = hzFactory.newHazelcastInstance(config);
        warmUpPartitions(hz1, hz2);
        OperationServiceImpl opService = getOperationServiceImpl(hz1);

        Map<Integer, Object> result = opService.invokeOnAllPartitions(null, new SlowOperationFactoryImpl());

        assertEquals(100, result.size());
        for (Map.Entry<Integer, Object> entry : result.entrySet()) {
            int partitionId = entry.getKey();
            assertEquals(partitionId * 2, entry.getValue());
        }
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
        public int getId() {
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
        public int getId() {
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
            sleepSeconds(5);
            response = getPartitionId() * 2;
        }

        @Override
        public Object getResponse() {
            return response;
        }
    }

}
