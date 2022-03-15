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

package com.hazelcast.client.impl.operations;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationFactoryWrapperTest extends HazelcastTestSupport {

    /**
     * OperationFactoryWrapper gets a UUID and sets it to its created operations.
     * This test ensures UUID doesn't change through the operation system.
     */
    @Test
    public void testOperationSeesActualCallersUUID() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        OperationServiceImpl operationService = getOperationService(hz);

        UUID expectedCallersUUID = UUID.randomUUID();

        GetCallersUUIDOperationFactory operationFactory = new GetCallersUUIDOperationFactory();
        OperationFactoryWrapper wrapper = new OperationFactoryWrapper(operationFactory, expectedCallersUUID);

        int partitionId = 0;
        Map<Integer, Object> responses = operationService.invokeOnPartitions(SERVICE_NAME, wrapper, singletonList(partitionId));

        UUID actualCallersUUID = (UUID) responses.get(partitionId);
        assertEquals("Callers UUID should not be changed", expectedCallersUUID, actualCallersUUID);
    }

    private class GetCallersUUIDOperationFactory implements OperationFactory {

        GetCallersUUIDOperationFactory() {
        }

        @Override
        public Operation createOperation() {
            return new GetCallersUUIDOperation();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            throw new UnsupportedOperationException("Intended to work locally");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException("Intended to work locally");
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

    private class GetCallersUUIDOperation extends Operation {

        GetCallersUUIDOperation() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return getCallerUuid();
        }
    }
}
