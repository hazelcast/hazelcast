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
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_NestedLocalTest extends Invocation_NestedAbstractTest {

    private static final String RESPONSE = "someresponse";

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void invokeOnPartition_outerGeneric_innerGeneric_forbidden() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);

        expected.expect(Exception.class);
        InternalCompletableFuture<Object> future =
                operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());
        future.join();
    }

    @Test
    public void invokeOnPartition_outerLocal_innerGeneric() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(local);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, partitionId);

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnPartition_outerLocal_innerSameInstance_samePartition() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(local);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, partitionId);

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnPartition_outerLocal_innerSameInstance_callsDifferentPartition() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(local);
        int innerPartitionId = randomPartitionIdNotMappedToSameThreadAsGivenPartitionIdOnInstance(local, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        future.join();
    }

    @Test
    public void invokeOnPartition_outerLocal_innerSameInstance_callsDifferentPartition_mappedToSameThread() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "2");
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        HazelcastInstance local = createHazelcastInstance(config);
        final OperationService operationService = getOperationService(local);

        int outerPartitionId = 1;
        int innerPartitionId = 0;
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture future
                = operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());

        expected.expect(IllegalThreadStateException.class);
        expected.expectMessage("cannot make remote call");
        future.join();
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerGeneric() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, outerOperation, getAddress(local));

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerSameInstance() {
        HazelcastInstance local = createHazelcastInstance();
        OperationService operationService = getOperationService(local);

        int innerPartitionId = getPartitionId(local);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, outerOperation, getAddress(local));

        assertEquals(RESPONSE, future.join());
    }
}
