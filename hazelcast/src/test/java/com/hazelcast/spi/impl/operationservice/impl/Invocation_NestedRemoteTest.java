/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionException;

import static com.hazelcast.internal.util.RootCauseMatcher.rootCause;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_NestedRemoteTest extends Invocation_NestedAbstractTest {

    private static final String RESPONSE = "someresponse";

    @Test
    public void invokeOnPartition_outerGeneric_innerGeneric_forbidden() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(1).newInstances();
        HazelcastInstance local = cluster[0];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);

        assertThatThrownBy(() -> {
            InternalCompletableFuture<Object> future =
                    operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());
            future.join();
        }).isInstanceOf(Exception.class);
    }

    @Test
    public void invokeOnPartition_outerRemote_innerGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, partitionId);

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerSameInstance_samePartition() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, partitionId);

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerSameInstance_callsDifferentPartition_mappedToSameThread() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = randomPartitionIdMappedToSameThreadAsGivenPartitionIdOnInstance(outerPartitionId, remote,
                operationService);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalThreadStateException.class))
                .hasMessageContaining("cannot make remote call");
    }

    @Test
    public void invokeOnPartition_outerRemote_innerDifferentInstance_forbidden() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = getPartitionId(local);
        assertNotEquals("partitions should be different", innerPartitionId, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalThreadStateException.class))
                .hasMessageContaining("cannot make remote call");
    }

    @Test
    public void invokeOnPartition_outerLocal_innerDifferentInstance_forbidden() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(local);
        int innerPartitionId = getPartitionId(remote);
        assertNotEquals("partitions should be different", innerPartitionId, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalThreadStateException.class))
                .hasMessageContaining("cannot make remote call");
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerGeneric() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerSameInstance() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = cluster[0];
        HazelcastInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, 0);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture future = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(RESPONSE, future.join());
    }
}
