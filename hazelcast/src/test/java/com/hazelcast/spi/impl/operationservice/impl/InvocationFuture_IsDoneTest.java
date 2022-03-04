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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_IsDoneTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        operationService = getOperationService(local);
    }

    @Test
    public void whenNullResponse() throws ExecutionException, InterruptedException {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        assertTrue(future.isDone());
    }

    @Test
    public void whenInterruptedResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.complete(InvocationConstant.INTERRUPTED);

        assertTrue(future.isDone());
    }

    @Test
    public void whenTimeoutResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, getAddress(local));
        future.complete(InvocationConstant.CALL_TIMEOUT);

        assertTrue(future.isDone());
    }

    @Test
    public void isDone_whenNoResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        assertFalse(future.isDone());
    }

    @Test
    public void isDone_whenObjectResponse() {
        DummyOperation op = new DummyOperation("foobar");

        InternalCompletableFuture future = operationService.invokeOnTarget(null, op, getAddress(local));

        assertTrue(future.isDone());
    }

    // needed to have an invocation and this is the easiest way how to get one and do not bother with its result
    private static class GetLostPartitionOperation extends DummyOperation {
        {
            // we need to set the call ID to prevent running the operation on the calling-thread
            setPartitionId(1);
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(5000);
        }
    }
}
