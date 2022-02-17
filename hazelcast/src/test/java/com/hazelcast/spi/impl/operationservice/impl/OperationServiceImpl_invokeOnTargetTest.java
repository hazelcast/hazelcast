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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.test.ExceptionThrowingCallable;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_invokeOnTargetTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private OperationServiceImpl operationService;
    private HazelcastInstance remote;

    @Before
    public void setup() {
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(nodes);

        local = nodes[0];
        remote = nodes[1];
        operationService = getOperationService(local);
    }

    @Test
    public void whenLocal() {
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(local));
        assertEquals(expected, invocation.join());

        //todo: we need to verify that the call was run on the calling thread
    }

    @Test
    public void whenRemote() {
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));
        assertEquals(expected, invocation.join());
    }

    @Test
    public void whenNonExistingTarget() {
        Address remoteAddress = getAddress(remote);
        remote.shutdown();

        // ensure local instance observes remote shutdown
        assertClusterSizeEventually(1, local);

        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected) {
            @Override
            public ExceptionAction onInvocationException(Throwable throwable) {
                // Don't retry when TargetNotMemberException is received.
                // Invocation is registered before checking target. If invocation is retried
                // when TargetNotMemberException is get, invocation may fail with MemberLeftException too.
                if (throwable instanceof TargetNotMemberException) {
                    return ExceptionAction.THROW_EXCEPTION;
                }
                return super.onInvocationException(throwable);
            }
        };

        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, remoteAddress);

        try {
            invocation.joinInternal();
            fail();
        } catch (TargetNotMemberException e) {
        }
    }

    @Test
    public void whenExceptionThrownInOperationRun() {
        DummyOperation operation = new DummyOperation(new ExceptionThrowingCallable());
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));

        try {
            invocation.joinInternal();
            fail();
        } catch (ExpectedRuntimeException expected) {
        }
    }

}
