/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.impl.operation.AsyncJobOperation;
import com.hazelcast.spi.impl.operationservice.CallsPerMember;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiveOperationRegistryTest {

    private final LiveOperationRegistry r = new LiveOperationRegistry();

    @Test
    public void when_registerDuplicateCallId_then_exception() throws UnknownHostException {
        AsyncJobOperation operation = createOperation("1.2.3.4", 1234, 2222L);
        assertTrue(r.register(operation));

        // this should return true
        assertTrue(r.register(createOperation("1.2.3.4", 1234, 2223L)));

        // adding a duplicate, expecting false
        assertFalse(r.register(operation));
    }

    @Test
    public void test_registerAndDeregister() throws UnknownHostException {
        AsyncJobOperation op1 = createOperation("1.2.3.4", 1234, 2222L);
        AsyncJobOperation op2 = createOperation("1.2.3.4", 1234, 2223L);

        assertTrue(r.register(op1));
        assertTrue(r.register(op2));
        r.deregister(op1);
        r.deregister(op2);
    }

    @Test
    public void when_deregisterNotExistingAddress_then_fail() throws UnknownHostException {
        AsyncJobOperation op1 = createOperation("1.2.3.4", 1234, 2222L);
        assertThrows(IllegalStateException.class, () -> r.deregister(op1));
    }

    @Test
    public void when_deregisterNotExistingCallId_then_fail() throws UnknownHostException {
        r.register(createOperation("1.2.3.4", 1234, 2222L));
        AsyncJobOperation operation = createOperation("1.2.3.4", 1234, 2223L);
        assertThrows(IllegalStateException.class, () -> r.deregister(operation));
    }

    @Test
    public void testPopulate() throws UnknownHostException {
        r.register(createOperation("1.2.3.4", 1234, 2223L));
        r.register(createOperation("1.2.3.4", 1234, 2222L));
        r.register(createOperation("1.2.3.3", 1234, 2222L));

        CallsPerMember liveOperations = new CallsPerMember(new Address("1.2.3.3", 1234));
        r.populate(liveOperations);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(2, addresses.size());
        assertTrue(addresses.contains(new Address("1.2.3.4", 1234)));
        assertTrue(addresses.contains(new Address("1.2.3.3", 1234)));
        long[] runningOperations = liveOperations.toOpControl(new Address("1.2.3.4", 1234)).runningOperations();
        assertTrue(Arrays.equals(new long[] { 2222, 2223}, runningOperations)
                || Arrays.equals(new long[] { 2223, 2222 }, runningOperations));
        runningOperations = liveOperations.toOpControl(new Address("1.2.3.3", 1234)).runningOperations();
        assertArrayEquals(new long[] { 2222 }, runningOperations);
        //callIds.
    }

    private AsyncJobOperation createOperation(String host, int port, long callId) throws UnknownHostException {
        AsyncJobOperation op = spy(AsyncJobOperation.class);
        Address address = new Address(host, port);

        OperationAccessor.setCallerAddress(op, address);
        OperationAccessor.setCallId(op, callId);
        return op;
    }

}
