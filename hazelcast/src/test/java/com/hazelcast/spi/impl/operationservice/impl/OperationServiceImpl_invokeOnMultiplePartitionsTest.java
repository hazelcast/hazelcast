/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.ExceptionThrowingCallable;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationServiceImpl_invokeOnMultiplePartitionsTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private HazelcastInstance remote;

    private Address localAddress;
    private Address remoteAddress;

    private InternalOperationService operationService;

    @Before
    public void setup() {
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(nodes);

        local = nodes[0];
        remote = nodes[1];

        localAddress = local.getCluster().getLocalMember().getAddress();
        remoteAddress = remote.getCluster().getLocalMember().getAddress();

        operationService = getOperationService(local);
    }

    @Test
    public void whenLocalPartition() throws Exception {
        int partitionId = getPartitionId(local);
        String expected = "foobar";
        DummyMultiPartitionOperation operation = new DummyMultiPartitionOperation(partitionId, expected);

        Map<Integer, Object> resultMap = operationService.invokeMultiplePartitionOperation(null, operation, localAddress, null);

        assertEquals(1, resultMap.size());
        assertEquals(expected, resultMap.get(partitionId));

        assertFalse("A failure operation was created, although this was not expected", operation.getFailureOperationCreated());
    }

    @Test
    public void whenRemotePartition() throws Exception {
        int partitionId = getPartitionId(remote);
        String expected = "foobar";
        DummyMultiPartitionOperation operation = new DummyMultiPartitionOperation(partitionId, expected);

        Map<Integer, Object> resultMap = operationService.invokeMultiplePartitionOperation(null, operation, remoteAddress, null);

        assertEquals(1, resultMap.size());
        assertEquals(expected, resultMap.get(partitionId));

        assertFalse("A failure operation was created, although this was not expected", operation.getFailureOperationCreated());
    }

    @Test
    public void whenExceptionThrownInOperationRun() throws Exception {
        int partitionId = getPartitionId(remote);
        DummyMultiPartitionOperation operation = new DummyMultiPartitionOperation(partitionId, new ExceptionThrowingCallable());

        try {
            operationService.invokeMultiplePartitionOperation(null, operation, remoteAddress, null);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ExpectedRuntimeException);
        }

        assertTrue("No failure operation was created, although this was expected", operation.getFailureOperationCreated());
    }
}
