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
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that instances returned by the InvocationFuture, are always copied instances.
 * We don't want instances to be shared unexpectedly. In the future there might be some sharing going
 * on when we see that an instance is immutable.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_GetNewInstanceTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private HazelcastInstance remote;

    @Before
    public void setUp() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(instances);
        local = instances[0];
        remote = instances[1];
    }

    @Test
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        Node localNode = getNode(local);

        Data response = localNode.nodeEngine.toData(new DummyObject());
        Operation op = new OperationWithResponse(response);

        OperationService service = getOperationService(local);
        Future future = service.createInvocationBuilder(null, op, localNode.address).invoke();
        Object instance1 = future.get();
        Object instance2 = future.get();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertTrue(instance1 instanceof DummyObject);
        assertTrue(instance2 instanceof DummyObject);
        assertNotSame(instance1, instance2);
        assertNotSame(instance1, response);
        assertNotSame(instance2, response);
    }

    @Test
    public void invocationToRemoteMember() throws ExecutionException, InterruptedException {
        Node localNode = getNode(local);

        Data response = localNode.nodeEngine.toData(new DummyObject());
        Operation op = new OperationWithResponse(response);

        Address remoteAddress = getAddress(remote);

        OperationService operationService = getOperationService(local);
        Future future = operationService.createInvocationBuilder(null, op, remoteAddress).invoke();
        Object instance1 = future.get();
        Object instance2 = future.get();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertTrue(instance1 instanceof DummyObject);
        assertTrue(instance2 instanceof DummyObject);
        assertNotSame(instance1, instance2);
        assertNotSame(instance1, response);
        assertNotSame(instance2, response);
    }

    public static class DummyObject implements Serializable {
    }

    public static class OperationWithResponse extends Operation {

        private Data response;

        public OperationWithResponse() {
        }

        OperationWithResponse(Data response) {
            this.response = response;
        }

        @Override
        public void run() throws Exception {
            //no-op
        }

        @Override
        public Object getResponse() {
            return response;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            IOUtil.writeData(out, response);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            response = IOUtil.readData(in);
        }
    }
}
