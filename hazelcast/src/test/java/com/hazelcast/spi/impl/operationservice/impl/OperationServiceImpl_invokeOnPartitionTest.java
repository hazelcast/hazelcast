/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class OperationServiceImpl_invokeOnPartitionTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private InternalOperationService operationService;
    private HazelcastInstance remote;

    @Before
    public void setup(){
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(nodes);

        local = nodes[0];
        remote = nodes[1];
        operationService = getOperationService(local);
    }

    @Test
    public void test_whenLocalPartition(){
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);

        InternalCompletableFuture<String> invocation = operationService.invokeOnPartition(
                null, operation, getPartitionId(local));
        assertEquals(expected, invocation.getSafely());
    }

    @Test
    public void test_whenRemotePartition(){
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);

        InternalCompletableFuture<String> invocation = operationService.invokeOnPartition(
                null, operation, getPartitionId(remote));
        assertEquals(expected, invocation.getSafely());
    }

    public static class DummyOperation extends AbstractOperation {
        private Object value;

        public DummyOperation() {
        }

        public DummyOperation(Object value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return value;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readObject();
        }
    }
}
