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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.spi.properties.ClusterProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PRIORITY_GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_BasicTest extends HazelcastTestSupport {

    @Test
    public void testGetPartitionThreadCount() {
        Config config = new Config();
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "5");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl operationService = getOperationService(hz);

        assertEquals(5, operationService.getPartitionThreadCount());
    }

    @Test
    public void testGetGenericThreadCount() {
        Config config = new Config();
        config.setProperty(GENERIC_OPERATION_THREAD_COUNT.getName(), "5");
        config.setProperty(PRIORITY_GENERIC_OPERATION_THREAD_COUNT.getName(), "1");
        HazelcastInstance hz = createHazelcastInstance(config);
        OperationServiceImpl operationService = getOperationService(hz);

        assertEquals(6, operationService.getGenericThreadCount());
    }

    // there was a memory leak caused by the invocation not releasing the backup registration
    // when Future.get() is not called.
    @Test
    public void testAsyncOpsSingleMember() {
        HazelcastInstance hz = createHazelcastInstance();
        final IMap<Object, Object> map = hz.getMap("test");

        final int count = 1000;
        for (int i = 0; i < count; i++) {
            map.putAsync(i, i);
        }

        assertSizeEventually(count, map);
        assertNoLitterInOpService(hz);
    }

    // there was a memory leak caused by the invocation not releasing the backup registration
    // when Future.get() is not called.
    @Test
    public void testAsyncOpsMultiMember() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz2, hz);

        final IMap<Object, Object> map = hz.getMap("test");
        final IMap<Object, Object> map2 = hz2.getMap("test");

        final int count = 2000;
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                map.putAsync(i, i);
            } else {
                map2.putAsync(i, i);
            }
        }

        assertSizeEventually(count, map);
        assertSizeEventually(count, map2);

        assertNoLitterInOpService(hz);
        assertNoLitterInOpService(hz2);
    }


    @Test(expected = ExecutionException.class)
    public void testPropagateSerializationErrorOnResponseToCallerGithubIssue2559()
            throws Exception {

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);

        HazelcastInstanceImpl impl = (HazelcastInstanceImpl) original.get(hz1);
        OperationService operationService = impl.node.nodeEngine.getOperationService();

        Address address = hz2.getCluster().getLocalMember().getAddress();

        Operation operation = new GithubIssue2559Operation();
        String serviceName = DistributedExecutorService.SERVICE_NAME;
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(serviceName, operation, address);
        invocationBuilder.invoke().get();
    }

    public static class GithubIssue2559Operation
            extends Operation {

        private GithubIssue2559Value value;

        @Override
        public void run()
                throws Exception {

            value = new GithubIssue2559Value();
            value.foo = 10;
        }

        @Override
        public Object getResponse() {
            return value;
        }
    }

    public static class GithubIssue2559Value
            implements DataSerializable {

        private int foo;

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            throw new RuntimeException("BAM!");
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            foo = in.readInt();
        }
    }

    public static void assertNoLitterInOpService(HazelcastInstance hz) {
        final OperationServiceImpl operationService = (OperationServiceImpl) getNode(hz).nodeEngine.getOperationService();

        // we need to do this with an assertTrueEventually because it can happen that system calls are being send
        // and this leads to the maps not being empty. But eventually they will be empty at some moment in time.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("invocations should be empty", 0, operationService.invocationRegistry.size());
            }
        });
    }

    @Test(expected = HazelcastSerializationException.class)
    public void invocation_shouldFail_whenResponse_isNotSerializable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        OperationServiceImpl operationService = getOperationService(hz1);
        Address target = getAddress(hz2);

        InternalCompletableFuture<Object> future = operationService
                .invokeOnTarget(null, new NonSerializableResponseOperation(), target);

        future.joinInternal();
    }

    @Test(expected = HazelcastSerializationException.class)
    public void invocation_shouldFail_whenNormalResponse_isNotSerializable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        OperationServiceImpl operationService = getOperationService(hz1);
        Address target = getAddress(hz2);


        InternalCompletableFuture<Object> future = operationService
                .invokeOnTarget(null, new NonSerializableResponseOperation_withNormalResponseWrapper(), target);

        future.joinInternal();
    }

    private static class NonSerializableResponse {
    }

    private static class NonSerializableResponseOperation extends Operation {

        @Override
        public Object getResponse() {
            return new NonSerializableResponse();
        }
    }

    private static class NonSerializableResponseOperation_withNormalResponseWrapper extends Operation {
        @Override
        public Object getResponse() {
            return new NormalResponse(new NonSerializableResponse(), getCallId(), 0, false);
        }
    }
}
