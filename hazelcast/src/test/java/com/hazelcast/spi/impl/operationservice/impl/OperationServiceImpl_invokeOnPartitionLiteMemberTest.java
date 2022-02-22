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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.partition.InternalPartitionService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Collections.singletonList;
import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_invokeOnPartitionLiteMemberTest
        extends HazelcastTestSupport {

    private Config liteMemberConfig = new Config().setLiteMember(true);

    private Operation operation;

    @Before
    public void before() {
        operation = new DummyOperation("foobar");
    }

    @Test
    public void test_invokeOnPartition_onLiteMember()
            throws InterruptedException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final OperationServiceImpl operationService = getOperationService(instance);
        final InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, operation, 0);

        try {
            future.get();
            fail("partition operation should not run on lite member!");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NoDataMemberInClusterException);
        }
    }

    @Test
    public void test_invokeOnPartition_withDataMember()
            throws ExecutionException, InterruptedException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance lite = factory.newHazelcastInstance(liteMemberConfig);
        factory.newHazelcastInstance();

        final OperationServiceImpl operationService = getOperationService(lite);
        final InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, operation, 0);

        assertEquals("foobar", future.get());
    }

    @Test
    public void test_invokeOnPartitionAsync_onLiteMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final OperationServiceImpl operationService = getOperationService(instance);
        final DummyExecutionCallback callback = new DummyExecutionCallback();
        operationService.<String>invokeOnPartitionAsync(null, operation, 0).whenComplete(callback);

        assertOpenEventually(callback.responseLatch);
        assertTrue(callback.response instanceof NoDataMemberInClusterException);
    }

    @Test
    public void test_asyncInvokeOnPartition_withDataMember() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);
        factory.newHazelcastInstance();

        final OperationServiceImpl operationService = getOperationService(instance);
        final DummyExecutionCallback callback = new DummyExecutionCallback();
        operationService.<String>invokeOnPartitionAsync(null, operation, 0).whenComplete(callback);

        assertOpenEventually(callback.responseLatch);
        assertEquals("foobar", callback.response);
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_invokeOnPartitions_onLiteMember()
            throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final OperationServiceImpl operationService = getOperationService(instance);
        operationService.invokeOnPartitions(SERVICE_NAME, new DummyOperationFactory(), singletonList(0));
    }

    @Test
    public void test_invokeOnPartitions_withDataMember()
            throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);
        factory.newHazelcastInstance();

        final OperationServiceImpl operationService = getOperationService(instance);
        final Map<Integer, Object> resultMap = operationService
                .invokeOnPartitions(SERVICE_NAME, new DummyOperationFactory(), singletonList(0));

        assertEquals(1, resultMap.size());
        assertEquals("foobar", resultMap.get(0));
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_invokeOnAllPartitions_onLiteMember()
            throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);

        final OperationServiceImpl operationService = getOperationService(instance);
        operationService.invokeOnAllPartitions(SERVICE_NAME, new DummyOperationFactory());
    }

    @Test
    public void test_invokeOnAllPartitions_withDataMember()
            throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newHazelcastInstance(liteMemberConfig);
        factory.newHazelcastInstance();

        final OperationServiceImpl operationService = getOperationService(instance);
        final Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(SERVICE_NAME, new DummyOperationFactory());

        assertFalse(resultMap.isEmpty());
    }

    private static class DummyOperationFactory implements OperationFactory {

        @Override
        public Operation createOperation() {
            return new DummyOperation("foobar");
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

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

    static class DummyExecutionCallback implements BiConsumer<String, Throwable> {

        private final CountDownLatch responseLatch = new CountDownLatch(1);

        private volatile Object response;

        @Override
        public void accept(String response, Throwable t) {
            if (t == null) {
                setResponse(response);
            } else {
                setResponse(t);
            }
        }

        private void setResponse(Object response) {
            this.response = response;
            responseLatch.countDown();
        }

        public Object getResponse() {
            return response;
        }

    }

}
