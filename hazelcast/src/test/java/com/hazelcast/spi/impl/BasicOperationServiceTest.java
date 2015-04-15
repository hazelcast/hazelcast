/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicOperationServiceTest extends HazelcastTestSupport {

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutSingleMember() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final IQueue<Object> q = hz.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        assertNoLitterInOpService(hz);
    }

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutWithMultiMemberCluster() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        final IQueue<Object> q = hz1.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        assertNoLitterInOpService(hz1);
        assertNoLitterInOpService(hz2);
    }

    //there was a memory leak caused by the invocation not releasing the backup registration
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

    //there was a memory leak caused by the invocation not releasing the backup registration
    // when Future.get() is not called.
    @Test
    public void testAsyncOpsMultiMember() throws InterruptedException {
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

    @Test
    public void testSyncOperationTimeoutSingleMember() {
        testOperationTimeout(1, false);
    }

    @Test
    public void testSyncOperationTimeoutMultiMember() {
        testOperationTimeout(3, false);
    }

    @Test
    public void testAsyncOperationTimeoutSingleMember() {
        testOperationTimeout(1, true);
    }

    @Test
    public void testAsyncOperationTimeoutMultiMember() {
        testOperationTimeout(3, true);
    }

    private void testOperationTimeout(int memberCount, boolean async) {
        assertTrue(memberCount > 0);
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "3000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(memberCount);
        HazelcastInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);

        final HazelcastInstance hz = instances[memberCount - 1];
        Node node = TestUtil.getNode(hz);
        NodeEngine nodeEngine = node.nodeEngine;
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = (int) (Math.random() * node.getPartitionService().getPartitionCount());

        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(null, new TimedOutBackupAwareOperation(), partitionId);

        final CountDownLatch latch = new CountDownLatch(1);
        if (async) {
            future.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof OperationTimeoutException) {
                        latch.countDown();
                    }
                }
            });
        } else {
            try {
                future.getSafely();
                fail("Should throw OperationTimeoutException!");
            } catch (OperationTimeoutException ignored) {
                latch.countDown();
            }
        }

        assertOpenEventually("Should throw OperationTimeoutException", latch);

        for (HazelcastInstance instance : instances) {
            assertNoLitterInOpService(instance);
        }
    }

    static class TimedOutBackupAwareOperation extends AbstractOperation
            implements BackupAwareOperation {
        @Override
        public void run() throws Exception {
            LockSupport.parkNanos((long) (Math.random() * 1000 + 10));
        }

        @Override
        public boolean returnsResponse() {
            // required for operation timeout
            return false;
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return null;
        }
    }

    /*
     * Github Issue 2559
     */
    @Test(expected = HazelcastSerializationException.class)
    public void testPropagateSerializationErrorOnOperationToCaller() throws Exception {

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        Node node = getNode(hz1);
        OperationService operationService = node.nodeEngine.getOperationService();

        Address address = ((MemberImpl) hz2.getCluster().getLocalMember()).getAddress();

        Operation operation = new OperationWithFailingSerializableParameter(new FailingSerializableValue());
        operationService.invokeOnTarget(null, operation, address);
    }

    /*
     * Github Issue 2559
     */
    @Test
    public void testPropagateSerializationErrorOnResponseToCaller() throws Exception {

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        Node node = getNode(hz1);
        OperationService operationService = node.nodeEngine.getOperationService();

        Address address = ((MemberImpl) hz2.getCluster().getLocalMember()).getAddress();

        Operation operation = new OperationWithFailingSerializableResponse();
        ICompletableFuture<Object> future = operationService.invokeOnTarget(null, operation, address);
        try {
            future.get(30, TimeUnit.SECONDS);
            fail("Invocation should fail!");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            assertEquals("Cause of failure should be " + HazelcastSerializationException.class.getName(),
                    e.getCause().getClass(), HazelcastSerializationException.class);
        }
    }

    static class OperationWithFailingSerializableParameter extends AbstractOperation {

        private FailingSerializableValue value;

        public OperationWithFailingSerializableParameter() {
        }

        public OperationWithFailingSerializableParameter(FailingSerializableValue value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return null;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            value.writeData(out);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            value = new FailingSerializableValue();
        }
    }

    static class OperationWithFailingSerializableResponse extends AbstractOperation {

        private FailingSerializableValue value;

        @Override
        public void run() throws Exception {
            value = new FailingSerializableValue();
        }

        @Override
        public Object getResponse() {
            return value;
        }
    }

    public static class FailingSerializableValue implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            throw new RuntimeException("BAM!");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private void assertNoLitterInOpService(HazelcastInstance hz) {
        final BasicOperationService operationService = (BasicOperationService) getNode(hz).nodeEngine.getOperationService();

        //we need to do this with an assertTrueEventually because it can happen that system calls are being send
        //and this leads to the maps not being empty. But eventually they will be empty at some moment in time.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("invocations should be empty", 0, operationService.invocations.size());
             }
        });
    }
}
