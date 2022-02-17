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
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_timeoutTest extends HazelcastTestSupport {

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutSingleMember() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        final IQueue<Object> q = hz.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        OperationServiceImpl_BasicTest.assertNoLitterInOpService(hz);
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

        OperationServiceImpl_BasicTest.assertNoLitterInOpService(hz1);
        OperationServiceImpl_BasicTest.assertNoLitterInOpService(hz2);
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
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(memberCount);
        HazelcastInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);

        final HazelcastInstance hz = instances[memberCount - 1];
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = (int) (Math.random() * nodeEngine.getPartitionService().getPartitionCount());

        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(null, new TimedOutBackupAwareOperation(), partitionId);

        final CountDownLatch latch = new CountDownLatch(1);
        if (async) {
            future.exceptionally((throwable) -> {
                if (throwable instanceof OperationTimeoutException) {
                    latch.countDown();
                }
                return null;
            });
        } else {
            try {
                future.joinInternal();
                fail("Should throw OperationTimeoutException!");
            } catch (OperationTimeoutException ignored) {
                latch.countDown();
            }
        }

        assertOpenEventually("Should throw OperationTimeoutException", latch);

        for (HazelcastInstance instance : instances) {
            OperationServiceImpl_BasicTest.assertNoLitterInOpService(instance);
        }
    }

    static class TimedOutBackupAwareOperation extends Operation
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

    @Test
    public void testOperationTimeoutForLongRunningLocalOperation() throws Exception {
        int callTimeoutMillis = 500;
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(callTimeoutMillis));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        // invoke on the "local" member
        Address localAddress = getNode(hz1).getThisAddress();
        OperationService operationService = getNode(hz1).getNodeEngine().getOperationService();
        InternalCompletableFuture<Boolean> future = operationService
                .invokeOnTarget(null, new SleepingOperation(callTimeoutMillis * 5), localAddress);

        // wait more than operation timeout
        sleepAtLeastMillis(callTimeoutMillis * 3);
        assertTrue(future.get());
    }

    public static class SleepingOperation extends Operation {
        private long sleepTime;

        public SleepingOperation() {
        }

        public SleepingOperation(long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public void run() throws Exception {
            sleepAtLeastMillis(sleepTime);
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(sleepTime);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            sleepTime = in.readLong();
        }
    }

}
