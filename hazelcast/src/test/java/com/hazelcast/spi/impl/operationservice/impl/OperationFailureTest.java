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
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationFailureTest extends HazelcastTestSupport {

    // static reference to store backup operation failure
    private static final AtomicReference<Throwable> backupOperationFailure = new AtomicReference<Throwable>();

    @Test
    public void onFailure_shouldBeCalled_whenOperationExecutionFails() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);

        FailingOperation op = new FailingOperation(new CountDownLatch(1));
        nodeEngine.getOperationService().execute(op);

        assertOpenEventually(op.latch);
        assertInstanceOf(ExpectedRuntimeException.class, op.failure);
    }

    @Test
    public void onFailure_shouldBeCalled_whenOperationIsRejected() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);

        FailingOperation op = new FailingOperation(new CountDownLatch(1));
        op.setPartitionId(1).setReplicaIndex(1);
        nodeEngine.getOperationService().execute(op);

        assertOpenEventually(op.latch);
        assertInstanceOf(WrongTargetException.class, op.failure);
    }

    @Test
    public void onFailure_shouldBeCalled_whenBackupExecutionFails() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz, hz2);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);

        nodeEngine.getOperationService().invokeOnPartition(null, new EmptyBackupAwareOperation(), 0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(backupOperationFailure.get());
            }
        });

        Throwable failure = backupOperationFailure.getAndSet(null);
        assertInstanceOf(ExpectedRuntimeException.class, failure);
    }

    private static class FailingOperation extends Operation {
        final CountDownLatch latch;
        volatile Throwable failure;

        FailingOperation(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() throws Exception {
            throw new ExpectedRuntimeException();
        }

        @Override
        public void onExecutionFailure(Throwable e) {
            failure = e;
            latch.countDown();
        }
    }

    private static class EmptyBackupAwareOperation extends Operation implements BackupAwareOperation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 1;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new FailingBackupOperation();
        }
    }

    private static class FailingBackupOperation extends Operation implements BackupOperation {
        @Override
        public void run() throws Exception {
            throw new ExpectedRuntimeException();
        }

        @Override
        public void onExecutionFailure(Throwable e) {
            backupOperationFailure.set(e);
        }
    }
}
