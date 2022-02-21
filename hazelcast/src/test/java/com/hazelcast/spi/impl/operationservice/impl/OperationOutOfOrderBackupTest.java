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
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.partition.TestPartitionUtils.getDefaultReplicaVersions;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationOutOfOrderBackupTest extends HazelcastTestSupport {

    private final ValueHolderService service = new ValueHolderService();

    private int partitionId;
    private NodeEngineImpl nodeEngine1;
    private NodeEngineImpl nodeEngine2;

    @Before
    public void setup() {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig()
                .setImplementation(this.service)
                .setName(ValueHolderService.NAME)
                .setEnabled(true);
        ConfigAccessor.getServicesConfig(config)
                      .addServiceConfig(serviceConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        warmUpPartitions(hz2, hz1);

        partitionId = getPartitionId(hz1);
        nodeEngine1 = getNodeEngineImpl(hz1);
        nodeEngine2 = getNodeEngineImpl(hz2);
    }

    @Test
    public void test() throws InterruptedException {
        // set 1st value
        int oldValue = 111;
        setValue(nodeEngine1, partitionId, oldValue);

        long[] initialReplicaVersions = getDefaultReplicaVersions(nodeEngine1.getNode(), partitionId);
        assertBackupReplicaVersions(nodeEngine2.getNode(), partitionId, initialReplicaVersions);

        // set 2nd value
        int newValue = 222;
        setValue(nodeEngine1, partitionId, newValue);

        long[] lastReplicaVersions = getDefaultReplicaVersions(nodeEngine1.getNode(), partitionId);
        assertBackupReplicaVersions(nodeEngine2.getNode(), partitionId, lastReplicaVersions);

        // run a stale backup
        runBackup(nodeEngine2, oldValue, initialReplicaVersions, nodeEngine1.getThisAddress());

        long[] backupReplicaVersions = getDefaultReplicaVersions(nodeEngine2.getNode(), partitionId);
        assertArrayEquals(lastReplicaVersions, backupReplicaVersions);

        assertEquals(newValue, service.value.get());
    }

    private void runBackup(NodeEngine nodeEngine, int value, long[] replicaVersions, Address sender)
            throws InterruptedException {
        Backup
                backup = new Backup(new SampleBackupOperation(value), sender, replicaVersions, false);
        backup.setPartitionId(partitionId).setReplicaIndex(1).setNodeEngine(nodeEngine);
        nodeEngine.getOperationService().execute(backup);

        LatchOperation latchOp = new LatchOperation(1);
        nodeEngine.getOperationService().execute(latchOp.setPartitionId(partitionId));
        assertTrue(latchOp.latch.await(1, TimeUnit.MINUTES));
    }

    private void assertBackupReplicaVersions(final Node node, final int partitionId,
                                             final long[] expectedReplicaVersions) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long[] backupReplicaVersions = getDefaultReplicaVersions(node, partitionId);
                assertArrayEquals(expectedReplicaVersions, backupReplicaVersions);
            }
        });
    }

    private void setValue(NodeEngine nodeEngine, int partitionId, int value) {
        InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                .invokeOnPartition(new SampleBackupAwareOperation(value).setPartitionId(partitionId));
        future.join();
    }

    private static class ValueHolderService {
        static final String NAME = "value-holder-service";

        final AtomicLong value = new AtomicLong();
    }

    private static class SampleBackupAwareOperation extends Operation implements BackupAwareOperation {

        long value;

        SampleBackupAwareOperation() {
        }

        SampleBackupAwareOperation(long value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            ValueHolderService service = nodeEngine.getService(ValueHolderService.NAME);
            service.value.set(value);
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
            return new SampleBackupOperation(value);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readLong();
        }
    }

    private static class SampleBackupOperation extends Operation implements BackupOperation {

        final CountDownLatch latch = new CountDownLatch(1);

        long value;

        SampleBackupOperation() {
        }

        SampleBackupOperation(long value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {
            try {
                NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
                ValueHolderService service = nodeEngine.getService(ValueHolderService.NAME);
                service.value.set(value);
            } finally {
                latch.countDown();
            }
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readLong();
        }
    }

    private static class LatchOperation extends Operation {

        final CountDownLatch latch;

        private LatchOperation(int count) {
            latch = new CountDownLatch(count);
        }

        @Override
        public void run() throws Exception {
            latch.countDown();
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        public boolean validatesTarget() {
            return false;
        }
    }
}
