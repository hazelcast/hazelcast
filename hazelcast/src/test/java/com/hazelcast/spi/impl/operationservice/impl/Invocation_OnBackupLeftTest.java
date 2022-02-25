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
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.VOID;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Collections.newSetFromMap;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_OnBackupLeftTest extends HazelcastTestSupport {

    private static final Set<String> backupRunning = newSetFromMap(new ConcurrentHashMap<>());

    // we use 20 seconds so we don't get spurious build failures
    private static final int COMPLETION_TIMEOUT_SECONDS = 20;

    private OperationServiceImpl localOperationService;
    private HazelcastInstance remote;
    private HazelcastInstance local;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        Config config = new Config()
                // a long timeout is configured to verify that the fast timeout is kicking in
                .setProperty(ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), "100000")
                .setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "5");

        HazelcastInstance[] cluster = instanceFactory.newInstances(config, 2);
        local = cluster[0];
        remote = cluster[1];
        warmUpPartitions(local, remote);
        localOperationService = getOperationService(cluster[0]);
    }

    @Test
    public void whenPrimaryResponseNotYetReceived() {
        String backupId = newUnsecureUuidString();
        int responseDelaySeconds = 5;
        Operation op = new PrimaryOperation(backupId)
                .setPrimaryResponseDelaySeconds(responseDelaySeconds)
                .setPartitionId(getPartitionId(local));
        InvocationFuture f = (InvocationFuture) localOperationService.invokeOnPartition(op);

        waitForBackupRunning(backupId);

        remote.getLifecycleService().terminate();

        assertCompletesEventually(f, responseDelaySeconds + COMPLETION_TIMEOUT_SECONDS);
    }

    @Test
    public void whenPrimaryResponseAlreadyReceived() {
        String backupId = newUnsecureUuidString();
        Operation op = new PrimaryOperation(backupId)
                .setPartitionId(getPartitionId(local));
        InvocationFuture f = (InvocationFuture) localOperationService.invokeOnPartition(op);

        waitForPrimaryResponse(f);

        waitForBackupRunning(backupId);

        remote.getLifecycleService().terminate();

        assertCompletesEventually(f, COMPLETION_TIMEOUT_SECONDS);
    }

    private void waitForBackupRunning(final String backupId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(backupRunning.contains(backupId));
            }
        });
    }

    private void waitForPrimaryResponse(final InvocationFuture f) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals(VOID, f.invocation.pendingResponse);
            }
        });
    }

    static class PrimaryOperation extends Operation implements BackupAwareOperation {

        private String backupId;
        private int primaryResponseDelaySeconds;

        PrimaryOperation() {
        }

        PrimaryOperation(String backupId) {
            this.backupId = backupId;
        }

        public PrimaryOperation setPrimaryResponseDelaySeconds(int delaySeconds) {
            this.primaryResponseDelaySeconds = delaySeconds;
            return this;
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
            return new SlowBackupOperation(backupId);
        }

        @Override
        public Object getResponse() {
            // backups are send before the response is send, and we stall sending a response so we can trigger
            // a slow primary response while the backup is running.
            sleepSeconds(primaryResponseDelaySeconds);
            return super.getResponse();
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            out.writeString(backupId);
            out.writeInt(primaryResponseDelaySeconds);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            backupId = in.readString();
            primaryResponseDelaySeconds = in.readInt();
        }
    }

    // the backup operation is going to be terribly slow with sending a backup
    static class SlowBackupOperation extends Operation {
        private String backupId;

        SlowBackupOperation() {
        }

        SlowBackupOperation(String backupId) {
            this.backupId = backupId;
        }

        @Override
        public void run() throws Exception {
            backupRunning.add(backupId);
            sleepSeconds(120);
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            out.writeString(backupId);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            backupId = in.readString();
        }
    }
}
