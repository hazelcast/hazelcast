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
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.partition.InternalPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.impl.operationservice.impl.DummyBackupAwareOperation.backupCompletedMap;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.lang.Math.min;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationBackupHandlerTest extends HazelcastTestSupport {

    public static final boolean FORCE_SYNC_ENABLED = true;
    public static final boolean FORCE_SYNC_DISABLED = false;
    public static final boolean BACKPRESSURE_ENABLED = true;
    public static final boolean BACKPRESSURE_DISABLED = false;

    private HazelcastInstance local;
    private OperationServiceImpl operationService;
    private OperationBackupHandler backupHandler;
    int BACKUPS = 4;

    @BeforeClass
    @AfterClass
    public static void cleanup() throws Exception {
        backupCompletedMap.clear();
    }

    public void setup(boolean backPressureEnabled) {
        Config config = new Config()
                .setProperty(ClusterProperty.BACKPRESSURE_ENABLED.getName(), String.valueOf(backPressureEnabled))
                .setProperty(ClusterProperty.BACKPRESSURE_SYNCWINDOW.getName(), "1");

        // we create a nice big cluster so that we have enough backups.
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(BACKUPS + 1).newInstances(config);
        warmUpPartitions(cluster);
        local = cluster[0];

        operationService = (OperationServiceImpl) getOperationService(local);
        backupHandler = operationService.backupHandler;
    }

    // ============================ syncBackups =================================

    @Test
    public void syncBackups_whenForceSyncEnabled() {
        setup(BACKPRESSURE_ENABLED);

        // when force sync enabled, we sum tot sync and asyncs
        assertEquals(0, backupHandler.syncBackups(0, 0, FORCE_SYNC_ENABLED));
        assertEquals(1, backupHandler.syncBackups(1, 0, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.syncBackups(0, 0, FORCE_SYNC_ENABLED));
        assertEquals(3, backupHandler.syncBackups(1, 2, FORCE_SYNC_ENABLED));

        // checking to see what happens when we are at or above the maximum number of backups
        assertEquals(BACKUPS, backupHandler.syncBackups(BACKUPS, 0, FORCE_SYNC_ENABLED));
        assertEquals(BACKUPS, backupHandler.syncBackups(BACKUPS + 1, 0, FORCE_SYNC_ENABLED));
        assertEquals(BACKUPS, backupHandler.syncBackups(BACKUPS, 1, FORCE_SYNC_ENABLED));
    }

    @Test
    public void syncBackups_whenForceSyncDisabled() {
        setup(BACKPRESSURE_ENABLED);

        // when force-sync disabled, we only look at the sync backups
        assertEquals(0, backupHandler.syncBackups(0, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.syncBackups(1, 0, FORCE_SYNC_DISABLED));
        assertEquals(0, backupHandler.syncBackups(0, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.syncBackups(1, 1, FORCE_SYNC_DISABLED));

        // checking to see what happens when we are at or above the maximum number of backups
        assertEquals(BACKUPS, backupHandler.syncBackups(BACKUPS, 0, FORCE_SYNC_DISABLED));
        assertEquals(BACKUPS, backupHandler.syncBackups(BACKUPS + 1, 0, FORCE_SYNC_DISABLED));
    }

    // ============================ asyncBackups =================================

    @Test
    public void asyncBackups_whenForceSyncDisabled() {
        setup(BACKPRESSURE_ENABLED);

        // when forceSync disabled, only the async matters
        assertEquals(0, backupHandler.asyncBackups(0, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.asyncBackups(0, 1, FORCE_SYNC_DISABLED));
        assertEquals(0, backupHandler.asyncBackups(2, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.asyncBackups(2, 1, FORCE_SYNC_DISABLED));

        // see what happens when we reach maximum number of backups
        assertEquals(BACKUPS, backupHandler.asyncBackups(0, BACKUPS + 1, FORCE_SYNC_DISABLED));
    }

    @Test
    public void asyncBackups_whenForceSyncEnabled() {
        setup(true);

        // when forceSync is enabled, then async should always be 0
        assertEquals(0, backupHandler.asyncBackups(0, 0, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.asyncBackups(0, 1, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.asyncBackups(2, 0, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.asyncBackups(2, 1, FORCE_SYNC_ENABLED));

        // see what happens when we reach maximum number of backups
        assertEquals(0, backupHandler.asyncBackups(0, BACKUPS + 1, FORCE_SYNC_ENABLED));
    }

    // ============================ backup =================================

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenNegativeSyncBackupCount() throws Exception {
        setup(BACKPRESSURE_ENABLED);

        BackupAwareOperation op = makeOperation(-1, 0);
        backupHandler.sendBackups0(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenTooLargeSyncBackupCount() throws Exception {
        setup(BACKPRESSURE_ENABLED);

        BackupAwareOperation op = makeOperation(MAX_BACKUP_COUNT + 1, 0);
        backupHandler.sendBackups0(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenNegativeAsyncBackupCount() throws Exception {
        setup(BACKPRESSURE_ENABLED);

        BackupAwareOperation op = makeOperation(0, -1);
        backupHandler.sendBackups0(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenTooLargeAsyncBackupCount() throws Exception {
        setup(BACKPRESSURE_ENABLED);

        BackupAwareOperation op = makeOperation(0, MAX_BACKUP_COUNT + 1);
        backupHandler.sendBackups0(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenTooLargeSumOfSyncAndAsync() throws Exception {
        setup(BACKPRESSURE_ENABLED);

        BackupAwareOperation op = makeOperation(1, MAX_BACKUP_COUNT);
        backupHandler.sendBackups0(op);
    }

    @Test
    public void backup_whenBackpressureDisabled() throws Exception {
        setup(BACKPRESSURE_DISABLED);

        assertBackup(0, 0, 0);
        assertBackup(0, 1, 0);
        assertBackup(2, 0, 2);
        assertBackup(2, 1, 2);

        assertBackup(BACKUPS, 0, BACKUPS);
        assertBackup(BACKUPS + 1, 0, BACKUPS);
    }

    @Test
    public void backup_whenBackpressureEnabled() throws Exception {
        setup(BACKPRESSURE_ENABLED);

        // when forceSync is enabled, then number of backups=sync+async
        assertBackup(0, 0, 0);
        assertBackup(0, 1, 1);
        assertBackup(2, 0, 2);
        assertBackup(2, 1, 3);

        // see what happens when we reach the maximum number of backups
        assertBackup(0, BACKUPS, BACKUPS);
        assertBackup(0, BACKUPS + 1, BACKUPS);
    }

    private void assertBackup(final int syncBackups, final int asyncBackups, int expectedResult) throws Exception {
        final DummyBackupAwareOperation backupAwareOp = makeOperation(syncBackups, asyncBackups);

        int result = backupHandler.sendBackups0(backupAwareOp);

        assertEquals(expectedResult, result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Integer completed = backupCompletedMap.get(backupAwareOp.backupKey);
                if (completed == null) {
                    completed = 0;
                }
                int totalBackups = min(BACKUPS, syncBackups + asyncBackups);
                assertEquals(new Integer(totalBackups), completed);
            }
        });
    }

    private DummyBackupAwareOperation makeOperation(int syncBackupCount, int asyncBackupCount) {
        DummyBackupAwareOperation operation = new DummyBackupAwareOperation();
        operation.syncBackupCount = syncBackupCount;
        operation.asyncBackupCount = asyncBackupCount;
        operation.backupKey = randomUUID().toString();
        setCallerAddress(operation, getAddress(local));
        operation.setPartitionId(getPartitionId(local));
        return operation;
    }
}
