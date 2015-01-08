package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.partition.InternalPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class OperationBackupHandlerTest extends HazelcastTestSupport {

    public static final boolean FORCE_SYNC_ENABLED = true;
    public static final boolean FORCE_SYNC_DISABLED = false;
    private HazelcastInstance local;
    private OperationServiceImpl operationService;
    private OperationBackupHandler backupHandler;
    int BACKUPS = 4;

    @BeforeClass
    @AfterClass
    public static void cleanup() throws Exception {
        DummyBackupAwareOperation.backupCompletedMap.clear();
    }

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true")
                .setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "1");

        // we create a nice big cluster so that we have enough backups.
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(BACKUPS + 1).newInstances(config);
        warmUpPartitions(cluster);
        local = cluster[0];

        operationService = (OperationServiceImpl) getOperationService(local);
        backupHandler = operationService.operationBackupHandler;
    }

    // ============================ actualSyncBackups =================================

    @Test
    public void actualSyncBackups_whenForceSyncEnabled() {
        // when force sync enabled, we sum tot sync and asyncs
        assertEquals(0, backupHandler.actualSyncBackups(0, 0, FORCE_SYNC_ENABLED));
        assertEquals(1, backupHandler.actualSyncBackups(1, 0, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.actualSyncBackups(0, 0, FORCE_SYNC_ENABLED));
        assertEquals(3, backupHandler.actualSyncBackups(1, 2, FORCE_SYNC_ENABLED));

        // checking to see what happens when we are at or above the maximum number of backups
        assertEquals(BACKUPS, backupHandler.actualSyncBackups(BACKUPS, 0, FORCE_SYNC_ENABLED));
        assertEquals(BACKUPS, backupHandler.actualSyncBackups(BACKUPS + 1, 0, FORCE_SYNC_ENABLED));
        assertEquals(BACKUPS, backupHandler.actualSyncBackups(BACKUPS, 1, FORCE_SYNC_ENABLED));
    }

    @Test
    public void actualSyncBackups_whenForceSyncDisabled() {
        // when force-sync disabled, we only look at the sync backups
        assertEquals(0, backupHandler.actualSyncBackups(0, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.actualSyncBackups(1, 0, FORCE_SYNC_DISABLED));
        assertEquals(0, backupHandler.actualSyncBackups(0, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.actualSyncBackups(1, 1, FORCE_SYNC_DISABLED));

        // checking to see what happens when we are at or above the maximum number of backups
        assertEquals(BACKUPS, backupHandler.actualSyncBackups(BACKUPS, 0, FORCE_SYNC_DISABLED));
        assertEquals(BACKUPS, backupHandler.actualSyncBackups(BACKUPS + 1, 0, FORCE_SYNC_DISABLED));
    }

    // ============================ actualAsyncBackups =================================

    @Test
    public void actualAsyncBackups_whenForceSyncDisabled() {
        // when forceSync disabled, only the async matters
        assertEquals(0, backupHandler.actualAsyncBackups(0, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.actualAsyncBackups(0, 1, FORCE_SYNC_DISABLED));
        assertEquals(0, backupHandler.actualAsyncBackups(2, 0, FORCE_SYNC_DISABLED));
        assertEquals(1, backupHandler.actualAsyncBackups(2, 1, FORCE_SYNC_DISABLED));

        // see what happens when we reach maximum number of backups
        assertEquals(BACKUPS, backupHandler.actualAsyncBackups(0, BACKUPS + 1, FORCE_SYNC_DISABLED));
    }

    @Test
    public void actualAsyncBackups_whenForceSyncEnabled() {
        // when forceSync is enabled, then async should always be 0
        assertEquals(0, backupHandler.actualAsyncBackups(0, 0, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.actualAsyncBackups(0, 1, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.actualAsyncBackups(2, 0, FORCE_SYNC_ENABLED));
        assertEquals(0, backupHandler.actualAsyncBackups(2, 1, FORCE_SYNC_ENABLED));

        // see what happens when we reach maximum number of backups
        assertEquals(0, backupHandler.actualAsyncBackups(0, BACKUPS + 1, FORCE_SYNC_ENABLED));
    }

    // ============================ backup =================================

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenNegativeSyncBackupCount() throws Exception {
        BackupAwareOperation op = makeOperation(-1, 0, false);
        backupHandler.backup(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenTooLargeSyncBackupCount() throws Exception {
        BackupAwareOperation op = makeOperation(MAX_BACKUP_COUNT + 1, 0, false);
        backupHandler.backup(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenNegativeAsyncBackupCount() throws Exception {
        BackupAwareOperation op = makeOperation(0, -1, false);
        backupHandler.backup(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenTooLargeAsyncBackupCount() throws Exception {
        BackupAwareOperation op = makeOperation(0, MAX_BACKUP_COUNT + 1, false);
        backupHandler.backup(op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backup_whenTooLargeSumOfSyncAndAsync() throws Exception {
        BackupAwareOperation op = makeOperation(1, MAX_BACKUP_COUNT, false);
        backupHandler.backup(op);
    }

    @Test
    public void backup_whenForceSyncDisabled() throws Exception {
        assertBackup(0, 0, FORCE_SYNC_DISABLED, 0);
        assertBackup(0, 1, FORCE_SYNC_DISABLED, 0);
        assertBackup(2, 0, FORCE_SYNC_DISABLED, 2);
        assertBackup(2, 1, FORCE_SYNC_DISABLED, 2);

        assertBackup(BACKUPS, 0, FORCE_SYNC_DISABLED, BACKUPS);
        assertBackup(BACKUPS + 1, 0, FORCE_SYNC_DISABLED, BACKUPS);
    }

    @Test
    public void backup_whenForceSyncEnabled() throws Exception {
        // when forceSync is enabled, then number of backups=sync+async
        assertBackup(0, 0, FORCE_SYNC_ENABLED, 0);
        assertBackup(0, 1, FORCE_SYNC_ENABLED, 1);
        assertBackup(2, 0, FORCE_SYNC_ENABLED, 2);
        assertBackup(2, 1, FORCE_SYNC_ENABLED, 3);

        // see what happens when we reach the maximum number of backups
        assertBackup(0, BACKUPS, FORCE_SYNC_ENABLED, BACKUPS);
        assertBackup(0, BACKUPS + 1, FORCE_SYNC_ENABLED, BACKUPS);
    }

    private void assertBackup(final int syncBackups, final int asyncBackups, boolean forceSync, int expectedResult) throws Exception {
        final DummyBackupAwareOperation backupAwareOp = makeOperation(syncBackups, asyncBackups, forceSync);

        int result = backupHandler.backup(backupAwareOp);

        assertEquals(expectedResult, result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Integer completed = DummyBackupAwareOperation.backupCompletedMap.get(backupAwareOp.backupKey);
                if (completed == null) {
                    completed = 0;
                }
                int totalBackups = min(BACKUPS, syncBackups + asyncBackups);
                assertEquals(new Integer(totalBackups), completed);
            }
        });
    }

    private DummyBackupAwareOperation makeOperation(int syncBackupCount, int asyncBackupCount, boolean forceSync) {
        DummyBackupAwareOperation operation = new DummyBackupAwareOperation();
        operation.syncBackupCount = syncBackupCount;
        operation.asyncBackupCount = asyncBackupCount;
        operation.backupKey = UUID.randomUUID().toString();
        setCallerAddress(operation, getAddress(local));
        operation.setPartitionId(getPartitionId(local));
        operation.setSyncForced(forceSync);
        return operation;
    }
}
