package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Backup_Test extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private final static AtomicInteger BACKUP_COMPLETED_COUNT = new AtomicInteger();

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(3).newInstances();
        warmUpPartitions(cluster);
        hz = cluster[0];
    }

    @Test
    public void test() {
        InternalCompletableFuture f = getOperationService(hz).invokeOnPartition(new DummyUpdateOperation()
                .setPartitionId(getPartitionId(hz)));
        assertCompletesEventually(f);

        assertEquals(BACKUP_COMPLETED_COUNT.get(), 2);
    }

    private static class DummyUpdateOperation extends AbstractOperation implements BackupAwareOperation {
        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 2;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new DummyBackupOperation();
        }

        @Override
        public void run() throws Exception {
        }
    }

    private static class DummyBackupOperation extends AbstractOperation implements BackupOperation {
        @Override
        public void run() throws Exception {
            BACKUP_COMPLETED_COUNT.incrementAndGet();
        }
    }
}
