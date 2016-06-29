package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
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

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * A unit test that verifies that there is no need to explicitly pass the callerUuid.
 *
 * See the following PR for more detail:
 * https://github.com/hazelcast/hazelcast/pull/8173
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Backup_CallerUuidTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private final static AtomicReference CALLER_UUID = new AtomicReference();

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(cluster);
        hz = cluster[0];
    }

    @Test
    public void test() {
        InternalCompletableFuture f = getOperationService(hz).invokeOnPartition(new DummyUpdateOperation()
                .setPartitionId(getPartitionId(hz)));
        assertCompletesEventually(f);

        assertEquals(CALLER_UUID.get(), hz.getCluster().getLocalMember().getUuid());
    }

    private static class DummyUpdateOperation extends Operation implements BackupAwareOperation {
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
            return new DummyBackupOperation();
        }

        @Override
        public void run() throws Exception {
        }
    }

    private static class DummyBackupOperation extends Operation implements BackupOperation {
        @Override
        public void run() throws Exception {
            CALLER_UUID.set(getCallerUuid());
        }
    }
}
