package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.MigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MasterSplitTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void test_migrationFailsOnMasterMismatch_onSource() throws InterruptedException {
        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();

        warmUpPartitions(member1, member2);

        MigrationInfo migration = createMigrationInfo(member1, member2);

        int partitionStateVersion = getPartitionService(member1).getPartitionStateVersion();

        Operation op = new MigrationRequestOperation(migration, partitionStateVersion);

        InvocationBuilder invocationBuilder = getOperationServiceImpl(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));
        Future future = invocationBuilder.invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    private MigrationInfo createMigrationInfo(HazelcastInstance master, HazelcastInstance nonMaster) {
        MigrationInfo migration = new MigrationInfo(getPartitionId(nonMaster), getAddress(nonMaster), getNode(nonMaster).getThisUuid(),
                getAddress(master), getNode(master).getThisUuid(), 0, 1, -1, 0);
        migration.setMaster(getAddress(nonMaster));
        return migration;
    }

    @Test
    public void test_migrationFailsOnMasterMismatch_onDestination() throws InterruptedException {
        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();

        warmUpPartitions(member1, member2);

        MigrationInfo migration = createMigrationInfo(member1, member2);

        int partitionStateVersion = getPartitionService(member1).getPartitionStateVersion();

        Operation op = new MigrationOperation(migration, new long[MAX_BACKUP_COUNT], Collections.<Operation>emptyList(), partitionStateVersion);

        InvocationBuilder invocationBuilder = getOperationServiceImpl(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));
        Future future = invocationBuilder.invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void test_fetchPartitionStateFailsOnMasterMismatch()
            throws InterruptedException {
        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();
        HazelcastInstance member3 = factory.newHazelcastInstance();

        warmUpPartitions(member1, member2, member3);

        InternalCompletableFuture<Object> future = getOperationServiceImpl(member2)
                .createInvocationBuilder(SERVICE_NAME, new FetchPartitionStateOperation(), getAddress(member3))
                .setTryCount(Integer.MAX_VALUE).setCallTimeout(Long.MAX_VALUE).invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

}
