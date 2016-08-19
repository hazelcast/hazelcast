package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class StaleReadDuringMigrationTest extends HazelcastTestSupport {

    @Test
    public void testReadOperationFailsWhenStaleReadDisabledDuringMigration()
            throws ExecutionException, InterruptedException {
        final Config config = new Config();
        config.setProperty(GroupProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION.getName(), "true");

        final InternalCompletableFuture<Boolean> future = invokeOperation(config);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PartitionMigratingException);
        }
    }

    @Test
    public void testReadOperationSucceedsWhenStaleReadEnabledDuringMigration()
            throws ExecutionException, InterruptedException {
        final InternalCompletableFuture<Boolean> future = invokeOperation(new Config());

        assertTrue(future.get());
    }

    private InternalCompletableFuture<Boolean> invokeOperation(final Config config) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        warmUpPartitions(instance);

        final int partitionId = 0;
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);
        partition.setMigrating(true);

        final InternalOperationService operationService = getOperationService(instance);
        final InvocationBuilder invocationBuilder = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new DummyOperation(), partitionId);
        return invocationBuilder.invoke();
    }

    private static class DummyOperation extends Operation implements ReadonlyOperation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return true;
        }

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            if (throwable instanceof PartitionMigratingException) {
                return ExceptionAction.THROW_EXCEPTION;
            }
            return super.onInvocationException(throwable);
        }
    }
}
