package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.properties.GroupProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IndeterminateOperationStateExceptionTest extends HazelcastTestSupport {

    private HazelcastInstance instance1;

    private HazelcastInstance instance2;

    @Before
    public void init() {
        Config config = new Config();
        config.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), String.valueOf(1000));
        config.setProperty(FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1, instance2);
    }

    @Test
    public void partitionInvocation_shouldFail_whenBackupTimeoutOccurs() throws InterruptedException, TimeoutException {
        dropOperationsBetween(instance1, instance2, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        int partitionId = getPartitionId(instance1);

        InternalOperationService operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new PrimaryOperation(), partitionId).invoke();
        try {
            future.get(2, TimeUnit.MINUTES);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IndeterminateOperationStateException);
        }
    }

    @Test
    public void partitionInvocation_shouldFail_whenPartitionPrimaryLeaves() throws InterruptedException, TimeoutException {
        int partitionId = getPartitionId(instance2);
        InternalOperationService operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new SilentOperation(), partitionId).invoke();
        spawn(new Runnable() {
            @Override
            public void run() {
                instance2.getLifecycleService().terminate();
            }
        });
        try {
            future.get(2, TimeUnit.MINUTES);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IndeterminateOperationStateException);
        }
    }


    @Test
    public void transaction_shouldFail_whenBackupTimeoutOccurs() throws InterruptedException, TimeoutException {
        dropOperationsBetween(instance1, instance2, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        dropOperationsBetween(instance2, instance1, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.BACKUP));

        String name = randomMapName();
        String key1 = generateKeyOwnedBy(instance1);
        String key2 = generateKeyOwnedBy(instance2);

        TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        try {
            TransactionalMap<Object, Object> map = context.getMap(name);
            map.put(key1, "value");
            map.put(key2, "value");

            context.commitTransaction();
            fail("Should fail with IndeterminateOperationStateException");
        } catch (IndeterminateOperationStateException e) {
            context.rollbackTransaction();
        }

        IMap<Object, Object> map = instance1.getMap(name);
        assertNull(map.get(key1));
        assertNull(map.get(key2));
    }

    @Test(expected = MemberLeftException.class)
    public void targetInvocation_shouldFailWithMemberLeftException_onTargetMemberLeave() throws Exception {
        InternalOperationService operationService = getNodeEngineImpl(instance1).getOperationService();
        Address target = getAddress(instance2);
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new SilentOperation(), target).invoke();

        spawn(new Runnable() {
            @Override
            public void run() {
                instance2.getLifecycleService().terminate();
            }
        });

        future.get(2, TimeUnit.MINUTES);
    }

    public static class PrimaryOperation extends Operation implements BackupAwareOperation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return true;
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
            return new BackupOperation();
        }

    }

    public static class BackupOperation extends Operation {

        @Override
        public void run() throws Exception {

        }

    }

    public static class SilentOperation extends Operation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

    }

}
