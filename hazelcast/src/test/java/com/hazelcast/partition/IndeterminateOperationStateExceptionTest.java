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

package com.hazelcast.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.impl.SpiDataSerializerHook.F_ID;
import static com.hazelcast.spi.properties.ClusterProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndeterminateOperationStateExceptionTest extends HazelcastTestSupport {

    private HazelcastInstance instance1;

    private HazelcastInstance instance2;

    private void setup(boolean enableFailOnIndeterminateOperationState) {
        Config config = new Config();
        config.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), String.valueOf(3000));
        if (enableFailOnIndeterminateOperationState) {
            config.setProperty(FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));
        }

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1, instance2);
    }

    @Test
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledGlobally() throws InterruptedException, TimeoutException {
        setup(true);

        dropOperationsBetween(instance1, instance2, F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        int partitionId = getPartitionId(instance1);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
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
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocation() throws InterruptedException, TimeoutException {
        setup(false);

        dropOperationsBetween(instance1, instance2, F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        int partitionId = getPartitionId(instance1);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new PrimaryOperation(), partitionId)
                .setFailOnIndeterminateOperationState(true)
                .invoke();
        try {
            future.get(2, TimeUnit.MINUTES);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IndeterminateOperationStateException);
        }
    }

    @Test
    public void partitionInvocation_shouldFail_whenPartitionPrimaryLeaves() throws InterruptedException, TimeoutException {
        setup(true);

        int partitionId = getPartitionId(instance2);
        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new SilentOperation(), partitionId).invoke();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(instance2.getUserContext().containsKey(SilentOperation.EXECUTION_STARTED));
            }
        });

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
    public void readOnlyPartitionInvocation_shouldSucceed_whenPartitionPrimaryLeaves()
            throws InterruptedException, TimeoutException, ExecutionException {
        setup(true);

        dropOperationsBetween(instance2, instance1, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.NORMAL_RESPONSE));

        int partitionId = getPartitionId(instance2);
        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Boolean> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new DummyReadOperation(), partitionId).invoke();
        spawn(new Runnable() {
            @Override
            public void run() {
                instance2.getLifecycleService().terminate();
            }
        });
        boolean response = future.get(2, TimeUnit.MINUTES);
        assertTrue(response);
        assertEquals(getAddress(instance1), instance1.getUserContext().get(DummyReadOperation.LAST_INVOCATION_ADDRESS));
    }

    @Test
    public void transaction_shouldFail_whenBackupTimeoutOccurs() {
        setup(true);

        dropOperationsBetween(instance1, instance2, F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        dropOperationsBetween(instance2, instance1, F_ID, singletonList(SpiDataSerializerHook.BACKUP));

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
        setup(true);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        Address target = getAddress(instance2);
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new SilentOperation(), target).invoke();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(instance2.getUserContext().containsKey(SilentOperation.EXECUTION_STARTED));
            }
        });

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
        public static final String EXECUTION_DONE = "execution-done";

        @Override
        public void run() throws Exception {
            getNodeEngine().getHazelcastInstance().getUserContext().put(EXECUTION_DONE, new Object());
        }
    }

    public static class SilentOperation extends Operation {

        static final String EXECUTION_STARTED = "execution-started";

        @Override
        public void run() throws Exception {
            getNodeEngine().getHazelcastInstance().getUserContext().put(EXECUTION_STARTED, new Object());
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

    }

    public static class DummyReadOperation extends Operation implements ReadonlyOperation {

        static final String LAST_INVOCATION_ADDRESS = "last-invocation-address";

        @Override
        public void run() throws Exception {
            Address address = getNodeEngine().getThisAddress();
            getNodeEngine().getHazelcastInstance().getUserContext().put(LAST_INVOCATION_ADDRESS, address);
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return true;
        }

    }

}
