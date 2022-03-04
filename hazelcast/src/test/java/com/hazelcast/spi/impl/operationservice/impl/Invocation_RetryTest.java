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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_RetryTest extends HazelcastTestSupport {

    private static final int NUMBER_OF_INVOCATIONS = 100;

    @Test
    public void whenPartitionTargetMemberDiesThenOperationSendToNewPartitionOwner() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        OperationService service = getOperationService(local);
        Operation op = new PartitionTargetOperation();
        Future future = service.createInvocationBuilder(null, op, getPartitionId(remote))
                .setCallTimeout(30000)
                .invoke();
        sleepSeconds(1);

        remote.shutdown();

        // future.get() should work without a problem because the operation should be re-targeted at the newest owner
        // for the given partition
        future.get();
    }

    @Test(expected = MemberLeftException.class)
    public void whenTargetMemberDiesThenOperationAbortedWithMembersLeftException() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        OperationService service = getOperationService(local);
        Operation op = new TargetOperation();
        Address address = new Address(remote.getCluster().getLocalMember().getSocketAddress());
        Future future = service.createInvocationBuilder(null, op, address).invoke();
        sleepSeconds(1);

        remote.getLifecycleService().terminate();

        future.get();
    }

    @Test
    public void testNoStuckInvocationsWhenRetriedMultipleTimes() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance(config);
        HazelcastInstance remote = factory.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        NodeEngineImpl localNodeEngine = getNodeEngineImpl(local);
        NodeEngineImpl remoteNodeEngine = getNodeEngineImpl(remote);
        final OperationServiceImpl operationService = (OperationServiceImpl) localNodeEngine.getOperationService();

        NonResponsiveOperation op = new NonResponsiveOperation();
        op.setValidateTarget(false);
        op.setPartitionId(1);

        InvocationFuture future = (InvocationFuture) operationService.invokeOnTarget(null, op, remoteNodeEngine.getThisAddress());

        Field invocationField = InvocationFuture.class.getDeclaredField("invocation");
        invocationField.setAccessible(true);
        Invocation invocation = (Invocation) invocationField.get(future);

        invocation.notifyError(new RetryableHazelcastException());
        invocation.notifyError(new RetryableHazelcastException());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Iterator<Invocation> invocations = operationService.invocationRegistry.iterator();
                assertFalse(invocations.hasNext());
            }
        });
    }

    @Test
    public void invocationShouldComplete_whenRetried_DuringShutdown() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        HazelcastInstance hz3 = factory.newHazelcastInstance();
        waitAllForSafeState(hz1, hz2, hz3);

        OperationServiceImpl operationService = getNodeEngineImpl(hz1).getOperationService();

        Future[] futures = new Future[NUMBER_OF_INVOCATIONS];
        for (int i = 0; i < NUMBER_OF_INVOCATIONS; i++) {
            int partitionId = getRandomPartitionId(hz2);

            futures[i] = operationService.createInvocationBuilder(null, new RetryingOperation(), partitionId)
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(Long.MAX_VALUE)
                    .invoke();
        }

        hz3.getLifecycleService().terminate();
        hz1.getLifecycleService().terminate();

        for (Future future : futures) {
            try {
                future.get(2, TimeUnit.MINUTES);
            } catch (ExecutionException ignored) {
            } catch (TimeoutException e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void invocationShouldComplete_whenOperationsPending_DuringShutdown() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        OperationServiceImpl operationService = nodeEngine.getOperationService();

        operationService.invokeOnPartition(new SleepingOperation(Long.MAX_VALUE).setPartitionId(0));
        sleepSeconds(1);

        Future[] futures = new Future[NUMBER_OF_INVOCATIONS];
        for (int i = 0; i < NUMBER_OF_INVOCATIONS; i++) {
            futures[i] = operationService.createInvocationBuilder(null, new RetryingOperation(), 0)
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(Long.MAX_VALUE)
                    .invoke();
        }

        hz.getLifecycleService().terminate();

        for (Future future : futures) {
            try {
                future.get(2, TimeUnit.MINUTES);
            } catch (ExecutionException ignored) {
            } catch (TimeoutException e) {
                fail(e.getMessage());
            }
        }
    }

    private static int getRandomPartitionId(HazelcastInstance hz) {
        warmUpPartitions(hz);

        InternalPartitionService partitionService = getPartitionService(hz);
        IPartition[] partitions = partitionService.getPartitions();
        Collections.shuffle(Arrays.asList(partitions));

        for (IPartition p : partitions) {
            if (p.isLocal()) {
                return p.getPartitionId();
            }
        }
        throw new RuntimeException("No local partitions are found for hz: " + hz.getName());
    }

    /**
     * Non-responsive operation.
     */
    public static class NonResponsiveOperation extends Operation {

        @Override
        public void run() throws InterruptedException {
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }

    /**
     * Operation send to a specific member.
     */
    private static class TargetOperation extends Operation {

        @Override
        public void run() throws InterruptedException {
            Thread.sleep(10000);
        }
    }

    /**
     * Operation send to a specific target partition.
     */
    private static class PartitionTargetOperation extends Operation implements PartitionAwareOperation {

        @Override
        public void run() throws InterruptedException {
            Thread.sleep(5000);
        }
    }

    private static class RetryingOperation extends Operation implements AllowedDuringPassiveState {

        @Override
        public void run() throws Exception {
            throw new RetryableHazelcastException();
        }
    }

    private static class SleepingOperation extends Operation implements AllowedDuringPassiveState {

        private long sleepMillis;

        SleepingOperation() {
        }

        SleepingOperation(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(sleepMillis);
        }
    }
}
