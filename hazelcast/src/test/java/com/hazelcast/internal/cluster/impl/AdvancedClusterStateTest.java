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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.longregister.LongRegisterService;
import com.hazelcast.internal.longregister.operations.AddAndGetOperation;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionLogRecord;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class AdvancedClusterStateTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void changeClusterState_shouldFail_whenMemberAdded_duringTx() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = TransactionOptions.getDefault();
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new Answer<Transaction>() {
            boolean started;

            @Override
            public Transaction answer(InvocationOnMock invocation) throws Throwable {
                Transaction tx = (Transaction) invocation.callRealMethod();
                return new DelegatingTransaction(tx) {
                    @Override
                    public void add(TransactionLogRecord record) {
                        super.add(record);
                        if (!started) {
                            started = true;
                            factory.newHazelcastInstance();
                        }
                    }
                };
            }
        });

        exception.expect(IllegalStateException.class);
        hz.getCluster().changeClusterState(ClusterState.PASSIVE, options);
    }

    @Test
    public void changeClusterState_shouldFail_whenMemberRemoved_duringTx() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = TransactionOptions.getDefault();
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new Answer<Transaction>() {
            boolean shutdown;

            @Override
            public Transaction answer(InvocationOnMock invocation) throws Throwable {
                Transaction tx = (Transaction) invocation.callRealMethod();
                return new DelegatingTransaction(tx) {
                    @Override
                    public void add(TransactionLogRecord record) {
                        super.add(record);
                        if (!shutdown) {
                            shutdown = true;
                            terminateInstance(instances[0]);
                        }
                    }
                };
            }
        });

        exception.expect(IllegalStateException.class);
        hz.getCluster().changeClusterState(ClusterState.PASSIVE, options);
    }

    @Test
    public void changeClusterState_shouldFail_whenStateIsAlreadyLocked() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];

        lockClusterState(hz);

        HazelcastInstance hz2 = instances[instances.length - 2];

        exception.expect(TransactionException.class);
        hz2.getCluster().changeClusterState(ClusterState.PASSIVE);
    }

    private void lockClusterState(HazelcastInstance hz) {
        Node node = getNode(hz);
        ClusterServiceImpl clusterService = node.getClusterService();
        int memberListVersion = clusterService.getMemberListVersion();
        long partitionStateStamp = node.getPartitionService().getPartitionStateStamp();
        long timeoutInMillis = TimeUnit.SECONDS.toMillis(60);
        ClusterStateManager clusterStateManager = clusterService.getClusterStateManager();
        clusterStateManager.lockClusterState(ClusterStateChange.from(ClusterState.FROZEN), node.getThisAddress(), UUID.randomUUID(),
                timeoutInMillis, memberListVersion, partitionStateStamp);
    }

    @Test
    public void changeClusterState_shouldFail_whenInitiatorDies_beforePrepare() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = TransactionOptions.getDefault().setTimeout(60, TimeUnit.SECONDS);
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new TransactionAnswer() {
            @Override
            protected void beforePrepare() {
                terminateInstance(hz);
            }
        });

        try {
            hz.getCluster().changeClusterState(ClusterState.FROZEN, options);
            fail("`changeClusterState` should throw HazelcastInstanceNotActiveException!");
        } catch (HazelcastInstanceNotActiveException ignored) {
        }

        assertClusterStateEventually(ClusterState.ACTIVE, instances[0], instances[1]);
    }

    @Test
    public void changeClusterState_shouldNotFail_whenInitiatorDies_afterPrepare() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = TransactionOptions.getDefault().setDurability(1);
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new TransactionAnswer() {
            @Override
            protected void afterPrepare() {
                terminateInstance(hz);
            }
        });

        try {
            hz.getCluster().changeClusterState(ClusterState.FROZEN, options);
            fail("This instance is terminated. Cannot commit the transaction!");
        } catch (HazelcastInstanceNotActiveException ignored) {
        }

        assertClusterStateEventually(ClusterState.FROZEN, instances[0], instances[1]);
    }

    @Test
    public void changeClusterState_shouldFail_withoutBackup_whenInitiatorDies_beforePrepare() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = new TransactionOptions().setDurability(0).setTimeout(60, TimeUnit.SECONDS);
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new TransactionAnswer() {
            @Override
            protected void beforePrepare() {
                terminateInstance(hz);
            }
        });

        try {
            hz.getCluster().changeClusterState(ClusterState.FROZEN, options);
            fail("This instance is terminated. Cannot commit the transaction!");
        } catch (HazelcastInstanceNotActiveException ignored) {
        }

        assertClusterStateEventually(ClusterState.ACTIVE, instances[0], instances[1]);
    }

    @Test
    public void changeClusterState_shouldFail_withoutBackup_whenInitiatorDies_afterPrepare() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = new TransactionOptions().setDurability(0).setTimeout(60, TimeUnit.SECONDS);
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new TransactionAnswer() {
            @Override
            protected void afterPrepare() {
                terminateInstance(hz);
            }
        });

        try {
            hz.getCluster().changeClusterState(ClusterState.FROZEN, options);
            fail("This instance is terminated. Cannot commit the transaction!");
        } catch (HazelcastInstanceNotActiveException ignored) {
        }

        assertClusterStateEventually(ClusterState.ACTIVE, instances[0], instances[1]);
    }

    @Test
    public void changeClusterState_shouldNotFail_whenNonInitiatorMemberDies_duringCommit() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[2];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        Address address = getAddress(instances[0]);

        TransactionOptions options = TransactionOptions.getDefault().setDurability(0);
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new TransactionAnswer() {
            @Override
            protected void afterPrepare() {
                terminateInstance(instances[0]);
            }
        });

        hz.getCluster().changeClusterState(ClusterState.FROZEN, options);

        assertClusterStateEventually(ClusterState.FROZEN, instances[2], instances[1]);

        instances[0] = factory.newHazelcastInstance(address);

        assertClusterSizeEventually(3, instances);
        assertClusterState(ClusterState.FROZEN, instances);
    }

    @Test
    public void changeClusterState_shouldFail_whenNonInitiatorMemberDies_beforePrepare() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        HazelcastInstance hz = instances[2];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        Address address = getAddress(instances[0]);

        TransactionOptions options = TransactionOptions.getDefault().setDurability(0);
        when(transactionManagerService.newAllowedDuringPassiveStateTransaction(options)).thenAnswer(new TransactionAnswer() {
            @Override
            protected void beforePrepare() {
                terminateInstance(instances[0]);
            }
        });

        try {
            hz.getCluster().changeClusterState(ClusterState.FROZEN, options);
            fail("A member is terminated. Cannot commit the transaction!");
        } catch (IllegalStateException ignored) {
        }

        assertClusterStateEventually(ClusterState.ACTIVE, instances[2], instances[1]);

        instances[0] = factory.newHazelcastInstance(address);

        assertClusterSizeEventually(3, instances);
        assertClusterState(ClusterState.ACTIVE, instances);
    }

    @Test
    public void changeClusterState_shouldFail_whenStartupIsNotCompleted() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        AtomicBoolean startupDone = new AtomicBoolean(false);

        HazelcastInstance instance = HazelcastInstanceFactory.newHazelcastInstance(new Config(), randomName(),
                new MockNodeContext(factory.getRegistry(), new Address("127.0.0.1", 5555)) {
                    @Override
                    public NodeExtension createNodeExtension(Node node) {
                        return new DefaultNodeExtension(node) {
                            @Override
                            public boolean isStartCompleted() {
                                return startupDone.get() && super.isStartCompleted();
                            }
                        };
                    }
                });

        try {
            instance.getCluster().changeClusterState(ClusterState.FROZEN);
            fail("Should not be able to change cluster state when startup is not completed yet!");
        } catch (IllegalStateException expected) {
        }

        startupDone.set(true);
        instance.getCluster().changeClusterState(ClusterState.FROZEN);
    }

    @Test
    public void clusterState_shouldBeTheSame_finally_onAllNodes() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        CountDownLatch latch = new CountDownLatch(instances.length);
        int iteration = 20;
        Random random = new Random();
        for (HazelcastInstance instance : instances) {
            new IterativeStateChangeThread(instance, iteration, latch, random).start();
        }

        assertOpenEventually(latch);
        assertClusterState(instances[0].getCluster().getClusterState(), instances);
    }

    @Test
    public void partitionTable_shouldBeFrozen_whenMemberLeaves_inFrozenState() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];

        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        Address owner = getNode(hz1).getThisAddress();
        int partitionId = getPartitionId(hz1);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);

        InternalPartition partition = getNode(hz2).getPartitionService().getPartition(partitionId);
        assertTrueAllTheTime(() -> assertEquals(owner, partition.getOwnerOrNull()), 3);
    }

    @Test
    public void partitionAssignment_shouldFail_whenTriggered_inNoMigrationState() {
        partitionAssignment_shouldFail_whenMigrationNotAllowed(ClusterState.NO_MIGRATION);
    }

    @Test
    public void partitionAssignment_shouldFail_whenTriggered_inFrozenState() {
        partitionAssignment_shouldFail_whenMigrationNotAllowed(ClusterState.FROZEN);
    }

    @Test
    public void partitionAssignment_shouldFail_whenTriggered_inPassiveState() {
        partitionAssignment_shouldFail_whenMigrationNotAllowed(ClusterState.PASSIVE);
    }

    private void partitionAssignment_shouldFail_whenMigrationNotAllowed(ClusterState state) {
        assertFalse(state.isMigrationAllowed());

        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];

        changeClusterStateEventually(hz2, state);

        InternalPartitionService partitionService = getPartitionService(hz1);
        exception.expect(IllegalStateException.class);
        partitionService.getPartitionOwnerOrWait(1);
    }

    @Test
    public void partitionInvocation_shouldFail_whenPartitionsNotAssigned_inNoMigrationState() throws InterruptedException {
        partitionInvocation_shouldFail_whenPartitionsNotAssigned_whenMigrationNotAllowed(ClusterState.NO_MIGRATION);
    }

    @Test
    public void partitionInvocation_shouldFail_whenPartitionsNotAssigned_inFrozenState() throws InterruptedException {
        partitionInvocation_shouldFail_whenPartitionsNotAssigned_whenMigrationNotAllowed(ClusterState.FROZEN);
    }

    @Test
    public void partitionInvocation_shouldFail_whenPartitionsNotAssigned_inPassiveState() throws InterruptedException {
        partitionInvocation_shouldFail_whenPartitionsNotAssigned_whenMigrationNotAllowed(ClusterState.PASSIVE);
    }

    private void partitionInvocation_shouldFail_whenPartitionsNotAssigned_whenMigrationNotAllowed(ClusterState state)
            throws InterruptedException {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];

        changeClusterStateEventually(hz2, state);

        OperationServiceImpl operationService = getNode(hz3).getNodeEngine().getOperationService();
        Operation op = new AddAndGetOperation(randomName(), 1);
        Future<Long> future = operationService
                .invokeOnPartition(LongRegisterService.SERVICE_NAME, op, 1);

        exception.expect(IllegalStateException.class);
        try {
            future.get();
        } catch (ExecutionException e) {
            // IllegalStateException should be cause of ExecutionException.
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Test
    public void test_eitherClusterStateChange_orPartitionInitialization_shouldBeSuccessful() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];

        assertClusterSizeEventually(instances.length, instances);

        InternalPartitionService partitionService = getNode(hz1).getPartitionService();
        long initialPartitionStateStamp = partitionService.getPartitionStateStamp();

        ClusterState newState = ClusterState.PASSIVE;

        Future future = spawn(() -> {
            try {
                changeClusterState(hz2, newState, initialPartitionStateStamp);
            } catch (Exception ignored) {
            }
        });

        partitionService.firstArrangement();

        future.get(2, TimeUnit.MINUTES);

        ClusterState currentState = hz2.getCluster().getClusterState();
        if (currentState == newState) {
            // if cluster state changed then partition state version should be equal to initial version
            for (HazelcastInstance instance : instances) {
                assertEquals(initialPartitionStateStamp, getPartitionService(instance).getPartitionStateStamp());
            }
        } else {
            assertEquals(ClusterState.ACTIVE, currentState);

            InternalPartition partition = partitionService.getPartition(0, false);
            if (partition.getOwnerOrNull() == null) {
                // if partition assignment failed then partition state version should be equal to initial version
                for (HazelcastInstance instance : instances) {
                    assertEquals(initialPartitionStateStamp, getPartitionService(instance).getPartitionStateStamp());
                }
            } else {
                // if cluster state change failed and partition assignment is done
                // then partition state version should be different.
                waitAllForSafeState(instances);
                long partitionStateStampNew = getPartitionService(hz1).getPartitionStateStamp();
                for (HazelcastInstance instance : instances) {
                    long partitionStateStamp = getPartitionService(instance).getPartitionStateStamp();
                    assertNotEquals("Instance: " + getAddress(instance), initialPartitionStateStamp, partitionStateStamp);
                    assertEquals("Instance: " + getAddress(instance), partitionStateStampNew, partitionStateStamp);
                }
            }
        }
    }

    @Test
    public void clusterState_shouldBeFrozen_whenMemberReJoins_inFrozenState() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];

        changeClusterStateEventually(hz2, ClusterState.FROZEN);

        Address owner = getNode(hz1).getThisAddress();
        terminateInstance(hz1);

        hz1 = factory.newHazelcastInstance(owner);
        assertClusterSizeEventually(3, hz2);

        assertClusterState(ClusterState.FROZEN, hz1, hz2, hz3);
    }

    @Test
    public void nodesCanShutDown_whenClusterState_frozen() {
        nodesCanShutDown_whenClusterState_changesTo(ClusterState.FROZEN);
    }

    @Test
    public void nodesCanShutDown_whenClusterState_passive() {
        nodesCanShutDown_whenClusterState_changesTo(ClusterState.PASSIVE);
    }

    @Test
    public void nodesCanShutDown_whenClusterState_noMigration() {
        nodesCanShutDown_whenClusterState_changesTo(ClusterState.NO_MIGRATION);
    }

    private void nodesCanShutDown_whenClusterState_changesTo(ClusterState state) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getMap(randomMapName()).put(1, 1); // for updating partition version

        changeClusterStateEventually(hz, state);

        List<HazelcastInstance> instanceList = new ArrayList<>(Arrays.asList(instances));
        while (!instanceList.isEmpty()) {
            HazelcastInstance instanceToShutdown = instanceList.remove(0);
            instanceToShutdown.shutdown();
            for (HazelcastInstance instance : instanceList) {
                assertClusterSizeEventually(instanceList.size(), instance);
            }
        }
    }

    @Test
    public void invocationShouldComplete_whenMemberReJoins_inFrozenState() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        Address owner = getNode(hz1).getThisAddress();
        String key = generateKeyOwnedBy(hz1);
        int partitionId = hz1.getPartitionService().getPartition(key).getPartitionId();

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);

        OperationServiceImpl operationService = getNode(hz3).getNodeEngine().getOperationService();
        Operation op = new AddAndGetOperation(key, 1);
        Future<Long> future = operationService.invokeOnPartition(LongRegisterService.SERVICE_NAME, op, partitionId);

        assertFalse(future.isDone());

        factory.newHazelcastInstance(owner);
        assertClusterSizeEventually(3, hz2);

        assertTrueEventually(() -> assertTrue(future.isDone()));

        // should not fail
        future.get();
    }

    private void changeClusterState(HazelcastInstance instance, ClusterState newState, long partitionStateStamp) {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(instance);
        MemberMap members = clusterService.getMembershipManager().getMemberMap();
        clusterService.getClusterStateManager().changeClusterState(ClusterStateChange.from(newState), members,
                partitionStateStamp, false);
    }

    private static TransactionManagerServiceImpl spyTransactionManagerService(HazelcastInstance hz) throws Exception {
        NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
        TransactionManagerServiceImpl transactionManagerService
                = (TransactionManagerServiceImpl) nodeEngine.getTransactionManagerService();
        TransactionManagerServiceImpl spiedTransactionManagerService = spy(transactionManagerService);

        Field transactionManagerServiceField = NodeEngineImpl.class.getDeclaredField("transactionManagerService");
        transactionManagerServiceField.setAccessible(true);
        transactionManagerServiceField.set(nodeEngine, spiedTransactionManagerService);
        return spiedTransactionManagerService;
    }

    private abstract static class TransactionAnswer implements Answer<Transaction> {
        @Override
        public Transaction answer(InvocationOnMock invocation) throws Throwable {
            Transaction tx = (Transaction) invocation.callRealMethod();
            return new DelegatingTransaction(tx) {
                @Override
                public void prepare() throws TransactionException {
                    beforePrepare();
                    super.prepare();
                    afterPrepare();
                }
            };
        }

        protected void beforePrepare() {
        }

        protected void afterPrepare() {
        }
    }

    private static class IterativeStateChangeThread extends Thread {
        private final HazelcastInstance instance;
        private final int iteration;
        private final CountDownLatch latch;
        private final Random random;

        IterativeStateChangeThread(HazelcastInstance instance, int iteration, CountDownLatch latch, Random random) {
            this.instance = instance;
            this.iteration = iteration;
            this.latch = latch;
            this.random = random;
        }

        public void run() {
            Cluster cluster = instance.getCluster();
            ClusterState newState = flipState(cluster.getClusterState());
            for (int i = 0; i < iteration; i++) {
                try {
                    cluster.changeClusterState(newState);
                } catch (TransactionException e) {
                    ignore(e);
                }
                newState = flipState(newState);
                sleepMillis(random.nextInt(5) + 1);
            }
            latch.countDown();
        }

        private static ClusterState flipState(ClusterState state) {
            state = state == ClusterState.ACTIVE ? ClusterState.FROZEN : ClusterState.ACTIVE;
            return state;
        }
    }

    private static class DelegatingTransaction implements Transaction {

        final Transaction tx;

        DelegatingTransaction(Transaction tx) {
            this.tx = tx;
        }

        @Override
        public void begin() throws IllegalStateException {
            tx.begin();
        }

        @Override
        public void prepare() throws TransactionException {
            tx.prepare();
        }

        @Override
        public void commit() throws TransactionException, IllegalStateException {
            tx.commit();
        }

        @Override
        public void rollback() throws IllegalStateException {
            tx.rollback();
        }

        @Override
        public UUID getTxnId() {
            return tx.getTxnId();
        }

        @Override
        public State getState() {
            return tx.getState();
        }

        @Override
        public long getTimeoutMillis() {
            return tx.getTimeoutMillis();
        }

        @Override
        public void add(TransactionLogRecord record) {
            tx.add(record);
        }

        @Override
        public void remove(Object key) {
            tx.remove(key);
        }

        @Override
        public TransactionLogRecord get(Object key) {
            return tx.get(key);
        }

        @Override
        public UUID getOwnerUuid() {
            return tx.getOwnerUuid();
        }

        @Override
        public TransactionType getTransactionType() {
            return tx.getTransactionType();
        }

        @Override
        public boolean isOriginatedFromClient() {
            return tx.isOriginatedFromClient();
        }
    }

    public static void changeClusterStateEventually(HazelcastInstance hz, ClusterState newState) {
        final Cluster cluster = hz.getCluster();
        long timeout = TimeUnit.SECONDS.toMillis(ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        Throwable t = null;
        while (timeout > 0) {
            long start = Clock.currentTimeMillis();
            try {
                cluster.changeClusterState(newState);
                return;
            } catch (Throwable e) {
                t = e;
            }
            sleepMillis(500);
            long end = Clock.currentTimeMillis();
            timeout -= (end - start);
        }
        throw ExceptionUtil.rethrow(t);
    }
}
