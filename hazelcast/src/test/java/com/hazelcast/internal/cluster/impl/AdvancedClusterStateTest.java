/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomiclong.operations.AddAndGetOperation;
import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionLogRecord;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AdvancedClusterStateTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void changeClusterState_shouldFail_whenMemberAdded_duringTx() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance[] instances = new HazelcastInstance[3];
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[instances.length - 1];

        lockClusterState(hz);

        final HazelcastInstance hz2 = instances[instances.length - 2];

        exception.expect(TransactionException.class);
        hz2.getCluster().changeClusterState(ClusterState.PASSIVE);
    }

    private void lockClusterState(HazelcastInstance hz) {
        final Node node = getNode(hz);
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        long timeoutInMillis = TimeUnit.SECONDS.toMillis(60);
        ClusterStateManager clusterStateManager = node.clusterService.getClusterStateManager();
        clusterStateManager.lockClusterState(ClusterStateChange.from(ClusterState.FROZEN), node.getThisAddress(), "fakeTxn", timeoutInMillis, partitionStateVersion);
    }

    @Test
    public void changeClusterState_shouldFail_whenInitiatorDies_beforePrepare() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[instances.length - 1];
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[instances.length - 1];
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[instances.length - 1];
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[instances.length - 1];
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[2];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        final Address address = getAddress(instances[0]);

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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        assertClusterSizeEventually(instances.length, instances);

        final HazelcastInstance hz = instances[2];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        final Address address = getAddress(instances[0]);

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

        final AtomicBoolean startupDone = new AtomicBoolean(false);

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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
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
        HazelcastInstance hz3 = instances[2];
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        final Address owner = getNode(hz1).getThisAddress();
        int partitionId = getPartitionId(hz1);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);

        final InternalPartition partition = getNode(hz2).getPartitionService().getPartition(partitionId);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(owner, partition.getOwnerOrNull());
            }
        }, 3);
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
        HazelcastInstance hz3 = instances[2];

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
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];

        changeClusterStateEventually(hz2, state);

        InternalOperationService operationService = getNode(hz3).getNodeEngine().getOperationService();
        Operation op = new AddAndGetOperation(randomName(), 1);
        Future<Long> future = operationService
                .invokeOnPartition(AtomicLongService.SERVICE_NAME, op, 1);

        exception.expect(IllegalStateException.class);
        try {
            future.get();
        } catch (ExecutionException e) {
            // IllegalStateException should be cause of ExecutionException.
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Test
    public void test_eitherClusterStateChange_orPartitionInitialization_shouldBeSuccessful()
            throws Exception {

        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        final HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];

        assertClusterSizeEventually(instances.length, instances);

        final InternalPartitionService partitionService = getNode(hz1).getPartitionService();
        final int initialPartitionStateVersion = partitionService.getPartitionStateVersion();

        final ClusterState newState = ClusterState.PASSIVE;

        final Future future = spawn(new Runnable() {
            public void run() {
                try {
                    changeClusterState(hz2, newState, initialPartitionStateVersion);
                } catch (Exception ignored) {
                }
            }
        });

        partitionService.firstArrangement();

        future.get(2, TimeUnit.MINUTES);

        final ClusterState currentState = hz2.getCluster().getClusterState();
        if (currentState == newState) {
            // if cluster state changed then partition state version should be equal to initial version
            assertEquals(initialPartitionStateVersion, partitionService.getPartitionStateVersion());
        } else {
            assertEquals(ClusterState.ACTIVE, currentState);

            final InternalPartition partition = partitionService.getPartition(0, false);
            if (partition.getOwnerOrNull() == null) {
                // if partition assignment failed then partition state version should be equal to initial version
                assertEquals(initialPartitionStateVersion, partitionService.getPartitionStateVersion());
            } else {
                // if cluster state change failed and partition assignment is done
                // then partition state version should be some positive number
                final int partitionStateVersion = partitionService.getPartitionStateVersion();
                assertTrue("Version should be positive: " + partitionService, partitionStateVersion > 0);
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

        final Address owner = getNode(hz1).getThisAddress();
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getMap(randomMapName()).put(1, 1); // for updating partition version

        changeClusterStateEventually(hz, state);

        List<HazelcastInstance> instanceList = new ArrayList<HazelcastInstance>(Arrays.asList(instances));
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

        InternalOperationService operationService = getNode(hz3).getNodeEngine().getOperationService();
        Operation op = new AddAndGetOperation(key, 1);
        final Future<Long> future = operationService
                .invokeOnPartition(AtomicLongService.SERVICE_NAME, op, partitionId);

        assertFalse(future.isDone());

        factory.newHazelcastInstance(owner);
        assertClusterSizeEventually(3, hz2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(future.isDone());
            }
        });

        // should not fail
        future.get();
    }

    private void changeClusterState(HazelcastInstance instance, ClusterState newState, int partitionStateVersion) {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(instance);
        MemberMap members = clusterService.getMembershipManager().getMemberMap();
        clusterService.getClusterStateManager().changeClusterState(ClusterStateChange.from(newState), members,
                partitionStateVersion, false);
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

    private static abstract class TransactionAnswer implements Answer<Transaction> {
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
        public String getTxnId() {
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
        public String getOwnerUuid() {
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
