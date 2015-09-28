/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomiclong.operations.AddAndGetOperation;
import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionLogRecord;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.EmptyStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AdvancedClusterStateTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void changeClusterState_shouldFail_whenMemberAdded_duringTx() throws Exception {
        changeClusterState_shouldFail_whenMemberList_changes(new AddingMemberListAnswer());
    }

    @Test(expected = IllegalStateException.class)
    public void changeClusterState_shouldFail_whenMemberRemoved_duringTx() throws Exception {
        changeClusterState_shouldFail_whenMemberList_changes(new RemovingMemberListAnswer());
    }

    private void changeClusterState_shouldFail_whenMemberList_changes(MemberListAnswer memberListAnswer) throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz = instances[instances.length - 1];
        ClusterServiceImpl clusterService = spyClusterService(hz);
        memberListAnswer.cluster = clusterService;

        when(clusterService.getMemberImpls()).thenAnswer(memberListAnswer);

        hz.getCluster().changeClusterState(ClusterState.SHUTTING_DOWN);
    }

    @Test(expected = TransactionException.class)
    public void changeClusterState_shouldFail_whenStateIsAlreadyLocked() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        final HazelcastInstance hz = instances[instances.length - 1];
        final ClusterServiceImpl clusterService = spyClusterService(hz);

        final CountDownLatch stateLockedLatch = new CountDownLatch(1);
        final CountDownLatch transactionPauseLatch = new CountDownLatch(1);
        MemberListAnswer memberListAnswer = new MemberListAnswer() {
            @Override
            Collection<MemberImpl> onAnswer(Collection<MemberImpl> members) throws Exception {
                if (clusterService.getClusterState() == ClusterState.IN_TRANSITION) {
                    stateLockedLatch.countDown();
                    transactionPauseLatch.await(1, TimeUnit.MINUTES);
                }
                return members;
            }
        };
        memberListAnswer.cluster = clusterService;
        when(clusterService.getMemberImpls()).thenAnswer(memberListAnswer);

        Thread stateThread = new Thread() {
            public void run() {
                hz.getCluster().changeClusterState(ClusterState.SHUTTING_DOWN);
            }
        };
        stateThread.start();

        // first node is locked the state
        assertOpenEventually(stateLockedLatch);

        final HazelcastInstance hz2 = instances[instances.length - 2];
        try {
            hz2.getCluster().changeClusterState(ClusterState.SHUTTING_DOWN);
            fail("Cluster state change should fail, because state is already locked!");
        } finally {
            transactionPauseLatch.countDown();
            stateThread.join();
        }
    }

    @Test
    public void changeClusterState_shouldFail_whenInitiatorDies_beforePrepare() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        final HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = TransactionOptions.getDefault().setTimeout(5, TimeUnit.SECONDS);
        when(transactionManagerService.newTransaction(options)).thenAnswer(new TransactionAnswer() {
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

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertClusterState(ClusterState.ACTIVE, instances[0], instances[1]);
            }
        });
    }

    @Test
    public void changeClusterState_shouldNotFail_whenInitiatorDies_afterPrepare() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        final HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = TransactionOptions.getDefault().setDurability(1);
        when(transactionManagerService.newTransaction(options)).thenAnswer(new TransactionAnswer() {
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

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertClusterState(ClusterState.FROZEN, instances[0], instances[1]);
            }
        });
    }

    @Test
    public void changeClusterState_shouldFail_withoutBackup_whenInitiatorDies_beforePrepare() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        final HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = new TransactionOptions().setDurability(0).setTimeout(5, TimeUnit.SECONDS);
        when(transactionManagerService.newTransaction(options)).thenAnswer(new TransactionAnswer() {
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

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertClusterState(ClusterState.ACTIVE, instances[0], instances[1]);
            }
        });
    }

    @Test
    public void changeClusterState_shouldFail_withoutBackup_whenInitiatorDies_afterPrepare() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance[] instances = factory.newInstances();

        final HazelcastInstance hz = instances[instances.length - 1];
        TransactionManagerServiceImpl transactionManagerService = spyTransactionManagerService(hz);

        TransactionOptions options = new TransactionOptions().setDurability(0).setTimeout(5, TimeUnit.SECONDS);
        when(transactionManagerService.newTransaction(options)).thenAnswer(new TransactionAnswer() {
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

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertClusterState(ClusterState.ACTIVE, instances[0], instances[1]);
            }
        });
    }

    @Test
    public void clusterState_shouldBeTheSame_finally_onAllNodes() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        CountDownLatch latch = new CountDownLatch(instances.length);
        int iteration = 10;
        for (HazelcastInstance instance : instances) {
            new IterativeStateChangeThread(instance, iteration, latch).start();
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

        hz2.getCluster().changeClusterState(ClusterState.FROZEN);
        terminateInstance(hz1);

        final InternalPartition partition = getNode(hz2).getPartitionService().getPartition(partitionId);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(owner, partition.getOwnerOrNull());
            }
        }, 3);
    }

    @Test
    public void clusterState_shouldBeFrozen_whenMemberReJoins_inFrozenState() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];

        hz2.getCluster().changeClusterState(ClusterState.FROZEN);

        final Address owner = getNode(hz1).getThisAddress();
        terminateInstance(hz1);

        hz1 = factory.newHazelcastInstance(owner);
        assertClusterSizeEventually(3, hz2);

        assertClusterState(ClusterState.FROZEN, hz1, hz2, hz3);
    }

    @Test
    public void partitionTable_shouldBeSame_whenMemberReJoins_inFrozenState() {
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

        hz2.getCluster().changeClusterState(ClusterState.FROZEN);
        terminateInstance(hz1);

        factory.newHazelcastInstance(owner);
        assertClusterSizeEventually(3, hz2);

        final InternalPartition partition = getNode(hz2).getPartitionService().getPartition(partitionId);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(owner, partition.getOwnerOrNull());
            }
        }, 3);
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberLeaves_inFrozenState_thenStateChangesToActive() {
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

        hz2.getCluster().changeClusterState(ClusterState.FROZEN);
        terminateInstance(hz1);

        assertClusterSizeEventually(2, hz2);

        hz2.getCluster().changeClusterState(ClusterState.ACTIVE);

        final InternalPartition partition = getNode(hz2).getPartitionService().getPartition(partitionId);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(partition.getOwnerOrNull());
                assertNotEquals(owner, partition.getOwnerOrNull());
            }
        });
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

        Address owner = getNode(hz1).getThisAddress();
        String key = generateKeyOwnedBy(hz1);
        int partitionId = hz1.getPartitionService().getPartition(key).getPartitionId();

        hz2.getCluster().changeClusterState(ClusterState.FROZEN);
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

    private static ClusterServiceImpl spyClusterService(HazelcastInstance hz) throws Exception {
        Node node = getNode(hz);
        ClusterServiceImpl originalClusterService = node.clusterService;
        ClusterServiceImpl spiedClusterService = spy(originalClusterService);

        Field clusterServiceField = Node.class.getField("clusterService");
        clusterServiceField.setAccessible(true);
        clusterServiceField.set(node, spiedClusterService);
        return spiedClusterService;
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

    private static void assertClusterState(ClusterState expectedState, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals("Instance " + instance.getCluster().getLocalMember(),
                    expectedState, instance.getCluster().getClusterState());
        }
    }

    private static class AddingMemberListAnswer extends MemberListAnswer {

        Collection<MemberImpl> onAnswer(Collection<MemberImpl> members) throws Exception {
            if (addFakeMember()) {
                members = new ArrayList<MemberImpl>(members);
                members.add(new MemberImpl(new Address("127.0.0.1", 6000), false));
            }
            return members;
        }

        private boolean addFakeMember() {
            return cluster.getClusterState() == ClusterState.IN_TRANSITION;
        }
    }

    private static class RemovingMemberListAnswer extends MemberListAnswer {

        Collection<MemberImpl> onAnswer(Collection<MemberImpl> members) throws Exception {
            if (removeMember()) {
                LinkedList<MemberImpl> list = new LinkedList<MemberImpl>(members);
                list.removeFirst();
                members = list;
            }
            return members;
        }

        private boolean removeMember() {
            return cluster.getClusterState() == ClusterState.IN_TRANSITION;
        }
    }

    private static abstract class MemberListAnswer implements Answer<Collection<MemberImpl>> {
        Cluster cluster;

        @SuppressWarnings("unchecked")
        @Override
        public Collection<MemberImpl> answer(InvocationOnMock invocation) throws Throwable {
            Collection<MemberImpl> members = (Collection<MemberImpl>) invocation.callRealMethod();
            members = onAnswer(members);
            return members;
        }

        abstract Collection<MemberImpl> onAnswer(Collection<MemberImpl> members) throws Exception;
    }

    private static abstract class TransactionAnswer implements Answer<Transaction> {
        @Override
        public Transaction answer(InvocationOnMock invocation) throws Throwable {
            Transaction tx = (Transaction) invocation.callRealMethod();
            Transaction delegatingTransaction = new DelegatingTransaction(tx) {
                @Override
                public void prepare() throws TransactionException {
                    beforePrepare();
                    super.prepare();
                    afterPrepare();
                }
            };
            return delegatingTransaction;
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

        public IterativeStateChangeThread(HazelcastInstance instance, int iteration, CountDownLatch latch) {
            this.instance = instance;
            this.iteration = iteration;
            this.latch = latch;
        }

        public void run() {
            if (!initialize()) {
                return;
            }

            Cluster cluster = instance.getCluster();
            ClusterState newState = flipState(cluster.getClusterState());
            for (int i = 0; i < iteration; i++) {
                try {
                    cluster.changeClusterState(newState);
                } catch (TransactionException e) {
                    EmptyStatement.ignore(e);
                }
                newState = flipState(newState);
            }
            latch.countDown();
        }

        private static ClusterState flipState(ClusterState state) {
            state = state == ClusterState.ACTIVE ? ClusterState.FROZEN : ClusterState.ACTIVE;
            return state;
        }

        private boolean initialize() {
            try {
                ClusterServiceImpl clusterService = spyClusterService(instance);
                MemberListAnswer memberListAnswer = new MemberListAnswer() {
                    Random random = new Random();
                    @Override
                    Collection<MemberImpl> onAnswer(Collection<MemberImpl> members) throws Exception {
                        sleepMillis(random.nextInt(10) + 1);
                        return members;
                    }
                };
                when(clusterService.getMemberImpls()).thenAnswer(memberListAnswer);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }

    private static class DelegatingTransaction implements Transaction {

        final Transaction tx;

        DelegatingTransaction(Transaction tx) {
            this.tx = tx;
        }

        @Override
        public void begin() throws IllegalStateException {tx.begin();}

        @Override
        public void prepare() throws TransactionException {tx.prepare();}

        @Override
        public void commit() throws TransactionException, IllegalStateException {tx.commit();}

        @Override
        public void rollback() throws IllegalStateException {tx.rollback();}

        @Override
        public String getTxnId() {return tx.getTxnId();}

        @Override
        public State getState() {return tx.getState();}

        @Override
        public long getTimeoutMillis() {return tx.getTimeoutMillis();}

        @Override
        public void add(TransactionLogRecord record) {tx.add(record);}

        @Override
        public void remove(Object key) {tx.remove(key);}

        @Override
        public TransactionLogRecord get(Object key) {return tx.get(key);}

        @Override
        public String getOwnerUuid() {return tx.getOwnerUuid();}
    }
}
