package com.hazelcast.spi.impl.waitnotifyservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Member;
import com.hazelcast.spi.impl.SplitBrainTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WaitNotifySplitBrainTest extends SplitBrainTestSupport {

    private static final int POLLERS_COUNT = 1000;
    private String queueName;

    @Override
    protected void onBeforeSplitBrainCreated() {
        HazelcastInstance hz1 = getAllInstances()[0];
        queueName = generateKeyOwnedBy(hz1);
        final IQueue<Object> queue = hz1.getQueue(queueName);

        startTakingFromQueue(queue);
        assertTakeOperationsAreWaitingEventually(hz1);
    }

    @Override
    protected void onAfterSplitBrainHealed() {
        HazelcastInstance[] instances = getAllInstances();
        assertOnlyOwnerHasWaitingOperationsEventually(queueName, instances);

        final IQueue<Object> queue = instances[0].getQueue(queueName);
        for (int i = 0; i < POLLERS_COUNT; i++) {
            queue.offer(i);
        }
        assertWaitingOperationCountEventually(0, instances);
    }

    private void assertOnlyOwnerHasWaitingOperationsEventually(String name, HazelcastInstance...instances) {
        for (HazelcastInstance hz : instances) {
            Member owner = hz.getPartitionService().getPartition(name).getOwner();
            int expectedWaitingOps = owner.equals(hz.getCluster().getLocalMember()) ? POLLERS_COUNT : 0;
            assertWaitingOperationCountEventually(expectedWaitingOps, hz);
        }
    }

    private void startTakingFromQueue(final IQueue<Object> queue) {
        for (int i = 0; i < POLLERS_COUNT; i++) {
            new Thread() {
                public void run() {
                    try {
                        queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
    }

    private void assertTakeOperationsAreWaitingEventually(HazelcastInstance instance) {
        assertWaitingOperationCountEventually(POLLERS_COUNT, instance);
    }


}
