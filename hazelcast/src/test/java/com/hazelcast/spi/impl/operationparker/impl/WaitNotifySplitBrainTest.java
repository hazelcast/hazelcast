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

package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WaitNotifySplitBrainTest extends SplitBrainTestSupport {

    private static final int POLL_COUNT = 1000;

    private String queueName;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        HazelcastInstance hz1 = instances[0];
        queueName = generateKeyOwnedBy(hz1);
        final IQueue<Object> queue = hz1.getQueue(queueName);

        startTakingFromQueue(queue);
        assertTakeOperationsAreWaitingEventually(hz1);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        assertOnlyOwnerHasWaitingOperationsEventually(queueName, instances);

        final IQueue<Object> queue = instances[0].getQueue(queueName);
        for (int i = 0; i < POLL_COUNT; i++) {
            queue.offer(i);
        }
        assertWaitingOperationCountEventually(0, instances);
    }

    private void assertOnlyOwnerHasWaitingOperationsEventually(String name, HazelcastInstance... instances) {
        for (HazelcastInstance hz : instances) {
            Member owner = hz.getPartitionService().getPartition(name).getOwner();
            int expectedWaitingOps = owner.equals(hz.getCluster().getLocalMember()) ? POLL_COUNT : 0;
            assertWaitingOperationCountEventually(expectedWaitingOps, hz);
        }
    }

    private void startTakingFromQueue(final IQueue<Object> queue) {
        for (int i = 0; i < POLL_COUNT; i++) {
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
        assertWaitingOperationCountEventually(POLL_COUNT, instance);
    }
}
