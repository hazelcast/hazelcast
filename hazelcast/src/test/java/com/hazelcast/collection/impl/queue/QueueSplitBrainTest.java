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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Queue;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupQueue;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link IQueue}.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 * <p>
 * The number and content of backup items are tested for all merge policies.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueSplitBrainTest extends SplitBrainTestSupport {

    private static final int INITIAL_COUNT = 10;
    private static final int NEW_ITEMS = 15;

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                PassThroughMergePolicy.class,
                PutIfAbsentMergePolicy.class,
                MergeIntegerValuesMergePolicy.class,
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private String queueNameA = randomMapName("QueueA-");
    private String queueNameB = randomMapName("QueueB-");
    private IQueue<Object> queueA1;
    private IQueue<Object> queueA2;
    private IQueue<Object> queueB1;
    private IQueue<Object> queueB2;
    private Queue<Object> backupQueue;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getQueueConfig(queueNameA)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        config.getQueueConfig(queueNameB)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        IQueue<Object> queue = instances[0].getQueue(queueNameA);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            queue.add("item" + i);
        }

        waitAllForSafeState(instances);

        int partitionId = ((QueueProxySupport) queue).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        Queue<Object> backupQueue = getBackupQueue(backupInstance, queueNameA);

        assertEquals("backupQueue should contain " + INITIAL_COUNT + " items", INITIAL_COUNT, backupQueue.size());
        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("backupQueue should contain item" + i + " " + toString(backupQueue), backupQueue.contains("item" + i));
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        queueA1 = firstBrain[0].getQueue(queueNameA);
        queueA2 = secondBrain[0].getQueue(queueNameA);

        queueB2 = secondBrain[0].getQueue(queueNameB);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            queueB2.add("item" + i);
        }

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterSplitPutIfAbsentMergePolicy();
        } else if (mergePolicyClass == MergeIntegerValuesMergePolicy.class) {
            afterSplitCustomMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        int partitionId = ((QueueProxySupport) queueA1).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        backupQueue = getBackupQueue(backupInstance, queueNameA);

        queueB1 = instances[0].getQueue(queueNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterMergePutIfAbsentMergePolicy();
        } else if (mergePolicyClass == MergeIntegerValuesMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        // we should have these items in the merged queueA, since they are added in both clusters
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            queueA1.add("item" + i);
            queueA2.add("item" + i);
        }

        // we should not have these items in the merged queues, since they are in the smaller cluster only
        for (int i = 0; i < NEW_ITEMS; i++) {
            queueA2.add("discardedItem" + i);
            queueB2.add("discardedItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertQueueContent(queueA1, false);
        assertQueueContent(queueA2, false);
        assertQueueContent(backupQueue, false);

        assertTrue(queueB1.isEmpty());
        assertTrue(queueB2.isEmpty());
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            queueA1.add("item" + i);
            queueB2.add("item" + i);
        }

        // we should not lose the additional items from queueA2 or the new queueB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            queueA2.add("item" + i);
            queueB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertQueueContent(queueA1, true);
        assertQueueContent(queueA2, true);
        assertQueueContent(backupQueue, true);

        assertQueueContent(queueB1, true);
        assertQueueContent(queueB2, true);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            queueA1.add("item" + i);
            queueB2.add("item" + i);
        }

        // we should not lose the additional items from queueA2 or the new queueB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            queueA2.add("item" + i);
            queueB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertQueueContent(queueA1, true);
        assertQueueContent(queueA2, true);
        assertQueueContent(backupQueue, true);

        assertQueueContent(queueB1, true);
        assertQueueContent(queueB2, true);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < NEW_ITEMS; i++) {
            queueA2.add(i);
            queueA2.add("discardedItem" + i);
        }
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(INITIAL_COUNT + NEW_ITEMS, queueA1.size());
        assertEquals(INITIAL_COUNT + NEW_ITEMS, queueA2.size());
        assertEquals(INITIAL_COUNT + NEW_ITEMS, backupQueue.size());

        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("queueA1 should contain 'item" + i + "' " + toString(queueA1), queueA1.contains("item" + i));
            assertTrue("queueA2 should contain 'item" + i + "' " + toString(queueA2), queueA2.contains("item" + i));
            assertTrue("backupQueue should contain 'item" + i + "' " + toString(backupQueue), backupQueue.contains("item" + i));
        }
        for (int i = 0; i < NEW_ITEMS; i++) {
            assertTrue("queueA1 should contain '" + i + "' " + toString(queueA1), queueA1.contains(i));
            assertTrue("queueA2 should contain '" + i + "' " + toString(queueA2), queueA2.contains(i));
            assertTrue("backupQueue should contain '" + i + "' " + backupQueue, backupQueue.contains(i));

            assertFalse("queueA1 should not contain 'discardedItem" + i + "'", queueA1.contains("discardedItem" + i));
            assertFalse("queueA2 should not contain 'discardedItem" + i + "'", queueA2.contains("discardedItem" + i));
            assertFalse("backupQueue should not contain 'discardedItem" + i + "'", backupQueue.contains("discardedItem" + i));
        }
    }

    private static void assertQueueContent(final Queue<Object> queue, boolean hasMergedItems) {
        final int expectedCount = INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("queue should contain " + expectedCount + " items", expectedCount, queue.size());
            }
        });

        for (int i = 0; i < INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1); i++) {
            assertTrue("queue should contain item" + i + " " + toString(queue), queue.contains("item" + i));
        }
    }
}
