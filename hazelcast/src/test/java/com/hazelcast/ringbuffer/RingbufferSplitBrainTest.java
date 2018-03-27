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

package com.hazelcast.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
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
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.ringbuffer.RingbufferTestUtil.getBackupRingbuffer;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link com.hazelcast.ringbuffer.Ringbuffer}.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 * <p>
 * The number and content of backup items are tested for all merge policies.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class RingbufferSplitBrainTest extends SplitBrainTestSupport {

    private static final int INITIAL_COUNT = 10;
    private static final int NEW_ITEMS = 15;

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, DiscardMergePolicy.class},
                {BINARY, PassThroughMergePolicy.class},
                {BINARY, PutIfAbsentMergePolicy.class},

                {BINARY, MergeIntegerValuesMergePolicy.class},
                {OBJECT, MergeIntegerValuesMergePolicy.class},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private String ringbufferNameA = randomMapName("ringbufferA-");
    private String ringbufferNameB = randomMapName("ringbufferB-");
    private Ringbuffer<Object> ringbufferA1;
    private Ringbuffer<Object> ringbufferA2;
    private Ringbuffer<Object> ringbufferB1;
    private Ringbuffer<Object> ringbufferB2;
    private Collection<Object> backupRingbuffer;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getRingbufferConfig(ringbufferNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0);
        config.getRingbufferConfig(ringbufferNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        Ringbuffer<Object> ringbuffer = instances[0].getRingbuffer(ringbufferNameA);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            ringbuffer.add("item" + i);
        }

        waitAllForSafeState(instances);

        int partitionId = ((RingbufferProxy) ringbuffer).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        Collection<Object> backupRingbuffer = getBackupRingbuffer(backupInstance, partitionId, ringbufferNameA);

        assertEquals("backupRingbuffer should contain " + INITIAL_COUNT + " items", INITIAL_COUNT, backupRingbuffer.size());
        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("backupRingbuffer should contain item" + i + " " + backupRingbuffer,
                    backupRingbuffer.contains("item" + i));
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        ringbufferA1 = firstBrain[0].getRingbuffer(ringbufferNameA);
        ringbufferA2 = secondBrain[0].getRingbuffer(ringbufferNameA);

        ringbufferB2 = secondBrain[0].getRingbuffer(ringbufferNameB);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            ringbufferB2.add("item" + i);
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

        backupRingbuffer = getBackupRingbuffer(instances, ringbufferA1);

        ringbufferB1 = instances[0].getRingbuffer(ringbufferNameB);

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
        // we should have these items in the merged ringbufferA, since they are added in both clusters
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            ringbufferA1.add("item" + i);
            ringbufferA2.add("item" + i);
        }

        // we should not have these items in the merged ringbuffers, since they are in the smaller cluster only
        for (int i = 0; i < NEW_ITEMS; i++) {
            ringbufferA2.add("lostItem" + i);
            ringbufferB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertRingbufferContent(ringbufferA1, false);
        assertRingbufferContent(ringbufferA2, false);
        assertRingbufferContent(backupRingbuffer, false);

        assertEquals(0, ringbufferB1.size());
        assertEquals(0, ringbufferB2.size());
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            ringbufferA1.add("item" + i);
        }

        // we should not lose the additional items from ringbufferA2 or the new ringbufferB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            ringbufferA2.add("item" + i);
            ringbufferB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertRingbufferContent(ringbufferA1, true);
        assertRingbufferContent(ringbufferA2, true);
        assertRingbufferContent(backupRingbuffer, true);

        assertRingbufferContent(ringbufferB1, true);
        assertRingbufferContent(ringbufferB2, true);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            ringbufferA1.add("item" + i);
        }

        // we should not lose the additional items from ringbufferA2 or the new ringbufferB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            ringbufferA2.add("item" + i);
            ringbufferB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertRingbufferContent(ringbufferA1, true);
        assertRingbufferContent(ringbufferA2, true);
        assertRingbufferContent(backupRingbuffer, true);

        assertRingbufferContent(ringbufferB1, true);
        assertRingbufferContent(ringbufferB2, true);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < NEW_ITEMS; i++) {
            ringbufferA2.add(i);
            ringbufferA2.add("lostItem" + i);
        }
    }

    private void afterMergeCustomMergePolicy() {
        Collection<Object> ringbufferContent1 = getRingbufferContent(ringbufferA1);
        Collection<Object> ringbufferContent2 = getRingbufferContent(ringbufferA2);

        int expected = INITIAL_COUNT + NEW_ITEMS;
        assertEquals("ringbufferA1 should contain " + expected + " items " + ringbufferContent1, expected, ringbufferA1.size());
        assertEquals("ringbufferA2 should contain " + expected + " items " + ringbufferContent2, expected, ringbufferA2.size());
        assertEquals("backupRingbuffer should contain " + expected + " items " + backupRingbuffer,
                expected, backupRingbuffer.size());

        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("ringbufferA1 should contain 'item" + i + "' " + ringbufferContent1,
                    ringbufferContent1.contains("item" + i));
            assertTrue("ringbufferA2 should contain 'item" + i + "' " + ringbufferContent2,
                    ringbufferContent2.contains("item" + i));
            assertTrue("backupRingbuffer should contain 'item" + i + "' " + backupRingbuffer,
                    backupRingbuffer.contains("item" + i));
        }
        for (int i = 0; i < NEW_ITEMS; i++) {
            assertTrue("ringbufferA1 should contain '" + i + "' " + ringbufferContent1, ringbufferContent1.contains(i));
            assertTrue("ringbufferA2 should contain '" + i + "' " + ringbufferContent2, ringbufferContent2.contains(i));
            assertTrue("backupRingbuffer should contain '" + i + "' " + backupRingbuffer, backupRingbuffer.contains(i));

            assertFalse("ringbufferA1 should not contain 'lostItem" + i + "'", ringbufferContent1.contains("lostItem" + i));
            assertFalse("ringbufferA2 should not contain 'lostItem" + i + "'", ringbufferContent2.contains("lostItem" + i));
            assertFalse("backupRingbuffer should not contain 'lostItem" + i + "'", backupRingbuffer.contains("lostItem" + i));
        }
    }

    private static void assertRingbufferContent(Ringbuffer<Object> ringbuffer, boolean hasMergedItems) {
        assertRingbufferContent(getRingbufferContent(ringbuffer), hasMergedItems);
    }

    private static void assertRingbufferContent(Collection<Object> ringbuffer, boolean hasMergedItems) {
        int expected = INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1);
        assertEquals("ringbuffer should contain " + expected + " items " + ringbuffer, expected, ringbuffer.size());

        for (int i = 0; i < INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1); i++) {
            assertTrue("ringbuffer should contain item" + i + " " + ringbuffer, ringbuffer.contains("item" + i));
        }
    }

    private static Collection<Object> getRingbufferContent(Ringbuffer<Object> ringbuffer) {
        List<Object> list = new LinkedList<Object>();
        try {
            for (long sequence = ringbuffer.headSequence(); sequence <= ringbuffer.tailSequence(); sequence++) {
                list.add(ringbuffer.readOne(sequence));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return list;
    }
}
