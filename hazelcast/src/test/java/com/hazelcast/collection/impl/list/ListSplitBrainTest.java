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

package com.hazelcast.collection.impl.list;

import com.hazelcast.collection.impl.collection.AbstractCollectionProxyImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
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
import java.util.List;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupList;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link IList}.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 * <p>
 * The number and content of backup items are tested for all merge policies.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListSplitBrainTest extends SplitBrainTestSupport {

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

    private String listNameA = randomMapName("listA-");
    private String listNameB = randomMapName("listB-");
    private IList<Object> listA1;
    private IList<Object> listA2;
    private IList<Object> listB1;
    private IList<Object> listB2;
    private List<Object> backupList;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getListConfig(listNameA)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getListConfig(listNameB)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        IList<Object> list = instances[0].getList(listNameA);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            list.add("item" + i);
        }

        waitAllForSafeState(instances);

        int partitionId = ((AbstractCollectionProxyImpl) list).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        List<Object> backupList = getBackupList(backupInstance, listNameA);

        assertEquals("backupList should contain " + INITIAL_COUNT + " items", INITIAL_COUNT, backupList.size());
        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("backupList should contain item" + i + " " + toString(backupList), backupList.contains("item" + i));
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        listA1 = firstBrain[0].getList(listNameA);
        listA2 = secondBrain[0].getList(listNameA);

        listB2 = secondBrain[0].getList(listNameB);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            listB2.add("item" + i);
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

        int partitionId = ((AbstractCollectionProxyImpl) listA1).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        backupList = getBackupList(backupInstance, listNameA);

        listB1 = instances[0].getList(listNameB);

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
        // we should have these items in the merged listA, since they are added in both clusters
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            listA1.add("item" + i);
            listA2.add("item" + i);
        }

        // we should not have these items in the merged lists, since they are in the smaller cluster only
        for (int i = 0; i < NEW_ITEMS; i++) {
            listA2.add("lostItem" + i);
            listB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertListContent(listA1, false);
        assertListContent(listA2, false);
        assertListContent(backupList, false);

        assertTrue(listB1.isEmpty());
        assertTrue(listB2.isEmpty());
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            listA1.add("item" + i);
        }

        // we should not lose the additional items from listA2 or the new listB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            listA2.add("item" + i);
            listB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertListContent(listA1, true);
        assertListContent(listA2, true);
        assertListContent(backupList, true);

        assertListContent(listB1, true);
        assertListContent(listB2, true);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            listA1.add("item" + i);
        }

        // we should not lose the additional items from listA2 or the new listB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            listA2.add("item" + i);
            listB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertListContent(listA1, true);
        assertListContent(listA2, true);
        assertListContent(backupList, true);

        assertListContent(listB1, true);
        assertListContent(listB2, true);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < NEW_ITEMS; i++) {
            listA2.add(i);
            listA2.add("lostItem" + i);
        }
    }

    private void afterMergeCustomMergePolicy() {
        int expectedSize = INITIAL_COUNT + NEW_ITEMS;
        assertEquals("listA1 should contain " + expectedSize + " items", expectedSize, listA1.size());
        assertEquals("listA2 should contain " + expectedSize + " items", expectedSize, listA2.size());
        assertEquals("backupList should contain " + expectedSize + " items " + backupList, expectedSize, backupList.size());

        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("listA1 should contain 'item" + i + "' " + toString(listA1), listA1.contains("item" + i));
            assertTrue("listA2 should contain 'item" + i + "' " + toString(listA2), listA2.contains("item" + i));
            assertTrue("backupList should contain 'item" + i + "' " + backupList, backupList.contains("item" + i));
        }
        for (int i = 0; i < NEW_ITEMS; i++) {
            assertTrue("listA1 should contain '" + i + "'", listA1.contains(i));
            assertTrue("listA2 should contain '" + i + "'", listA2.contains(i));
            assertTrue("backupList should contain '" + i + "'", backupList.contains(i));

            assertFalse("listA1 should not contain 'lostItem" + i + "'", listA1.contains("lostItem" + i));
            assertFalse("list2A should not contain 'lostItem" + i + "'", listA2.contains("lostItem" + i));
            assertFalse("backupList should not contain 'lostItem" + i + "'", backupList.contains("lostItem" + i));
        }
    }

    private static void assertListContent(List<Object> list, boolean hasMergedItems) {
        int expectedSize = INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1);
        assertEquals("list should contain " + expectedSize + " items " + toString(list), expectedSize, list.size());

        for (int i = 0; i < INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1); i++) {
            assertTrue("list should contain item" + i + " " + toString(list), list.contains("item" + i));
        }
    }
}
