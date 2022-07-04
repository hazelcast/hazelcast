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

package com.hazelcast.collection.impl.list;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupList;
import static java.util.Arrays.asList;
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
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListSplitBrainTest extends SplitBrainTestSupport {

    private static final int ITEM_COUNT = 25;

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                PassThroughMergePolicy.class,
                PutIfAbsentMergePolicy.class,
                RemoveValuesMergePolicy.class,
                ReturnPiCollectionMergePolicy.class,
                MergeCollectionOfIntegerValuesMergePolicy.class,
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

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        listA1 = firstBrain[0].getList(listNameA);
        listA2 = secondBrain[0].getList(listNameA);

        listB2 = secondBrain[0].getList(listNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterSplitPutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RemoveValuesMergePolicy.class) {
            afterSplitRemoveValuesMergePolicy();
        } else if (mergePolicyClass == ReturnPiCollectionMergePolicy.class) {
            afterSplitReturnPiCollectionMergePolicy();
        } else if (mergePolicyClass == MergeCollectionOfIntegerValuesMergePolicy.class) {
            afterSplitCustomMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        backupList = getBackupList(instances, listA1);

        listB1 = instances[0].getList(listNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterMergePutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RemoveValuesMergePolicy.class) {
            afterMergeRemoveValuesMergePolicy();
        } else if (mergePolicyClass == ReturnPiCollectionMergePolicy.class) {
            afterMergeReturnPiCollectionMergePolicy();
        } else if (mergePolicyClass == MergeCollectionOfIntegerValuesMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            listA1.add("item" + i);
            listA2.add("lostItem" + i);

            listB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertListContent(listA1);
        assertListContent(listA2);
        assertListContent(backupList);

        assertListContent(listB1, 0);
        assertListContent(listB2, 0);
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            listA1.add("lostItem" + i);
            listA2.add("item" + i);

            listB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertListContent(listA1);
        assertListContent(listA2);
        assertListContent(backupList);

        assertListContent(listB1);
        assertListContent(listB2);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            listA1.add("item" + i);
            listA2.add("lostItem" + i);

            listB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertListContent(listA1);
        assertListContent(listA2);
        assertListContent(backupList);

        assertListContent(listB1);
        assertListContent(listB2);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            listA1.add("lostItem" + i);
            listA2.add("lostItem" + i);

            listB2.add("lostItem" + i);
        }
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertListContent(listA1, 0);
        assertListContent(listA2, 0);
        assertListContent(backupList, 0);

        assertListContent(listB1, 0);
        assertListContent(listB2, 0);
    }

    private void afterSplitReturnPiCollectionMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            listA1.add("lostItem" + i);
            listA2.add("lostItem" + i);

            listB2.add("lostItem" + i);
        }
    }

    private void afterMergeReturnPiCollectionMergePolicy() {
        assertPiCollection(listA1);
        assertPiCollection(listA2);
        assertPiCollection(backupList);

        assertPiCollection(listB1);
        assertPiCollection(listB2);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            listA2.add(i);
            listA2.add("lostItem" + i);
        }
    }

    private void afterMergeCustomMergePolicy() {
        assertListContent(listA1, ITEM_COUNT);
        assertListContent(listA2, ITEM_COUNT);
        assertListContent(backupList, ITEM_COUNT);
    }

    private static void assertListContent(List<Object> list) {
        assertListContent(list, ITEM_COUNT, "item");
    }

    private static void assertListContent(List<Object> list, int expectedSize) {
        assertListContent(list, expectedSize, null);
    }

    private static void assertListContent(List<Object> list, int expectedSize, String prefix) {
        assertEqualsStringFormat("list " + toString(list) + " should contain %d items, but was %d ", expectedSize, list.size());

        for (int i = 0; i < expectedSize; i++) {
            Object expectedValue = prefix == null ? i : prefix + i;
            assertTrue("list " + toString(list) + " should contain " + expectedValue, list.contains(expectedValue));
        }
    }
}
