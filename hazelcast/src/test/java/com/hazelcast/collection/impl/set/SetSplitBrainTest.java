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

package com.hazelcast.collection.impl.set;

import com.hazelcast.collection.ISet;
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
import java.util.Set;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link ISet}.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 * <p>
 * The number and content of backup items are tested for all merge policies.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SetSplitBrainTest extends SplitBrainTestSupport {

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

    private String setNameA = randomMapName("setA-");
    private String setNameB = randomMapName("setB-");
    private ISet<Object> setA1;
    private ISet<Object> setA2;
    private ISet<Object> setB1;
    private ISet<Object> setB2;
    private Set<Object> backupSet;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getSetConfig(setNameA)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getSetConfig(setNameB)
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

        setA1 = firstBrain[0].getSet(setNameA);
        setA2 = secondBrain[0].getSet(setNameA);

        setB2 = secondBrain[0].getSet(setNameB);

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

        backupSet = getBackupSet(instances, setA1);

        setB1 = instances[0].getSet(setNameB);

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
            setA1.add("item" + i);
            setA2.add("lostItem" + i);

            setB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertSetContent(setA1);
        assertSetContent(setA2);
        assertSetContent(backupSet);

        assertSetContent(setB1, 0);
        assertSetContent(setB2, 0);
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            setA1.add("lostItem" + i);
            setA2.add("item" + i);

            setB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertSetContent(setA1);
        assertSetContent(setA2);
        assertSetContent(backupSet);

        assertSetContent(setB1);
        assertSetContent(setB2);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            setA1.add("item" + i);
            setA2.add("lostItem" + i);

            setB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertSetContent(setA1);
        assertSetContent(setA2);
        assertSetContent(backupSet);

        assertSetContent(setB1);
        assertSetContent(setB2);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            setA1.add("lostItem" + i);
            setA2.add("lostItem" + i);

            setB2.add("lostItem" + i);
        }
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertSetContent(setA1, 0);
        assertSetContent(setA2, 0);
        assertSetContent(backupSet, 0);

        assertSetContent(setB1, 0);
        assertSetContent(setB2, 0);
    }

    private void afterSplitReturnPiCollectionMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            setA1.add("lostItem" + i);
            setA2.add("lostItem" + i);

            setB2.add("lostItem" + i);
        }
    }

    private void afterMergeReturnPiCollectionMergePolicy() {
        assertPiSet(setA1);
        assertPiSet(setA2);
        assertPiSet(backupSet);

        assertPiSet(setB1);
        assertPiSet(setB2);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            setA2.add(i);
            setA2.add("lostItem" + i);
        }
    }

    private void afterMergeCustomMergePolicy() {
        assertSetContent(setA1, ITEM_COUNT);
        assertSetContent(setA2, ITEM_COUNT);
        assertSetContent(backupSet, ITEM_COUNT);
    }

    private static void assertSetContent(Set<Object> set) {
        assertSetContent(set, ITEM_COUNT, "item");
    }

    private static void assertSetContent(Set<Object> set, int expectedSize) {
        assertSetContent(set, expectedSize, null);
    }

    private static void assertSetContent(Set<Object> set, int expectedSize, String prefix) {
        assertEqualsStringFormat("set " + toString(set) + " should contain %d items, but was %d", expectedSize, set.size());

        for (int i = 0; i < expectedSize; i++) {
            Object expectedValue = prefix == null ? i : prefix + i;
            assertTrue("set " + toString(set) + " should contain " + expectedValue, set.contains(expectedValue));
        }
    }
}
