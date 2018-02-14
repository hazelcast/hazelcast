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

package com.hazelcast.collection.impl.set;

import com.hazelcast.collection.impl.collection.AbstractCollectionProxyImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
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
import java.util.Set;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetSplitBrainTest extends SplitBrainTestSupport {

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
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        ISet<Object> set = instances[0].getSet(setNameA);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            set.add("item" + i);
        }

        waitAllForSafeState(instances);

        int partitionId = ((AbstractCollectionProxyImpl) set).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        backupSet = getBackupSet(backupInstance, setNameA);
        assertEquals("backupSet should contain " + INITIAL_COUNT + " items", INITIAL_COUNT, backupSet.size());
        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("backupSet should contain item" + i + " " + toString(backupSet), backupSet.contains("item" + i));
        }
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
        for (int i = 0; i < INITIAL_COUNT; i++) {
            setB2.add("item" + i);
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

        int partitionId = ((AbstractCollectionProxyImpl) setA1).getPartitionId();
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        backupSet = getBackupSet(backupInstance, setNameA);

        setB1 = instances[0].getSet(setNameB);

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
        // we should have these items in the merged setA, since they are added in both clusters
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            setA1.add("item" + i);
            setA2.add("item" + i);
        }

        // we should not have these items in the merged sets, since they are in the smaller cluster only
        for (int i = 0; i < NEW_ITEMS; i++) {
            setA2.add("lostItem" + i);
            setB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertSetContent(setA1, false);
        assertSetContent(setA2, false);
        assertSetContent(backupSet, false);

        assertTrue(setB1.isEmpty());
        assertTrue(setB2.isEmpty());
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            setA1.add("item" + i);
        }

        // we should not lose the additional items from setA2 or the new setB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            setA2.add("item" + i);
            setB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertSetContent(setA1, true);
        assertSetContent(setA2, true);
        assertSetContent(backupSet, true);

        assertSetContent(setB1, true);
        assertSetContent(setB2, true);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS; i++) {
            setA1.add("item" + i);
        }

        // we should not lose the additional items from setA2 or the new setB2
        for (int i = INITIAL_COUNT; i < INITIAL_COUNT + NEW_ITEMS * 2; i++) {
            setA2.add("item" + i);
            setB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertSetContent(setA1, true);
        assertSetContent(setA2, true);
        assertSetContent(backupSet, true);

        assertSetContent(setB1, true);
        assertSetContent(setB2, true);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < NEW_ITEMS; i++) {
            setA2.add(i);
            setA2.add("lostItem" + i);
        }
    }

    private void afterMergeCustomMergePolicy() {
        int expectedSize = INITIAL_COUNT + NEW_ITEMS;
        assertEquals("set1 should contain " + expectedSize + " items", expectedSize, setA1.size());
        assertEquals("set2 should contain " + expectedSize + " items", expectedSize, setA2.size());
        assertEquals("backupSet should contain " + expectedSize + " items " + backupSet, expectedSize, backupSet.size());

        for (int i = 0; i < INITIAL_COUNT; i++) {
            assertTrue("setA1 should contain 'item" + i + "'", setA1.contains("item" + i));
            assertTrue("setA2 should contain 'item" + i + "'", setA2.contains("item" + i));
            assertTrue("backupSet should contain 'item" + i + "' " + backupSet, backupSet.contains("item" + i));
        }
        for (int i = 0; i < NEW_ITEMS; i++) {
            assertTrue("setA1 should contain '" + i + "'", setA1.contains(i));
            assertTrue("setA2 should contain '" + i + "'", setA2.contains(i));
            assertTrue("backupSet should contain '" + i + "' " + backupSet, backupSet.contains(i));

            assertFalse("setA1 should not contain 'lostItem" + i + "'", setA1.contains("lostItem" + i));
            assertFalse("setA2 should not contain 'lostItem" + i + "'", setA2.contains("lostItem" + i));
            assertFalse("backupSet should not contain 'lostItem" + i + "'", backupSet.contains("lostItem" + i));
        }
    }

    private static void assertSetContent(Set<Object> set, boolean hasMergedItems) {
        int expectedSize = INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1);
        assertEquals("set should contain " + expectedSize + " items " + toString(set), expectedSize, set.size());

        for (int i = 0; i < INITIAL_COUNT + NEW_ITEMS * (hasMergedItems ? 2 : 1); i++) {
            assertTrue("set should contain item" + i + " " + toString(set), set.contains("item" + i));
        }
    }
}
