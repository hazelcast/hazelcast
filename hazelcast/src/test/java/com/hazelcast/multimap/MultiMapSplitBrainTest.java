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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
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
import java.util.Map;

import static com.hazelcast.multimap.MultiMapTestUtil.getBackupMultiMap;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link MultiMap}.
 * <p>
 * Most merge policies are tested with {@link MultiMapConfig#isBinary()} as {@code true} only, since they don't check the value.
 * <p>
 * The {@link MergeCollectionOfIntegerValuesMergePolicy} is tested with both in-memory formats,
 * since it's using the value to merge.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "mergePolicy:{0}, isBinary:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {DiscardMergePolicy.class, true},
                {HigherHitsMergePolicy.class, true},
                {LatestAccessMergePolicy.class, true},
                {LatestUpdateMergePolicy.class, true},
                {PassThroughMergePolicy.class, true},
                {PutIfAbsentMergePolicy.class, true},
                {RemoveValuesMergePolicy.class, true},

                {ReturnPiCollectionMergePolicy.class, true},
                {ReturnPiCollectionMergePolicy.class, false},
                {MergeCollectionOfIntegerValuesMergePolicy.class, true},
                {MergeCollectionOfIntegerValuesMergePolicy.class, false},
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    @Parameter(value = 1)
    public boolean isBinary;

    private String multiMapNameA = randomMapName("multiMapA-");
    private String multiMapNameB = randomMapName("multiMapB-");
    private MultiMap<Object, Object> multiMapA1;
    private MultiMap<Object, Object> multiMapA2;
    private MultiMap<Object, Object> multiMapB1;
    private MultiMap<Object, Object> multiMapB2;
    private Map<Object, Collection<Object>> backupMultiMapA;
    private Map<Object, Collection<Object>> backupMultiMapB;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getMultiMapConfig(multiMapNameA)
                .setBinary(isBinary)
                .setMergePolicyConfig(mergePolicyConfig)
                .setStatisticsEnabled(true)
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        config.getMultiMapConfig(multiMapNameB)
                .setBinary(isBinary)
                .setMergePolicyConfig(mergePolicyConfig)
                .setStatisticsEnabled(true)
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        Map<Object, Collection<Object>> backupMap = getBackupMultiMap(instances, multiMapNameA);
        assertEquals("backupMultiMap should contain 0 entries", 0, backupMap.size());
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        multiMapA1 = firstBrain[0].getMultiMap(multiMapNameA);
        multiMapA2 = secondBrain[0].getMultiMap(multiMapNameA);
        multiMapB2 = secondBrain[0].getMultiMap(multiMapNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == HigherHitsMergePolicy.class) {
            afterSplitHigherHitsMergePolicy();
        } else if (mergePolicyClass == LatestAccessMergePolicy.class) {
            afterSplitLatestAccessMergePolicy();
        } else if (mergePolicyClass == LatestUpdateMergePolicy.class) {
            afterSplitLatestUpdateMergePolicy();
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

        multiMapB1 = instances[0].getMultiMap(multiMapNameB);

        backupMultiMapA = getBackupMultiMap(instances, multiMapNameA);
        backupMultiMapB = getBackupMultiMap(instances, multiMapNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == HigherHitsMergePolicy.class) {
            afterMergeHigherHitsMergePolicy();
        } else if (mergePolicyClass == LatestAccessMergePolicy.class) {
            afterMergeLatestAccessMergePolicy();
        } else if (mergePolicyClass == LatestUpdateMergePolicy.class) {
            afterMergeLatestUpdateMergePolicy();
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
        multiMapA1.put("key1", "value1");
        multiMapA1.put("key1", "value2");

        multiMapA2.put("key1", "DiscardedValue1a");
        multiMapA2.put("key1", "DiscardedValue1b");
        multiMapA2.put("key2", "DiscardedValue2a");
        multiMapA2.put("key2", "DiscardedValue2b");

        multiMapB2.put("key", "DiscardedValue1");
        multiMapB2.put("key", "DiscardedValue2");
    }

    private void afterMergeDiscardMergePolicy() {
        assertMultiMapsA("key1", "value1", "value2");
        assertMultiMapsA("key2");
        assertMultiMapsSizeA(2);

        assertMultiMapsB("key");
        assertMultiMapsSizeB(0);
    }

    /**
     * The hits are measured per MultiMapValue, so we can test it with multiple keys on the same map.
     */
    private void afterSplitHigherHitsMergePolicy() {
        multiMapA1.put("key1", "higherHitsValue1");
        multiMapA1.put("key2", "value2");

        // increase hits number
        multiMapA1.get("key1");
        multiMapA1.get("key1");

        multiMapA2.put("key1", "value1");
        multiMapA2.put("key2", "higherHitsValue2");

        // increase hits number
        multiMapA2.get("key2");
        multiMapA2.get("key2");
    }

    private void afterMergeHigherHitsMergePolicy() {
        assertMultiMapsA("key1", "higherHitsValue1");
        assertMultiMapsA("key2", "higherHitsValue2");
        assertMultiMapsSizeA(2);
    }

    /**
     * The lastAccessTime is measured per MultiMapContainer, so we cannot test it with multiple keys on the same map.
     */
    private void afterSplitLatestAccessMergePolicy() {
        multiMapA1.put("key", "value");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        multiMapA2.put("key", "LatestAccessedValue");

        multiMapB2.put("key", "LatestAccessedValue");
    }

    private void afterMergeLatestAccessMergePolicy() {
        assertMultiMapsA("key", "LatestAccessedValue");
        assertMultiMapsSizeA(1);

        assertMultiMapsB("key", "LatestAccessedValue");
        assertMultiMapsSizeB(1);
    }

    /**
     * The lastUpdateTime is measured per MultiMapContainer, so we cannot test it with multiple keys on the same map.
     */
    private void afterSplitLatestUpdateMergePolicy() {
        multiMapA1.put("key", "value");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        multiMapA2.put("key", "LatestUpdatedValue");
        multiMapB2.put("key", "LatestUpdatedValue");
    }

    private void afterMergeLatestUpdateMergePolicy() {
        assertMultiMapsA("key", "LatestUpdatedValue");
        assertMultiMapsSizeA(1);

        assertMultiMapsB("key", "LatestUpdatedValue");
        assertMultiMapsSizeB(1);
    }

    private void afterSplitPassThroughMergePolicy() {
        multiMapA1.lock("lockedKey");
        multiMapA1.put("lockedKey", "lockedValue");
        multiMapA1.put("key1", "value1");
        multiMapA1.put("key1", "value2");

        multiMapA2.put("lockedKey", "PassThroughValue");
        multiMapA2.put("key1", "PassThroughValue1a");
        multiMapA2.put("key1", "PassThroughValue1b");
        multiMapA2.put("key2", "PassThroughValue2a");
        multiMapA2.put("key2", "PassThroughValue2b");

        multiMapB2.put("key", "PassThroughValue");
    }

    private void afterMergePassThroughMergePolicy() {
        assertTrue("Expected lockedKey to be locked", multiMapA1.isLocked("lockedKey"));
        multiMapA1.unlock("lockedKey");
        assertFalse("Expected lockedKey to be unlocked", multiMapA1.isLocked("lockedKey"));

        assertMultiMapsA("lockedKey", "lockedValue");
        assertMultiMapsA("key1", "PassThroughValue1a", "PassThroughValue1b");
        assertMultiMapsA("key2", "PassThroughValue2a", "PassThroughValue2b");
        assertMultiMapsSizeA(5);

        assertMultiMapsB("key", "PassThroughValue");
        assertMultiMapsSizeB(1);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        multiMapA1.put("key1", "PutIfAbsentValue1a");
        multiMapA1.put("key1", "PutIfAbsentValue1b");

        multiMapA2.put("key1", "value");
        multiMapA2.put("key2", "PutIfAbsentValue2a");
        multiMapA2.put("key2", "PutIfAbsentValue2b");

        multiMapB2.put("key", "PutIfAbsentValue");
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertMultiMapsA("key1", "PutIfAbsentValue1a", "PutIfAbsentValue1b");
        assertMultiMapsA("key2", "PutIfAbsentValue2a", "PutIfAbsentValue2b");
        assertMultiMapsSizeA(4);

        assertMultiMapsB("key", "PutIfAbsentValue");
        assertMultiMapsSizeB(1);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        multiMapA1.put("key1", "discardedValue1a");
        multiMapA1.put("key1", "discardedValue1b");

        multiMapA2.put("key1", "discardedValue2");
        multiMapA2.put("key2", "discardedValue2a");
        multiMapA2.put("key2", "discardedValue2b");

        multiMapB2.put("key", "discardedValue");
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertMultiMapsA("key1");
        assertMultiMapsA("key2");
        assertMultiMapsSizeA(0);

        assertMultiMapsB("key");
        assertMultiMapsSizeB(0);
    }

    private void afterSplitReturnPiCollectionMergePolicy() {
        multiMapA1.put("key1", "discardedValue1a");
        multiMapA1.put("key1", "discardedValue1b");

        multiMapA2.put("key1", "discardedValue2");
        multiMapA2.put("key2", "discardedValue2a");
        multiMapA2.put("key2", "discardedValue2b");

        multiMapB2.put("key", "discardedValue");
    }

    private void afterMergeReturnPiCollectionMergePolicy() {
        assertPiSet(multiMapA1.get("key1"));
        assertPiSet(multiMapA2.get("key1"));
        assertPiSet(backupMultiMapA.get("key1"));

        assertPiSet(multiMapA1.get("key2"));
        assertPiSet(multiMapA2.get("key2"));
        assertPiSet(backupMultiMapA.get("key2"));

        assertPiSet(multiMapB1.get("key"));
        assertPiSet(multiMapB2.get("key"));
    }

    private void afterSplitCustomMergePolicy() {
        // for key1 just the Integer values survive
        multiMapA1.put("key1", "value1");
        multiMapA1.put("key1", 23);
        multiMapA2.put("key1", "value2");
        multiMapA2.put("key1", 42);

        // key2 is completely removed
        multiMapA1.put("key2", "value1");
        multiMapA2.put("key2", "value2");

        // key3 survives, since there is no mergingValue for it
        multiMapA1.put("key3", "value");

        multiMapB2.put("key1", 42);
        multiMapB2.put("key2", "value");
    }

    private void afterMergeCustomMergePolicy() {
        assertMultiMapsA("key1", 23, 42);
        assertMultiMapsA("key2");
        assertMultiMapsA("key3", "value");
        assertMultiMapsSizeA(3);

        assertMultiMapsB("key1", 42);
        assertMultiMapsB("key2");
        assertMultiMapsSizeB(1);
    }

    private void assertMultiMapsA(String key, Object... expectedValues) {
        assertMultiMaps(multiMapA1, multiMapA2, backupMultiMapA, key, expectedValues);
    }

    private void assertMultiMapsB(String key, Object... expectedValues) {
        assertMultiMaps(multiMapB1, multiMapB2, backupMultiMapB, key, expectedValues);
    }

    private static void assertMultiMaps(MultiMap<Object, Object> multiMap1, MultiMap<Object, Object> multiMap2,
                                        Map<Object, Collection<Object>> backupMultiMap, String key, Object... expectedValues) {
        Collection<Object> collection1 = multiMap1.get(key);
        Collection<Object> collection2 = multiMap2.get(key);
        Collection<Object> backupCollection = backupMultiMap.get(key);
        if (expectedValues.length > 0) {
            Collection<Object> expected = asList(expectedValues);
            assertContainsAll(collection1, expected);
            assertContainsAll(collection2, expected);
            assertContainsAll(backupCollection, expected);

            assertEquals(expectedValues.length, multiMap1.valueCount(key));
            assertEquals(expectedValues.length, multiMap2.valueCount(key));
            assertEquals(expectedValues.length, backupCollection.size());
        } else {
            assertTrue("multiMap1 should be empty for " + key + ", but was " + collection1, collection1.isEmpty());
            assertTrue("multiMap2 should be empty for " + key + ", but was " + collection2, collection2.isEmpty());
            assertNull("backupMultiMap should be null for " + key + ", but was " + backupCollection, backupCollection);

            assertEquals(0, multiMap1.valueCount(key));
            assertEquals(0, multiMap2.valueCount(key));
        }
    }

    private void assertMultiMapsSizeA(int expectedSize) {
        assertMultiMapsSize(multiMapA1, multiMapA2, backupMultiMapA, expectedSize);
    }

    private void assertMultiMapsSizeB(int expectedSize) {
        assertMultiMapsSize(multiMapB1, multiMapB2, backupMultiMapB, expectedSize);
    }

    private static void assertMultiMapsSize(MultiMap<?, ?> multiMap1, MultiMap<?, ?> multiMap2,
                                            Map<?, ? extends Collection<?>> backupMultiMap, int expectedSize) {
        assertEqualsStringFormat("multiMap1 should have size %d, but was %d", expectedSize, multiMap1.size());
        assertEqualsStringFormat("multiMap2 should have size %d, but was %d", expectedSize, multiMap2.size());
        int actualBackupSize = 0;
        for (Collection<?> values : backupMultiMap.values()) {
            actualBackupSize += values.size();
        }
        assertEqualsStringFormat("backupMultiMap should have size %d, but was %d", expectedSize, actualBackupSize);
    }
}
