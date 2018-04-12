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

package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
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
import com.hazelcast.test.backup.BackupAccessor;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryEqualsEventually;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryNullEventually;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;
import static com.hazelcast.test.backup.TestBackupUtils.newMapAccessor;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link IMap}.
 * <p>
 * Most merge policies are tested with {@link InMemoryFormat#BINARY} only, since they don't check the value.
 * <p>
 * The {@link MergeIntegerValuesMergePolicy} is tested with both in-memory formats, since it's using the value to merge.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("WeakerAccess")
public class MapSplitBrainTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 3};

    @Parameters(name = "mergePolicy:{0}, format:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {DiscardMergePolicy.class, BINARY},
                {HigherHitsMergePolicy.class, BINARY},
                {LatestAccessMergePolicy.class, BINARY},
                {PassThroughMergePolicy.class, BINARY},
                {PutIfAbsentMergePolicy.class, BINARY},
                {RemoveValuesMergePolicy.class, BINARY},

                {ReturnPiMergePolicy.class, BINARY},
                {ReturnPiMergePolicy.class, OBJECT},
                {MergeIntegerValuesMergePolicy.class, BINARY},
                {MergeIntegerValuesMergePolicy.class, OBJECT},
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    @Parameter(value = 1)
    public InMemoryFormat inMemoryFormat;

    protected String mapNameA = randomMapName("mapA-");
    protected String mapNameB = randomMapName("mapB-");

    private IMap<Object, Object> mapA1;
    private IMap<Object, Object> mapA2;
    private IMap<Object, Object> mapB1;
    private IMap<Object, Object> mapB2;
    private BackupAccessor<Object, Object> backupAccessorA;
    private BackupAccessor<Object, Object> backupAccessorB;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getMapConfig(mapNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(false);
        config.getMapConfig(mapNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(false);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        BackupAccessor<Object, Object> accessor = newMapAccessor(instances, mapNameA);
        assertEquals("backupMap should contain 0 entries", 0, accessor.size());
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        mapA1 = firstBrain[0].getMap(mapNameA);
        mapA2 = secondBrain[0].getMap(mapNameA);
        mapB2 = secondBrain[0].getMap(mapNameB);

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
        } else if (mergePolicyClass == ReturnPiMergePolicy.class) {
            afterSplitReturnPiMergePolicy();
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

        mapB1 = instances[0].getMap(mapNameB);

        backupAccessorA = newMapAccessor(instances, mapNameA);
        backupAccessorB = newMapAccessor(instances, mapNameB);

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
        } else if (mergePolicyClass == ReturnPiMergePolicy.class) {
            afterMergeReturnPiMergePolicy();
        } else if (mergePolicyClass == MergeIntegerValuesMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        mapA1.put("key1", "value1");

        mapA2.put("key1", "DiscardedValue1");
        mapA2.put("key2", "DiscardedValue2");

        mapB2.put("key", "DiscardedValue");
    }

    private void afterMergeDiscardMergePolicy() {
        assertEquals("value1", mapA1.get("key1"));
        assertEquals("value1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "value1", backupAccessorA);

        assertNull(mapA1.get("key2"));
        assertNull(mapA2.get("key2"));
        assertBackupEntryNullEventually("key2", backupAccessorA);

        assertEquals(1, mapA1.size());
        assertEquals(1, mapA2.size());
        assertBackupSizeEventually(1, backupAccessorA);

        assertNull(mapB1.get("key"));
        assertNull(mapB2.get("key"));
        assertBackupEntryNullEventually("key", backupAccessorB);

        assertTrue(mapB1.isEmpty());
        assertTrue(mapB2.isEmpty());
        assertBackupSizeEventually(0, backupAccessorB);
    }

    private void afterSplitHigherHitsMergePolicy() {
        mapA1.put("key1", "HigherHitsValue1");
        mapA1.put("key2", "value2");

        // increase hits number
        assertEquals("HigherHitsValue1", mapA1.get("key1"));
        assertEquals("HigherHitsValue1", mapA1.get("key1"));

        mapA2.put("key1", "value1");
        mapA2.put("key2", "HigherHitsValue2");

        // increase hits number
        assertEquals("HigherHitsValue2", mapA2.get("key2"));
        assertEquals("HigherHitsValue2", mapA2.get("key2"));
    }

    private void afterMergeHigherHitsMergePolicy() {
        assertEquals("HigherHitsValue1", mapA1.get("key1"));
        assertEquals("HigherHitsValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "HigherHitsValue1", backupAccessorA);

        assertEquals("HigherHitsValue2", mapA1.get("key2"));
        assertEquals("HigherHitsValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "HigherHitsValue2", backupAccessorA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupAccessorA);
    }

    private void afterSplitLatestAccessMergePolicy() {
        mapA1.put("key1", "value1");
        // access to record
        assertEquals("value1", mapA1.get("key1"));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        mapA2.put("key1", "LatestAccessedValue1");
        // access to record
        assertEquals("LatestAccessedValue1", mapA2.get("key1"));

        mapA2.put("key2", "value2");
        // access to record
        assertEquals("value2", mapA2.get("key2"));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        mapA1.put("key2", "LatestAccessedValue2");
        // access to record
        assertEquals("LatestAccessedValue2", mapA1.get("key2"));
    }

    private void afterMergeLatestAccessMergePolicy() {
        assertEquals("LatestAccessedValue1", mapA1.get("key1"));
        assertEquals("LatestAccessedValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "LatestAccessedValue1", backupAccessorA);

        assertEquals("LatestAccessedValue2", mapA1.get("key2"));
        assertEquals("LatestAccessedValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "LatestAccessedValue2", backupAccessorA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupAccessorA);
    }

    private void afterSplitLatestUpdateMergePolicy() {
        mapA1.put("key1", "value1");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        mapA2.put("key1", "LatestUpdatedValue1");
        mapA2.put("key2", "value2");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        mapA1.put("key2", "LatestUpdatedValue2");
    }

    private void afterMergeLatestUpdateMergePolicy() {
        assertEquals("LatestUpdatedValue1", mapA1.get("key1"));
        assertEquals("LatestUpdatedValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "LatestUpdatedValue1", backupAccessorA);

        assertEquals("LatestUpdatedValue2", mapA1.get("key2"));
        assertEquals("LatestUpdatedValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "LatestUpdatedValue2", backupAccessorA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupAccessorA);
    }

    private void afterSplitPassThroughMergePolicy() {
        mapA1.put("key1", "value1");

        mapA2.put("key1", "PassThroughValue1");
        mapA2.put("key2", "PassThroughValue2");

        mapB2.put("key", "PassThroughValue");
    }

    private void afterMergePassThroughMergePolicy() {
        assertEquals("PassThroughValue1", mapA1.get("key1"));
        assertEquals("PassThroughValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "PassThroughValue1", backupAccessorA);

        assertEquals("PassThroughValue2", mapA1.get("key2"));
        assertEquals("PassThroughValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "PassThroughValue2", backupAccessorA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupAccessorA);

        assertEquals("PassThroughValue", mapB1.get("key"));
        assertEquals("PassThroughValue", mapB2.get("key"));
        assertBackupEntryEqualsEventually("key", "PassThroughValue", backupAccessorB);

        assertEquals(1, mapB1.size());
        assertEquals(1, mapB2.size());
        assertBackupSizeEventually(1, backupAccessorB);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        mapA1.put("key1", "PutIfAbsentValue1");

        mapA2.put("key1", "value");
        mapA2.put("key2", "PutIfAbsentValue2");

        mapB2.put("key", "PutIfAbsentValue");
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertEquals("PutIfAbsentValue1", mapA1.get("key1"));
        assertEquals("PutIfAbsentValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "PutIfAbsentValue1", backupAccessorA);

        assertEquals("PutIfAbsentValue2", mapA1.get("key2"));
        assertEquals("PutIfAbsentValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "PutIfAbsentValue2", backupAccessorA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupAccessorA);

        assertEquals("PutIfAbsentValue", mapB1.get("key"));
        assertEquals("PutIfAbsentValue", mapB2.get("key"));
        assertBackupEntryEqualsEventually("key", "PutIfAbsentValue", backupAccessorB);

        assertEquals(1, mapB1.size());
        assertEquals(1, mapB2.size());
        assertBackupSizeEventually(1, backupAccessorB);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        mapA1.put("key", "discardedValue1");

        mapA2.put("key", "discardedValue2");

        mapB2.put("key", "discardedValue");
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertNull(mapA1.get("key"));
        assertNull(mapA2.get("key"));
        assertBackupEntryNullEventually("key", backupAccessorA);

        assertEquals(0, mapA1.size());
        assertEquals(0, mapA2.size());
        assertBackupSizeEventually(0, backupAccessorA);

        assertNull(mapB1.get("key"));
        assertNull(mapB2.get("key"));
        assertBackupEntryNullEventually("key", backupAccessorB);

        assertEquals(0, mapB1.size());
        assertEquals(0, mapB2.size());
        assertBackupSizeEventually(0, backupAccessorB);
    }

    private void afterSplitReturnPiMergePolicy() {
        mapA1.put("key", "discardedValue1");

        mapA2.put("key", "discardedValue2");

        mapB2.put("key", "discardedValue");
    }

    private void afterMergeReturnPiMergePolicy() {
        assertPi(mapA1.get("key"));
        assertPi(mapA2.get("key"));
        assertBackupEntryEqualsEventually("key", Math.PI, backupAccessorA);

        assertEquals(1, mapA1.size());
        assertEquals(1, mapA2.size());
        assertBackupSizeEventually(1, backupAccessorA);

        assertPi(mapB1.get("key"));
        assertPi(mapB2.get("key"));
        assertBackupEntryEqualsEventually("key", Math.PI, backupAccessorB);

        assertEquals(1, mapB1.size());
        assertEquals(1, mapB2.size());
        assertBackupSizeEventually(1, backupAccessorB);
    }

    private void afterSplitCustomMergePolicy() {
        mapA1.put("key", "value");
        mapA2.put("key", 1);
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(1, mapA1.get("key"));
        assertEquals(1, mapA2.get("key"));
        assertBackupEntryEqualsEventually("key", 1, backupAccessorA);

        assertEquals(1, mapA1.size());
        assertEquals(1, mapA2.size());
        assertBackupSizeEventually(1, backupAccessorA);
    }
}
