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

package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryEqualsEventually;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryNullEventually;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;
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
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(SlowTest.class)
@SuppressWarnings("WeakerAccess")
public class MapSplitBrainTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 3};

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, DiscardMergePolicy.class},
                {BINARY, HigherHitsMergePolicy.class},
                {BINARY, LatestAccessMergePolicy.class},
                {BINARY, LatestUpdateMergePolicy.class},
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

    protected String mapNameA = randomMapName("mapA-");
    protected String mapNameB = randomMapName("mapB-");

    private IMap<Object, Object> mapA1;
    private IMap<Object, Object> mapA2;
    private IMap<Object, Object> mapB1;
    private IMap<Object, Object> mapB2;
    private BackupAccessor<Object, Object> backupMapA;
    private BackupAccessor<Object, Object> backupMapB;
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
                .setStatisticsEnabled(true)
                .setPerEntryStatsEnabled(true);
        config.getMapConfig(mapNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(true)
                .setPerEntryStatsEnabled(true);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        BackupAccessor<Object, Object> accessor = TestBackupUtils.newMapAccessor(instances, mapNameA);
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

        backupMapA = TestBackupUtils.newMapAccessor(instances, mapNameA);
        backupMapB = TestBackupUtils.newMapAccessor(instances, mapNameB);

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
        assertBackupEntryEqualsEventually("key1", "value1", backupMapA);

        assertNull(mapA1.get("key2"));
        assertNull(mapA2.get("key2"));
        assertBackupEntryNullEventually("key2", backupMapA);

        assertEquals(1, mapA1.size());
        assertEquals(1, mapA2.size());
        assertBackupSizeEventually(1, backupMapA);

        assertNull(mapB1.get("key"));
        assertNull(mapB2.get("key"));
        assertBackupEntryNullEventually("key", backupMapB);

        assertTrue(mapB1.isEmpty());
        assertTrue(mapB2.isEmpty());
        assertBackupSizeEventually(0, backupMapB);
    }

    private void afterSplitHigherHitsMergePolicy() {
        mapA1.put("key1", "higherHitsValue1");
        mapA1.put("key2", "value2");

        // increase hits number
        assertEquals("higherHitsValue1", mapA1.get("key1"));
        assertEquals("higherHitsValue1", mapA1.get("key1"));

        mapA2.put("key1", "value1");
        mapA2.put("key2", "higherHitsValue2");

        // increase hits number
        assertEquals("higherHitsValue2", mapA2.get("key2"));
        assertEquals("higherHitsValue2", mapA2.get("key2"));
    }

    private void afterMergeHigherHitsMergePolicy() {
        assertEquals("higherHitsValue1", mapA1.get("key1"));
        assertEquals("higherHitsValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "higherHitsValue1", backupMapA);

        assertEquals("higherHitsValue2", mapA1.get("key2"));
        assertEquals("higherHitsValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "higherHitsValue2", backupMapA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupMapA);
    }

    private void afterSplitLatestAccessMergePolicy() {
        mapA1.put("key1", "value1");
        // access to record
        assertEquals("value1", mapA1.get("key1"));

        // prevent updating at the same time
        sleepAtLeastMillis(1000);

        mapA2.put("key1", "LatestAccessedValue1");
        // access to record
        assertEquals("LatestAccessedValue1", mapA2.get("key1"));

        mapA2.put("key2", "value2");
        // access to record
        assertEquals("value2", mapA2.get("key2"));

        // prevent updating at the same time
        sleepAtLeastMillis(1000);

        mapA1.put("key2", "LatestAccessedValue2");
        // access to record
        assertEquals("LatestAccessedValue2", mapA1.get("key2"));
    }

    private void afterMergeLatestAccessMergePolicy() {
        assertEquals("LatestAccessedValue1", mapA1.get("key1"));
        assertEquals("LatestAccessedValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "LatestAccessedValue1", backupMapA);

        assertEquals("LatestAccessedValue2", mapA1.get("key2"));
        assertEquals("LatestAccessedValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "LatestAccessedValue2", backupMapA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupMapA);
    }

    private void afterSplitLatestUpdateMergePolicy() {
        mapA1.put("key1", "value1");

        // prevent updating at the same time
        sleepAtLeastMillis(1000);

        mapA2.put("key1", "LatestUpdatedValue1");
        mapA2.put("key2", "value2");

        // prevent updating at the same time
        sleepAtLeastMillis(1000);

        mapA1.put("key2", "LatestUpdatedValue2");
    }

    private void afterMergeLatestUpdateMergePolicy() {
        assertEquals("LatestUpdatedValue1", mapA1.get("key1"));
        assertEquals("LatestUpdatedValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "LatestUpdatedValue1", backupMapA);

        assertEquals("LatestUpdatedValue2", mapA1.get("key2"));
        assertEquals("LatestUpdatedValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "LatestUpdatedValue2", backupMapA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupMapA);
    }

    private void afterSplitPassThroughMergePolicy() {
        mapA1.put("key1", "value1");

        mapA2.put("key1", "PassThroughValue1");
        mapA2.put("key2", "PassThroughValue2");

        mapB2.put("key", "PutIfAbsentValue");
    }

    private void afterMergePassThroughMergePolicy() {
        assertEquals("PassThroughValue1", mapA1.get("key1"));
        assertEquals("PassThroughValue1", mapA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "PassThroughValue1", backupMapA);

        assertEquals("PassThroughValue2", mapA1.get("key2"));
        assertEquals("PassThroughValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "PassThroughValue2", backupMapA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupMapA);

        assertEquals("PutIfAbsentValue", mapB1.get("key"));
        assertEquals("PutIfAbsentValue", mapB2.get("key"));
        assertBackupEntryEqualsEventually("key", "PutIfAbsentValue", backupMapB);

        assertEquals(1, mapB1.size());
        assertEquals(1, mapB2.size());
        assertBackupSizeEventually(1, backupMapB);
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
        assertBackupEntryEqualsEventually("key1", "PutIfAbsentValue1", backupMapA);

        assertEquals("PutIfAbsentValue2", mapA1.get("key2"));
        assertEquals("PutIfAbsentValue2", mapA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "PutIfAbsentValue2", backupMapA);

        assertEquals(2, mapA1.size());
        assertEquals(2, mapA2.size());
        assertBackupSizeEventually(2, backupMapA);

        assertEquals("PutIfAbsentValue", mapB1.get("key"));
        assertEquals("PutIfAbsentValue", mapB2.get("key"));
        assertBackupEntryEqualsEventually("key", "PutIfAbsentValue", backupMapB);

        assertEquals(1, mapB1.size());
        assertEquals(1, mapB2.size());
        assertBackupSizeEventually(1, backupMapB);
    }

    private void afterSplitCustomMergePolicy() {
        mapA1.put("key", "value");
        mapA2.put("key", 1);
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(1, mapA1.get("key"));
        assertEquals(1, mapA2.get("key"));
        assertBackupEntryEqualsEventually("key", 1, backupMapA);

        assertEquals(1, mapA1.size());
        assertEquals(1, mapA2.size());
        assertBackupSizeEventually(1, backupMapA);
    }
}
