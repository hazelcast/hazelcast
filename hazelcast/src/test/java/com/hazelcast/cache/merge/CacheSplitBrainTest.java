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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link com.hazelcast.cache.ICache}.
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
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class CacheSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, DiscardMergePolicy.class},
                {BINARY, HigherHitsMergePolicy.class},
                {BINARY, LatestAccessMergePolicy.class},
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

    protected String cacheNameA = randomMapName("cacheA-");
    protected String cacheNameB = randomMapName("cacheB-");
    protected ICache<Object, Object> cacheA1;
    protected ICache<Object, Object> cacheA2;
    protected ICache<Object, Object> cacheB1;
    protected ICache<Object, Object> cacheB2;
    protected BackupAccessor<Object, Object> backupCacheA;
    protected BackupAccessor<Object, Object> backupCacheB;
    protected MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        Config config = super.config();
        config.getCacheConfig(cacheNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(false)
                .getMergePolicyConfig().setPolicy(mergePolicyClass.getName());
        config.getCacheConfig(cacheNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(false)
                .getMergePolicyConfig().setPolicy(mergePolicyClass.getName());
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        BackupAccessor<Object, Object> accessor = TestBackupUtils.newCacheAccessor(instances, cacheNameA);
        assertEquals("backupCache should contain 0 entries", 0, accessor.size());
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        cacheA1 = firstBrain[0].getCacheManager().getCache(cacheNameA);
        cacheA2 = secondBrain[0].getCacheManager().getCache(cacheNameA);
        cacheB2 = secondBrain[0].getCacheManager().getCache(cacheNameB);

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

        cacheB1 = instances[0].getCacheManager().getCache(cacheNameB);

        backupCacheA = TestBackupUtils.newCacheAccessor(instances, cacheNameA);
        backupCacheB = TestBackupUtils.newCacheAccessor(instances, cacheNameB);

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
        cacheA1.put("key1", "value1");

        cacheA2.put("key1", "DiscardedValue1");
        cacheA2.put("key2", "DiscardedValue2");

        cacheB2.put("key", "DiscardedValue");
    }

    private void afterMergeDiscardMergePolicy() {
        assertEquals("value1", cacheA1.get("key1"));
        assertEquals("value1", cacheA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "value1", backupCacheA);

        assertNull(cacheA1.get("key2"));
        assertNull(cacheA2.get("key2"));
        assertBackupEntryNullEventually("key2", backupCacheA);

        assertEquals(1, cacheA1.size());
        assertEquals(1, cacheA2.size());
        assertBackupSizeEventually(1, backupCacheA);

        assertNull(cacheB1.get("key"));
        assertNull(cacheB2.get("key"));
        assertBackupEntryNullEventually("key", backupCacheB);

        assertEquals(0, cacheB1.size());
        assertEquals(0, cacheB2.size());
        assertBackupSizeEventually(0, backupCacheB);
    }

    private void afterSplitHigherHitsMergePolicy() {
        cacheA1.put("key1", "higherHitsValue1");
        cacheA1.put("key2", "value2");

        // increase hits number
        assertEquals("higherHitsValue1", cacheA1.get("key1"));
        assertEquals("higherHitsValue1", cacheA1.get("key1"));

        cacheA2.put("key1", "value1");
        cacheA2.put("key2", "higherHitsValue2");

        // increase hits number
        assertEquals("higherHitsValue2", cacheA2.get("key2"));
        assertEquals("higherHitsValue2", cacheA2.get("key2"));
    }

    private void afterMergeHigherHitsMergePolicy() {
        assertEquals("higherHitsValue1", cacheA1.get("key1"));
        assertEquals("higherHitsValue1", cacheA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "higherHitsValue1", backupCacheA);

        assertEquals("higherHitsValue2", cacheA1.get("key2"));
        assertEquals("higherHitsValue2", cacheA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "higherHitsValue2", backupCacheA);

        assertEquals(2, cacheA1.size());
        assertEquals(2, cacheA2.size());
        assertBackupSizeEventually(2, backupCacheA);
    }

    private void afterSplitLatestAccessMergePolicy() {
        cacheA1.put("key1", "value1");
        // access to record
        assertEquals("value1", cacheA1.get("key1"));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        cacheA2.put("key1", "LatestAccessedValue1");
        // access to record
        assertEquals("LatestAccessedValue1", cacheA2.get("key1"));

        cacheA2.put("key2", "value2");
        // access to record
        assertEquals("value2", cacheA2.get("key2"));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        cacheA1.put("key2", "LatestAccessedValue2");
        // access to record
        assertEquals("LatestAccessedValue2", cacheA1.get("key2"));
    }

    private void afterMergeLatestAccessMergePolicy() {
        assertEquals("LatestAccessedValue1", cacheA1.get("key1"));
        assertEquals("LatestAccessedValue1", cacheA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "LatestAccessedValue1", backupCacheA);

        assertEquals("LatestAccessedValue2", cacheA1.get("key2"));
        assertEquals("LatestAccessedValue2", cacheA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "LatestAccessedValue2", backupCacheA);

        assertEquals(2, cacheA1.size());
        assertEquals(2, cacheA2.size());
        assertBackupSizeEventually(2, backupCacheA);
    }

    private void afterSplitLatestUpdateMergePolicy() {
        cacheA1.put("key1", "value1");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        cacheA2.put("key1", "LatestUpdatedValue1");
        cacheA2.put("key2", "value2");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        cacheA1.put("key2", "LatestUpdatedValue2");
    }

    private void afterMergeLatestUpdateMergePolicy() {
        assertEquals("LatestUpdatedValue1", cacheA1.get("key1"));
        assertEquals("LatestUpdatedValue1", cacheA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "LatestUpdatedValue1", backupCacheA);

        assertEquals("LatestUpdatedValue2", cacheA1.get("key2"));
        assertEquals("LatestUpdatedValue2", cacheA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "LatestUpdatedValue2", backupCacheA);

        assertEquals(2, cacheA1.size());
        assertEquals(2, cacheA2.size());
        assertBackupSizeEventually(2, backupCacheA);
    }

    private void afterSplitPassThroughMergePolicy() {
        cacheA1.put("key1", "value1");

        cacheA2.put("key1", "PassThroughValue1");
        cacheA2.put("key2", "PassThroughValue2");

        cacheB2.put("key", "PutIfAbsentValue");
    }

    private void afterMergePassThroughMergePolicy() {
        assertEquals("PassThroughValue1", cacheA1.get("key1"));
        assertEquals("PassThroughValue1", cacheA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "PassThroughValue1", backupCacheA);

        assertEquals("PassThroughValue2", cacheA1.get("key2"));
        assertEquals("PassThroughValue2", cacheA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "PassThroughValue2", backupCacheA);

        assertEquals(2, cacheA1.size());
        assertEquals(2, cacheA2.size());
        assertBackupSizeEventually(2, backupCacheA);

        assertEquals("PutIfAbsentValue", cacheB1.get("key"));
        assertEquals("PutIfAbsentValue", cacheB2.get("key"));
        assertBackupEntryEqualsEventually("key", "PutIfAbsentValue", backupCacheB);

        assertEquals(1, cacheB1.size());
        assertEquals(1, cacheB2.size());
        assertBackupSizeEventually(1, backupCacheB);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        cacheA1.put("key1", "PutIfAbsentValue1");

        cacheA2.put("key1", "value");
        cacheA2.put("key2", "PutIfAbsentValue2");

        cacheB2.put("key", "PutIfAbsentValue");
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertEquals("PutIfAbsentValue1", cacheA1.get("key1"));
        assertEquals("PutIfAbsentValue1", cacheA2.get("key1"));
        assertBackupEntryEqualsEventually("key1", "PutIfAbsentValue1", backupCacheA);

        assertEquals("PutIfAbsentValue2", cacheA1.get("key2"));
        assertEquals("PutIfAbsentValue2", cacheA2.get("key2"));
        assertBackupEntryEqualsEventually("key2", "PutIfAbsentValue2", backupCacheA);

        assertEquals(2, cacheA1.size());
        assertEquals(2, cacheA2.size());
        assertBackupSizeEventually(2, backupCacheA);

        assertEquals("PutIfAbsentValue", cacheB1.get("key"));
        assertEquals("PutIfAbsentValue", cacheB2.get("key"));
        assertBackupEntryEqualsEventually("key", "PutIfAbsentValue", backupCacheB);

        assertEquals(1, cacheB1.size());
        assertEquals(1, cacheB2.size());
        assertBackupSizeEventually(1, backupCacheB);
    }

    private void afterSplitCustomMergePolicy() {
        cacheA1.put("key", "value");
        cacheA2.put("key", 1);
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(1, cacheA1.get("key"));
        assertEquals(1, cacheA2.get("key"));
        assertBackupEntryEqualsEventually("key", 1, backupCacheA);

        assertEquals(1, cacheA1.size());
        assertEquals(1, cacheA2.size());
        assertBackupSizeEventually(1, backupCacheA);
    }
}
