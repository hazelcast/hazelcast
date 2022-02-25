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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.AssertTask;
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

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapSplitBrainTest extends SplitBrainTestSupport {

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

    private String replicatedMapNameA = randomMapName("replicatedMapA-");
    private String replicatedMapNameB = randomMapName("replicatedMapB-");
    private String key;
    private String key1;
    private String key2;
    private ReplicatedMap<Object, Object> replicatedMapA1;
    private ReplicatedMap<Object, Object> replicatedMapA2;
    private ReplicatedMap<Object, Object> replicatedMapB1;
    private ReplicatedMap<Object, Object> replicatedMapB2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getReplicatedMapConfig(replicatedMapNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setStatisticsEnabled(false);
        config.getReplicatedMapConfig(replicatedMapNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setStatisticsEnabled(false);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        // since the statistics are not replicated, we have to use keys on the same node like the proxy we are interacting with
        String[] keys = generateKeysBelongingToSamePartitionsOwnedBy(firstBrain[0], 3);
        key = keys[0];
        key1 = keys[1];
        key2 = keys[2];

        replicatedMapA1 = firstBrain[0].getReplicatedMap(replicatedMapNameA);
        replicatedMapA2 = secondBrain[0].getReplicatedMap(replicatedMapNameA);
        replicatedMapB2 = secondBrain[0].getReplicatedMap(replicatedMapNameB);

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

        replicatedMapB1 = instances[0].getReplicatedMap(replicatedMapNameB);

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
        replicatedMapA1.put(key1, "value1");

        replicatedMapA2.put(key1, "DiscardedValue1");
        replicatedMapA2.put(key2, "DiscardedValue2");

        replicatedMapB2.put(key, "DiscardedValue");
    }

    private void afterMergeDiscardMergePolicy() {
        assertReplicatedMapsA(key1, "value1");
        assertReplicatedMapsA(key2, null);
        assertReplicatedMapsSizeA(1);

        assertReplicatedMapsB(key, null);
        assertReplicatedMapsSizeB(0);
    }

    private void afterSplitHigherHitsMergePolicy() {
        replicatedMapA1.put(key1, "HigherHitsValue1");
        replicatedMapA1.put(key2, "value2");

        // increase hits number
        assertEquals("HigherHitsValue1", replicatedMapA1.get(key1));
        assertEquals("HigherHitsValue1", replicatedMapA1.get(key1));

        replicatedMapA2.put(key1, "value1");
        replicatedMapA2.put(key2, "HigherHitsValue2");

        // increase hits number
        assertEquals("HigherHitsValue2", replicatedMapA2.get(key2));
        assertEquals("HigherHitsValue2", replicatedMapA2.get(key2));
    }

    private void afterMergeHigherHitsMergePolicy() {
        assertReplicatedMapsA(key1, "HigherHitsValue1");
        assertReplicatedMapsA(key2, "HigherHitsValue2");
        assertReplicatedMapsSizeA(2);
    }

    private void afterSplitLatestAccessMergePolicy() {
        replicatedMapA1.put(key1, "value1");
        // access to record
        assertEquals("value1", replicatedMapA1.get(key1));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMapA2.put(key1, "LatestAccessedValue1");
        // access to record
        assertEquals("LatestAccessedValue1", replicatedMapA2.get(key1));

        replicatedMapA2.put(key2, "value2");
        // access to record
        assertEquals("value2", replicatedMapA2.get(key2));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMapA1.put(key2, "LatestAccessedValue2");
        // access to record
        assertEquals("LatestAccessedValue2", replicatedMapA1.get(key2));
    }

    private void afterMergeLatestAccessMergePolicy() {
        assertReplicatedMapsA(key1, "LatestAccessedValue1");
        assertReplicatedMapsA(key2, "LatestAccessedValue2");
        assertReplicatedMapsSizeA(2);
    }

    private void afterSplitLatestUpdateMergePolicy() {
        replicatedMapA1.put(key1, "value1");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMapA2.put(key1, "LatestUpdatedValue1");
        replicatedMapA2.put(key2, "value2");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMapA1.put(key2, "LatestUpdatedValue2");
    }

    private void afterMergeLatestUpdateMergePolicy() {
        assertReplicatedMapsA(key1, "LatestUpdatedValue1");
        assertReplicatedMapsA(key2, "LatestUpdatedValue2");
        assertReplicatedMapsSizeA(2);
    }

    private void afterSplitPassThroughMergePolicy() {
        replicatedMapA1.put(key1, "value1");

        replicatedMapA2.put(key1, "PassThroughValue1");
        replicatedMapA2.put(key2, "PassThroughValue2");

        replicatedMapB2.put(key, "PassThroughValue");
    }

    private void afterMergePassThroughMergePolicy() {
        assertReplicatedMapsA(key1, "PassThroughValue1");
        assertReplicatedMapsA(key2, "PassThroughValue2");
        assertReplicatedMapsSizeA(2);

        assertReplicatedMapsB(key, "PassThroughValue");
        assertReplicatedMapsSizeB(1);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        replicatedMapA1.put(key1, "PutIfAbsentValue1");

        replicatedMapA2.put(key1, "value");
        replicatedMapA2.put(key2, "PutIfAbsentValue2");

        replicatedMapB2.put(key, "PutIfAbsentValue");
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertReplicatedMapsA(key1, "PutIfAbsentValue1");
        assertReplicatedMapsA(key2, "PutIfAbsentValue2");
        assertReplicatedMapsSizeA(2);

        assertReplicatedMapsB(key, "PutIfAbsentValue");
        assertReplicatedMapsSizeB(1);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        replicatedMapA1.put(key, "discardedValue1");

        replicatedMapA2.put(key, "discardedValue2");

        replicatedMapB2.put(key, "discardedValue");
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertReplicatedMapsA(key, null);
        assertReplicatedMapsSizeA(0);

        assertReplicatedMapsB(key, null);
        assertReplicatedMapsSizeB(0);
    }

    private void afterSplitCustomMergePolicy() {
        replicatedMapA1.put(key, "value");
        replicatedMapA2.put(key, 23);
    }

    private void afterSplitReturnPiMergePolicy() {
        replicatedMapA1.put("key", "discardedValue1");

        replicatedMapA2.put("key", "discardedValue2");

        replicatedMapB2.put("key", "discardedValue");
    }

    private void afterMergeReturnPiMergePolicy() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertPi(replicatedMapA1.get("key"));
                assertPi(replicatedMapA2.get("key"));

                assertEquals(1, replicatedMapA1.size());
                assertEquals(1, replicatedMapA2.size());

                assertPi(replicatedMapB1.get("key"));
                assertPi(replicatedMapB2.get("key"));

                assertEquals(1, replicatedMapB1.size());
                assertEquals(1, replicatedMapB2.size());
            }
        });
    }

    private void afterMergeCustomMergePolicy() {
        assertReplicatedMapsA(key, 23);
        assertReplicatedMapsSizeA(1);
    }

    private void assertReplicatedMapsA(Object key, Object expectedValue) {
        assertReplicatedMaps(replicatedMapA1, replicatedMapA2, key, expectedValue);
    }

    private void assertReplicatedMapsB(Object key, Object expectedValue) {
        assertReplicatedMaps(replicatedMapB1, replicatedMapB2, key, expectedValue);
    }

    private static void assertReplicatedMaps(final ReplicatedMap<Object, Object> replicatedMap1,
                                             final ReplicatedMap<Object, Object> replicatedMap2,
                                             final Object key, final Object expectedValue) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEqualsStringFormat("expected value %s in replicatedMap1, but was %s",
                        expectedValue, replicatedMap1.get(key));
                assertEqualsStringFormat("expected value %s in replicatedMap2, but was %s",
                        expectedValue, replicatedMap2.get(key));
            }
        });
    }

    private void assertReplicatedMapsSizeA(int expectedSize) {
        assertReplicatedMapsSize(replicatedMapA1, replicatedMapA2, expectedSize);
    }

    private void assertReplicatedMapsSizeB(int expectedSize) {
        assertReplicatedMapsSize(replicatedMapB1, replicatedMapB2, expectedSize);
    }

    private static void assertReplicatedMapsSize(final ReplicatedMap<Object, Object> replicatedMap1,
                                                 final ReplicatedMap<Object, Object> replicatedMap2,
                                                 final int expectedSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEqualsStringFormat("replicatedMap1 should have size %d, but was %d", expectedSize, replicatedMap1.size());
                assertEqualsStringFormat("replicatedMap2 should have size %d, but was %d", expectedSize, replicatedMap2.size());
            }
        });
    }
}
