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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.AssertTask;
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

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
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
public class ReplicatedMapSplitBrainTest extends SplitBrainTestSupport {

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
                .setMergePolicyConfig(mergePolicyConfig);
        config.getReplicatedMapConfig(replicatedMapNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig);
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

        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() {
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
        };

        // wait completion of migration tasks after lite member promotion
        assertTrueEventually(assertTask);
    }

    private void afterSplitDiscardMergePolicy() {
        replicatedMapA1.put(key1, "value1");

        replicatedMapA2.put(key1, "DiscardedValue1");
        replicatedMapA2.put(key2, "DiscardedValue2");

        replicatedMapB2.put(key, "DiscardedValue");
    }

    private void afterMergeDiscardMergePolicy() {
        assertEquals("value1", replicatedMapA1.get(key1));
        assertEquals("value1", replicatedMapA2.get(key1));

        assertNull(replicatedMapA1.get(key2));
        assertNull(replicatedMapA2.get(key2));

        assertEquals(1, replicatedMapA1.size());
        assertEquals(1, replicatedMapA2.size());

        assertNull(replicatedMapB1.get(key));
        assertNull(replicatedMapB2.get(key));

        assertTrue(replicatedMapB1.isEmpty());
        assertTrue(replicatedMapB2.isEmpty());
    }

    private void afterSplitHigherHitsMergePolicy() {
        replicatedMapA1.put(key1, "higherHitsValue1");
        replicatedMapA1.put(key2, "value2");

        // increase hits number
        assertEquals("higherHitsValue1", replicatedMapA1.get(key1));
        assertEquals("higherHitsValue1", replicatedMapA1.get(key1));

        replicatedMapA2.put(key1, "value1");
        replicatedMapA2.put(key2, "higherHitsValue2");

        // increase hits number
        assertEquals("higherHitsValue2", replicatedMapA2.get(key2));
        assertEquals("higherHitsValue2", replicatedMapA2.get(key2));
    }

    private void afterMergeHigherHitsMergePolicy() {
        assertEquals("higherHitsValue1", replicatedMapA1.get(key1));
        assertEquals("higherHitsValue1", replicatedMapA2.get(key1));

        assertEquals("higherHitsValue2", replicatedMapA1.get(key2));
        assertEquals("higherHitsValue2", replicatedMapA2.get(key2));

        assertEquals(2, replicatedMapA1.size());
        assertEquals(2, replicatedMapA2.size());
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
        assertEquals("LatestAccessedValue1", replicatedMapA1.get(key1));
        assertEquals("LatestAccessedValue1", replicatedMapA2.get(key1));

        assertEquals("LatestAccessedValue2", replicatedMapA1.get(key2));
        assertEquals("LatestAccessedValue2", replicatedMapA2.get(key2));

        assertEquals(2, replicatedMapA1.size());
        assertEquals(2, replicatedMapA2.size());
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
        assertEquals("LatestUpdatedValue1", replicatedMapA1.get(key1));
        assertEquals("LatestUpdatedValue1", replicatedMapA2.get(key1));

        assertEquals("LatestUpdatedValue2", replicatedMapA1.get(key2));
        assertEquals("LatestUpdatedValue2", replicatedMapA2.get(key2));

        assertEquals(2, replicatedMapA1.size());
        assertEquals(2, replicatedMapA2.size());
    }

    private void afterSplitPassThroughMergePolicy() {
        replicatedMapA1.put(key1, "value1");

        replicatedMapA2.put(key1, "PassThroughValue1");
        replicatedMapA2.put(key2, "PassThroughValue2");

        replicatedMapB2.put(key, "PutIfAbsentValue");
    }

    private void afterMergePassThroughMergePolicy() {
        assertEquals("PassThroughValue1", replicatedMapA1.get(key1));
        assertEquals("PassThroughValue1", replicatedMapA2.get(key1));

        assertEquals("PassThroughValue2", replicatedMapA1.get(key2));
        assertEquals("PassThroughValue2", replicatedMapA2.get(key2));

        assertEquals(2, replicatedMapA1.size());
        assertEquals(2, replicatedMapA2.size());

        assertEquals("PutIfAbsentValue", replicatedMapB1.get(key));
        assertEquals("PutIfAbsentValue", replicatedMapB2.get(key));

        assertEquals(1, replicatedMapB1.size());
        assertEquals(1, replicatedMapB2.size());
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        replicatedMapA1.put(key1, "PutIfAbsentValue1");

        replicatedMapA2.put(key1, "value");
        replicatedMapA2.put(key2, "PutIfAbsentValue2");

        replicatedMapB2.put(key, "PutIfAbsentValue");
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertEquals("PutIfAbsentValue1", replicatedMapA1.get(key1));
        assertEquals("PutIfAbsentValue1", replicatedMapA2.get(key1));

        assertEquals("PutIfAbsentValue2", replicatedMapA1.get(key2));
        assertEquals("PutIfAbsentValue2", replicatedMapA2.get(key2));

        assertEquals(2, replicatedMapA1.size());
        assertEquals(2, replicatedMapA2.size());

        assertEquals("PutIfAbsentValue", replicatedMapB1.get(key));
        assertEquals("PutIfAbsentValue", replicatedMapB2.get(key));

        assertEquals(1, replicatedMapB1.size());
        assertEquals(1, replicatedMapB2.size());
    }

    private void afterSplitCustomMergePolicy() {
        replicatedMapA1.put(key, "value");
        replicatedMapA2.put(key, 1);
    }

    private void afterMergeCustomMergePolicy() {
        assertEquals(1, replicatedMapA1.get(key));
        assertEquals(1, replicatedMapA2.get(key));

        assertEquals(1, replicatedMapA1.size());
        assertEquals(1, replicatedMapA2.size());
    }
}
