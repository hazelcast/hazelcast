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
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class LegacyReplicatedMapSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                LatestUpdateMapMergePolicy.class,
                HigherHitsMapMergePolicy.class,
                PutIfAbsentMapMergePolicy.class,
                PassThroughMergePolicy.class,
                CustomReplicatedMergePolicy.class,
        });
    }

    @Parameter
    public Class<? extends ReplicatedMapMergePolicy> mergePolicyClass;

    private String replicatedMapName = randomMapName();
    private MergeLifecycleListener mergeLifecycleListener;
    private ReplicatedMap<Object, Object> replicatedMap1;
    private ReplicatedMap<Object, Object> replicatedMap2;
    private String key1;
    private String key2;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getReplicatedMapConfig(replicatedMapName)
                .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        replicatedMap1 = firstBrain[0].getReplicatedMap(replicatedMapName);
        replicatedMap2 = secondBrain[0].getReplicatedMap(replicatedMapName);

        // since the statistics are not replicated, we have to use keys on the same node like the proxy we are interacting with
        String[] keys = generateKeysBelongingToSamePartitionsOwnedBy(firstBrain[0], 2);
        key1 = keys[0];
        key2 = keys[1];

        if (mergePolicyClass == LatestUpdateMapMergePolicy.class) {
            afterSplitLatestUpdateMapMergePolicy();
        } else if (mergePolicyClass == HigherHitsMapMergePolicy.class) {
            afterSplitHigherHitsMapMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMapMergePolicy.class) {
            afterSplitPutIfAbsentMapMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMapMergePolicy();
        } else if (mergePolicyClass == CustomReplicatedMergePolicy.class) {
            afterSplitCustomReplicatedMapMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() {
                if (mergePolicyClass == LatestUpdateMapMergePolicy.class) {
                    afterMergeLatestUpdateMapMergePolicy();
                } else if (mergePolicyClass == HigherHitsMapMergePolicy.class) {
                    afterMergeHigherHitsMapMergePolicy();
                } else if (mergePolicyClass == PutIfAbsentMapMergePolicy.class) {
                    afterMergePutIfAbsentMapMergePolicy();
                } else if (mergePolicyClass == PassThroughMergePolicy.class) {
                    afterMergePassThroughMapMergePolicy();
                } else if (mergePolicyClass == CustomReplicatedMergePolicy.class) {
                    afterMergeCustomReplicatedMapMergePolicy();
                } else {
                    fail();
                }
            }
        };

        // wait completion of migration tasks after lite member promotion
        assertTrueEventually(assertTask);
    }

    private void afterSplitLatestUpdateMapMergePolicy() {
        replicatedMap1.put(key1, "value1");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMap2.put(key1, "LatestUpdatedValue1");
        replicatedMap2.put(key2, "value2");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMap1.put(key2, "LatestUpdatedValue2");
    }

    private void afterMergeLatestUpdateMapMergePolicy() {
        assertEquals("LatestUpdatedValue1", replicatedMap1.get(key1));
        assertEquals("LatestUpdatedValue1", replicatedMap2.get(key1));

        assertEquals("LatestUpdatedValue2", replicatedMap1.get(key2));
        assertEquals("LatestUpdatedValue2", replicatedMap2.get(key2));
    }

    private void afterSplitHigherHitsMapMergePolicy() {
        replicatedMap1.put(key1, "higherHitsValue1");
        replicatedMap1.put(key2, "value2");

        // increase hits number
        assertEquals("higherHitsValue1", replicatedMap1.get(key1));
        assertEquals("higherHitsValue1", replicatedMap1.get(key1));

        replicatedMap2.put(key1, "value1");
        replicatedMap2.put(key2, "higherHitsValue2");

        // increase hits number
        assertEquals("higherHitsValue2", replicatedMap2.get(key2));
        assertEquals("higherHitsValue2", replicatedMap2.get(key2));
    }

    private void afterMergeHigherHitsMapMergePolicy() {
        assertEquals("higherHitsValue1", replicatedMap1.get(key1));
        assertEquals("higherHitsValue1", replicatedMap2.get(key1));

        assertEquals("higherHitsValue2", replicatedMap1.get(key2));
        assertEquals("higherHitsValue2", replicatedMap2.get(key2));
    }

    private void afterSplitPutIfAbsentMapMergePolicy() {
        replicatedMap1.put(key1, "PutIfAbsentValue1");

        replicatedMap2.put(key1, "value1");
        replicatedMap2.put(key2, "PutIfAbsentValue2");
    }

    private void afterMergePutIfAbsentMapMergePolicy() {
        assertEquals("PutIfAbsentValue1", replicatedMap1.get(key1));
        assertEquals("PutIfAbsentValue1", replicatedMap2.get(key1));

        assertEquals("PutIfAbsentValue2", replicatedMap1.get(key2));
        assertEquals("PutIfAbsentValue2", replicatedMap2.get(key2));
    }

    private void afterSplitPassThroughMapMergePolicy() {
        replicatedMap1.put(key1, "value");
        replicatedMap2.put(key1, "passThroughValue");
    }

    private void afterMergePassThroughMapMergePolicy() {
        assertEquals("passThroughValue", replicatedMap1.get(key1));
        assertEquals("passThroughValue", replicatedMap2.get(key1));
    }

    private void afterSplitCustomReplicatedMapMergePolicy() {
        replicatedMap1.put(key1, "value");
        replicatedMap2.put(key1, 1);
    }

    private void afterMergeCustomReplicatedMapMergePolicy() {
        assertEquals(1, replicatedMap1.get(key1));
        assertEquals(1, replicatedMap2.get(key1));
    }

    private static class CustomReplicatedMergePolicy implements ReplicatedMapMergePolicy {

        @Override
        public Object merge(String replicatedMapName, ReplicatedMapEntryView mergingEntry, ReplicatedMapEntryView existingEntry) {
            if (mergingEntry.getValue() instanceof Integer) {
                return mergingEntry.getValue();
            }
            return null;
        }
    }
}
