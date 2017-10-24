/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
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

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                LatestUpdateMapMergePolicy.class,
                HigherHitsMapMergePolicy.class,
                PutIfAbsentMapMergePolicy.class,
                PassThroughMergePolicy.class,
                CustomReplicatedMergePolicy.class
        });
    }

    @Parameter
    public Class<? extends ReplicatedMapMergePolicy> mergePolicyClass;

    private String replicatedMapName = randomMapName();
    private MergeLifecycleListener mergeLifecycleListener;
    private ReplicatedMap<Object, Object> replicatedMap1;
    private ReplicatedMap<Object, Object> replicatedMap2;

    @Override
    protected Config config() {
        Config config = super.config();
        config.getReplicatedMapConfig(replicatedMapName)
                .setMergePolicy(mergePolicyClass.getName());
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

        if (mergePolicyClass == LatestUpdateMapMergePolicy.class) {
            afterSplitLatestUpdateMapMergePolicy();
        }
        if (mergePolicyClass == HigherHitsMapMergePolicy.class) {
            afterSplitHigherHitsMapMergePolicy();
        }
        if (mergePolicyClass == PutIfAbsentMapMergePolicy.class) {
            afterSplitPutIfAbsentMapMergePolicy();
        }
        if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMapMergePolicy();
        }
        if (mergePolicyClass == CustomReplicatedMergePolicy.class) {
            afterSplitCustomReplicatedMapMergePolicy();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        if (mergePolicyClass == LatestUpdateMapMergePolicy.class) {
            afterMergeLatestUpdateMapMergePolicy();
        }
        if (mergePolicyClass == HigherHitsMapMergePolicy.class) {
            afterMergeHigherHitsMapMergePolicy();
        }
        if (mergePolicyClass == PutIfAbsentMapMergePolicy.class) {
            afterMergePutIfAbsentMapMergePolicy();
        }
        if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMapMergePolicy();
        }
        if (mergePolicyClass == CustomReplicatedMergePolicy.class) {
            afterMergeCustomReplicatedMapMergePolicy();
        }
    }

    private void afterSplitLatestUpdateMapMergePolicy() {
        for (HazelcastInstance hz : getBrains().getFirstHalf()) {
            ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(replicatedMapName);
            replicatedMap.put("key1", "value1");
        }

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        replicatedMap2.put("key1", "LatestUpdatedValue1");
        replicatedMap2.put("key2", "value2");

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        for (HazelcastInstance hz : getBrains().getFirstHalf()) {
            ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(replicatedMapName);
            replicatedMap.put("key2", "LatestUpdatedValue2");
        }
    }

    private void afterMergeLatestUpdateMapMergePolicy() {
        assertEquals("LatestUpdatedValue1", replicatedMap1.get("key1"));
        assertEquals("LatestUpdatedValue1", replicatedMap2.get("key1"));

        assertEquals("LatestUpdatedValue2", replicatedMap1.get("key2"));
        assertEquals("LatestUpdatedValue2", replicatedMap2.get("key2"));
    }

    private void afterSplitHigherHitsMapMergePolicy() {
        // hits are not replicated and both nodes of the larger cluster can be the merge target,
        // so we have to create the hits on both nodes
        for (HazelcastInstance hz : getBrains().getFirstHalf()) {
            ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(replicatedMapName);
            replicatedMap.put("key1", "higherHitsValue1");
            replicatedMap.put("key2", "value2");

            // increase hits number
            assertEquals("higherHitsValue1", replicatedMap.get("key1"));
            assertEquals("higherHitsValue1", replicatedMap.get("key1"));
        }

        replicatedMap2.put("key1", "value1");
        replicatedMap2.put("key2", "higherHitsValue2");

        // increase hits number
        assertEquals("higherHitsValue2", replicatedMap2.get("key2"));
        assertEquals("higherHitsValue2", replicatedMap2.get("key2"));
    }

    private void afterMergeHigherHitsMapMergePolicy() {
        assertEquals("higherHitsValue1", replicatedMap1.get("key1"));
        assertEquals("higherHitsValue1", replicatedMap2.get("key1"));

        assertEquals("higherHitsValue2", replicatedMap1.get("key2"));
        assertEquals("higherHitsValue2", replicatedMap2.get("key2"));
    }

    private void afterSplitPutIfAbsentMapMergePolicy() {
        for (HazelcastInstance hz : getBrains().getFirstHalf()) {
            ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(replicatedMapName);
            replicatedMap.put("key1", "PutIfAbsentValue1");
        }

        replicatedMap2.put("key1", "value1");
        replicatedMap2.put("key2", "PutIfAbsentValue2");
    }

    private void afterMergePutIfAbsentMapMergePolicy() {
        assertEquals("PutIfAbsentValue1", replicatedMap1.get("key1"));
        assertEquals("PutIfAbsentValue1", replicatedMap2.get("key1"));

        assertEquals("PutIfAbsentValue2", replicatedMap1.get("key2"));
        assertEquals("PutIfAbsentValue2", replicatedMap2.get("key2"));
    }

    private void afterSplitPassThroughMapMergePolicy() {
        for (HazelcastInstance hz : getBrains().getFirstHalf()) {
            ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(replicatedMapName);
            replicatedMap.put("key", "value");
        }

        replicatedMap2.put("key", "passThroughValue");
    }

    private void afterMergePassThroughMapMergePolicy() {
        assertEquals("passThroughValue", replicatedMap1.get("key"));
        assertEquals("passThroughValue", replicatedMap2.get("key"));
    }

    private void afterSplitCustomReplicatedMapMergePolicy() {
        for (HazelcastInstance hz : getBrains().getFirstHalf()) {
            ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(replicatedMapName);
            replicatedMap.put("key", "value");
        }

        replicatedMap2.put("key", 1);
    }

    private void afterMergeCustomReplicatedMapMergePolicy() {
        assertEquals(1, replicatedMap1.get("key"));
        assertEquals(1, replicatedMap2.get("key"));
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
