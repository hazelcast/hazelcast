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
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * <b>Given:</b>
 * Cluster with 3 members, maps configured with a custom merge policy that subtracts merging from existing value (if exists)
 * or the merging value itself.
 * <p>
 * <b>When:</b>
 * Cluster splits in two sub-clusters with {1, 2} members respectively, on each brain put values:
 * <ul>
 * <li>on first brain, keys 0..1999 -> value 1</li>
 * <li>on second brain, keys 1000..2999 -> value 3</li>
 * </ul>
 * <p>
 * <b>Then:</b>
 * Custom merge policy's merge() method is invoked for all entries of the map, assert final map values as follows:
 * <ul>
 * <li>keys 0..999 -> value 1 (merged, no existing value)</li>
 * <li>keys 1000..1999 -> value 2 (merged, result of (3-1))</li>
 * <li>keys 2000..2999 -> value 3 (not merged)</li>
 * </ul>
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LegacyCustomMapSplitBrainTest extends SplitBrainTestSupport {

    private static final String TEST_MAPS_PREFIX = "MapSplitBrainTest";
    private static final CopyOnWriteArrayList<SubtractingMergePolicy> MERGE_POLICY_INSTANCES
            = new CopyOnWriteArrayList<SubtractingMergePolicy>();

    private final CountDownLatch clusterMergedLatch = new CountDownLatch(1);
    private final AtomicInteger countOfMerges = new AtomicInteger();

    private String testMapName;

    @Override
    protected int[] brains() {
        // first half merges into second half
        return new int[]{1, 2};
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.getMapConfig(TEST_MAPS_PREFIX + "*")
                .setMergePolicy(SubtractingMergePolicy.class.getName());
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        testMapName = TEST_MAPS_PREFIX + randomMapName();

        instances[0].getLifecycleService().addLifecycleListener(new MergedLifecycleListener());
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        // put value 1 for keys 0..1999 on first brain
        IMap<Integer, Integer> mapOnFirstBrain = firstBrain[0].getMap(testMapName);
        for (int i = 0; i < 2000; i++) {
            mapOnFirstBrain.put(i, 1);
        }

        // put value 3 for keys 1000..2999 on second brain
        IMap<Integer, Integer> mapOnSecondBrain = secondBrain[0].getMap(testMapName);
        for (int i = 1000; i < 3000; i++) {
            mapOnSecondBrain.put(i, 3);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        assertOpenEventually(clusterMergedLatch, 30);
        // map on smaller, merging cluster has 2000 entries (0..1999), so 2000 merges should have happened
        assertEquals(2000, countOfMerges.get());
        IMap<Integer, Integer> map = instances[0].getMap(testMapName);
        // final map should have:
        // keys 0..999: value 1
        // keys 1000..1999: value 2
        // keys 2000..2999: value 3
        for (int i = 0; i < 3000; i++) {
            try {
                assertEquals(i / 1000 + 1, (long) map.get(i));
            } catch (Exception e) {
                System.out.println(">>>> " + i);
                e.printStackTrace();
            }
        }
    }

    /**
     * Subtracts the integer value of the merging entry from the existing entry (if one exists).
     */
    public static class SubtractingMergePolicy implements MapMergePolicy {

        final AtomicInteger counter;

        public SubtractingMergePolicy() {
            this.counter = new AtomicInteger();
            MERGE_POLICY_INSTANCES.add(this);
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
        }

        @Override
        public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
            counter.incrementAndGet();
            Integer existingValue = (Integer) existingEntry.getValue();
            Integer mergingValue = (Integer) mergingEntry.getValue();
            if (existingValue != null) {
                return existingValue - mergingValue;
            } else {
                return mergingEntry.getValue();
            }
        }
    }

    class MergedLifecycleListener implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                for (SubtractingMergePolicy mergePolicy : MERGE_POLICY_INSTANCES) {
                    countOfMerges.addAndGet(mergePolicy.counter.get());
                }
                clusterMergedLatch.countDown();
            }
        }
    }
}
