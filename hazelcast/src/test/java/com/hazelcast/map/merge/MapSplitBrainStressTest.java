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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Runs several iterations of a split-brain and split-brain healing cycle on a constant data set.
 * <p>
 * There are {@value #MAP_COUNT} maps which are filled with {@value #ENTRY_COUNT} entries each.
 * The configured pass through merge policy will trigger the split-brain healing and some merge code,
 * but will not change any data.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapSplitBrainStressTest extends SplitBrainTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule
            = new ChangeLoggingRule("log4j2-trace-map-split-brain-stress.xml");

    static final int ITERATION_COUNT = 50;
    static final int MAP_COUNT = 100;
    static final int ENTRY_COUNT = 100;
    static final int FIRST_BRAIN_SIZE = 3;
    static final int SECOND_BRAIN_SIZE = 2;
    static final Class<PassThroughMergePolicy> MERGE_POLICY = PassThroughMergePolicy.class;

    static final int TEST_TIMEOUT_IN_MILLIS = 15 * 60 * 1000;
    static final String MAP_NAME_PREFIX = "map";
    static final ILogger LOGGER = Logger.getLogger(MapSplitBrainStressTest.class);

    final Map<HazelcastInstance, UUID> listenerRegistry = new ConcurrentHashMap<>();
    final Map<Integer, String> mapNames = new ConcurrentHashMap<>();

    MergeLifecycleListener mergeLifecycleListener;
    int iteration = 1;

    @Override
    protected Config config() {
        Config config = super.config();
        config.getMapConfig(MAP_NAME_PREFIX + "*")
                .getMergePolicyConfig()
                .setPolicy(MERGE_POLICY.getName());
        return config;
    }

    @Override
    protected int[] brains() {
        return new int[]{FIRST_BRAIN_SIZE, SECOND_BRAIN_SIZE};
    }

    @Override
    protected int iterations() {
        return ITERATION_COUNT;
    }

    @Test(timeout = TEST_TIMEOUT_IN_MILLIS)
    @Override
    public void testSplitBrain() throws Exception {
        super.testSplitBrain();
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        LOGGER.info("Starting iteration " + iteration);

        if (iteration == 1) {
            for (int mapIndex = 0; mapIndex < MAP_COUNT; mapIndex++) {
                LOGGER.info("Filling map " + mapIndex + "/" + MAP_COUNT + " with " + ENTRY_COUNT + " entries");
                String mapName = MAP_NAME_PREFIX + "_" + (mapIndex + 1);
                mapNames.put(mapIndex, mapName);

                IMap<Integer, Integer> mapOnFirstBrain = instances[0].getMap(mapName);
                for (int key = 0; key < ENTRY_COUNT; key++) {
                    mapOnFirstBrain.put(key, key);
                }
            }
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            UUID listener = instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
            listenerRegistry.put(instance, listener);
        }

        assertEquals(FIRST_BRAIN_SIZE, firstBrain.length);
        assertEquals(SECOND_BRAIN_SIZE, secondBrain.length);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();
        for (Map.Entry<HazelcastInstance, UUID> entry : listenerRegistry.entrySet()) {
            entry.getKey().getLifecycleService().removeLifecycleListener(entry.getValue());
        }

        int expectedClusterSize = FIRST_BRAIN_SIZE + SECOND_BRAIN_SIZE;
        assertEquals("expected cluster size " + expectedClusterSize, expectedClusterSize, instances.length);

        for (int mapIndex = 0; mapIndex < MAP_COUNT; mapIndex++) {
            String mapName = mapNames.get(mapIndex);
            IMap<Integer, Integer> map = instances[0].getMap(mapName);
            assertEquals(format("expected %d entries in map %d/%d (iteration %d)",
                            ENTRY_COUNT, mapIndex, MAP_COUNT, iteration),
                    ENTRY_COUNT, map.size());
            for (int key = 0; key < ENTRY_COUNT; key++) {
                int value = map.get(key);
                assertEquals(format("expected value %d for key %d in map %d/%d (iteration %d)",
                                value, key, mapIndex, MAP_COUNT, iteration),
                        key, value);
            }
        }

        iteration++;
    }

    private static class MergeLifecycleListener implements LifecycleListener {

        private final CountDownLatch latch;

        MergeLifecycleListener(int mergingClusterSize) {
            latch = new CountDownLatch(mergingClusterSize);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }

        public void await() {
            assertOpenEventually(latch);
        }
    }
}
