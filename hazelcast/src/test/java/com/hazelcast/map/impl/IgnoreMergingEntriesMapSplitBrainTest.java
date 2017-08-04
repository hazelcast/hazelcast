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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

/**
 * Before merge, puts some entries to the merging sub-cluster and expects not to see them after merge
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IgnoreMergingEntriesMapSplitBrainTest extends SplitBrainTestSupport {

    private String testMapName = randomMapName();

    final CountDownLatch clusterMergedLatch = new CountDownLatch(1);

    @Override
    protected Config config() {
        Config config = super.config();
        config.getMapConfig(testMapName)
              .setMergePolicy(IgnoreMergingEntryMapMergePolicy.class.getName());
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances)
            throws Exception {
        instances[0].getLifecycleService().addLifecycleListener(new MergedLifecycleListener());
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {
        IMap<Integer, Integer> mapOnFirstBrain = firstBrain[0].getMap(testMapName);
        for (int i = 0; i < 100; i++) {
            mapOnFirstBrain.put(i, i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances)
            throws Exception {
        assertOpenEventually(clusterMergedLatch, 30);

        IMap<Integer, Integer> map = instances[0].getMap(testMapName);
        assertTrue(map.isEmpty());

    }

    class MergedLifecycleListener implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                clusterMergedLatch.countDown();
            }
        }
    }

}
