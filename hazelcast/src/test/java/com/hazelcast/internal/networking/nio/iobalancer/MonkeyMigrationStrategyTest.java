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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ItemCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.TestCollectionUtils.setOf;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MonkeyMigrationStrategyTest extends HazelcastTestSupport {
    private MigrationStrategy strategy;

    private Map<NioThread, Set<MigratablePipeline>> ownerPipelines;
    private ItemCounter<MigratablePipeline> pipelineLoadCounter;
    private LoadImbalance imbalance;

    @Test
    public void imbalanceDetected_shouldReturnFalseWhenNoPipelineExist() {
        ownerPipelines.put(imbalance.srcOwner, Collections.<MigratablePipeline>emptySet());

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Before
    public void setUp() {
        ownerPipelines = new HashMap<NioThread, Set<MigratablePipeline>>();
        pipelineLoadCounter = new ItemCounter<MigratablePipeline>();
        imbalance = new LoadImbalance(ownerPipelines, pipelineLoadCounter);
        imbalance.srcOwner = mock(NioThread.class);

        this.strategy = new MonkeyMigrationStrategy();
    }

    @Test
    public void imbalanceDetected_shouldReturnTrueWhenPipelineExist() {
        MigratablePipeline pipeline = mock(MigratablePipeline.class);

        ownerPipelines.put(imbalance.srcOwner, setOf(pipeline));
        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertTrue(imbalanceDetected);
    }

    @Test
    public void findPipelineToMigrate_shouldWorkEvenWithASinglePipelineAvailable() {
        MigratablePipeline pipeline = mock(MigratablePipeline.class);

        ownerPipelines.put(imbalance.srcOwner, setOf(pipeline));
        MigratablePipeline pipelineToMigrate = strategy.findPipelineToMigrate(imbalance);
        assertEquals(pipeline, pipelineToMigrate);
    }

    @Test
    public void findPipelineToMigrate_shouldBeFair() {
        int iterationCount = 10000;
        double toleranceFactor = 0.25d;

        MigratablePipeline pipeline1 = mock(MigratablePipeline.class);
        MigratablePipeline pipeline2 = mock(MigratablePipeline.class);
        ownerPipelines.put(imbalance.srcOwner, setOf(pipeline1, pipeline2));

        assertFairSelection(iterationCount, toleranceFactor, pipeline1, pipeline2);
    }

    private void assertFairSelection(int iterationCount, double toleranceFactor, MigratablePipeline pipeline1, MigratablePipeline pipeline2) {
        int pipeline1Count = 0;
        int pipeline2Count = 0;
        for (int i = 0; i < iterationCount; i++) {
            MigratablePipeline candidate = strategy.findPipelineToMigrate(imbalance);
            if (candidate == pipeline1) {
                pipeline1Count++;
            } else if (candidate == pipeline2) {
                pipeline2Count++;
            } else {
                fail("No pipeline selected");
            }
        }
        int diff = abs(pipeline1Count - pipeline2Count);
        assertTrue(diff < (iterationCount * toleranceFactor));
    }


}
