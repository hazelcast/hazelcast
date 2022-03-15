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
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoadTrackerTest {

    private NioThread owner1;
    private NioThread owner2;

    private NioThread[] owner3;
    private LoadTracker loadTracker;

    @Before
    public void setUp() {
        owner1 = mock(NioThread.class);
        owner2 = mock(NioThread.class);
        owner3 = new NioThread[]{owner1, owner2};

        ILogger logger = mock(ILogger.class);
        when(logger.isFinestEnabled()).thenReturn(true);

        loadTracker = new LoadTracker(owner3, logger);
    }

    @Test
    public void testUpdateImbalance() throws Exception {
        MigratablePipeline owner1Pipeline1 = mock(MigratablePipeline.class);
        when(owner1Pipeline1.load()).thenReturn(0L)
                .thenReturn(100L);
        when(owner1Pipeline1.owner())
                .thenReturn(owner1);
        loadTracker.addPipeline(owner1Pipeline1);

        MigratablePipeline owner2Pipeline1 = mock(MigratablePipeline.class);
        when(owner2Pipeline1.load())
                .thenReturn(0L)
                .thenReturn(200L);
        when(owner2Pipeline1.owner())
                .thenReturn(owner2);
        loadTracker.addPipeline(owner2Pipeline1);

        MigratablePipeline owner2Pipeline3 = mock(MigratablePipeline.class);
        when(owner2Pipeline3.load())
                .thenReturn(0L)
                .thenReturn(100L);
        when(owner2Pipeline3.owner())
                .thenReturn(owner2);
        loadTracker.addPipeline(owner2Pipeline3);

        LoadImbalance loadImbalance = loadTracker.updateImbalance();
        assertEquals(0, loadImbalance.minimumLoad);
        assertEquals(0, loadImbalance.maximumLoad);

        loadTracker.updateImbalance();
        assertEquals(100, loadImbalance.minimumLoad);
        assertEquals(300, loadImbalance.maximumLoad);
        assertEquals(owner1, loadImbalance.dstOwner);
        assertEquals(owner2, loadImbalance.srcOwner);
    }

    // there is no point in selecting a selector with a single handler as source.
    @Test
    public void testUpdateImbalance_notUsingSinglePipelineOwnerAsSource() throws Exception {
        MigratablePipeline owmer1Pipeline1 = mock(MigratablePipeline.class);
        // the first selector has a handler with a large number of events
        when(owmer1Pipeline1.load()).thenReturn(10000L);
        when(owmer1Pipeline1.owner()).thenReturn(owner1);
        loadTracker.addPipeline(owmer1Pipeline1);

        MigratablePipeline owner2Pipeline = mock(MigratablePipeline.class);
        when(owner2Pipeline.load()).thenReturn(200L);
        when(owner2Pipeline.owner()).thenReturn(owner2);
        loadTracker.addPipeline(owner2Pipeline);

        MigratablePipeline owner2Pipeline2 = mock(MigratablePipeline.class);
        when(owner2Pipeline2.load()).thenReturn(200L);
        when(owner2Pipeline2.owner()).thenReturn(owner2);
        loadTracker.addPipeline(owner2Pipeline2);

        LoadImbalance loadImbalance = loadTracker.updateImbalance();

        assertEquals(400, loadImbalance.minimumLoad);
        assertEquals(400, loadImbalance.maximumLoad);
        assertEquals(owner2, loadImbalance.dstOwner);
        assertEquals(owner2, loadImbalance.srcOwner);
    }
}
